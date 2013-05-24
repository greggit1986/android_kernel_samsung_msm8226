/*
 *	An async IO implementation for Linux
 *	Written by Benjamin LaHaise <bcrl@kvack.org>
 *
 *	Implements an efficient asynchronous io interface.
 *
 *	Copyright 2000, 2001, 2002 Red Hat, Inc.  All Rights Reserved.
 *
 *	See ../COPYING for licensing terms.
 */
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/errno.h>
#include <linux/time.h>
#include <linux/aio_abi.h>
#include <linux/export.h>
#include <linux/syscalls.h>
#include <linux/backing-dev.h>
#include <linux/uio.h>

#define DEBUG 0

#include <linux/sched.h>
#include <linux/fs.h>
#include <linux/file.h>
#include <linux/mm.h>
#include <linux/mman.h>
#include <linux/mmu_context.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/aio.h>
#include <linux/highmem.h>
#include <linux/workqueue.h>
#include <linux/security.h>
#include <linux/eventfd.h>
#include <linux/blkdev.h>
#include <linux/compat.h>
#include <linux/personality.h>

#include <asm/kmap_types.h>
#include <asm/uaccess.h>

#if DEBUG > 1
#define dprintk		printk
#else
#define dprintk(x...)	do { ; } while (0)
#endif

#define AIO_RING_MAGIC			0xa10a10a1
#define AIO_RING_COMPAT_FEATURES	1
#define AIO_RING_INCOMPAT_FEATURES	0
struct aio_ring {
	unsigned	id;	/* kernel internal index number */
	unsigned	nr;	/* number of io_events */
	unsigned	head;
	unsigned	tail;

	unsigned	magic;
	unsigned	compat_features;
	unsigned	incompat_features;
	unsigned	header_length;	/* size of aio_ring */


	struct io_event		io_events[0];
}; /* 128 bytes + ring size */

#define AIO_RING_PAGES	8

struct kioctx {
	atomic_t		users;
	int			dead;

	/* This needs improving */
	unsigned long		user_id;
	struct hlist_node	list;

	/*
	 * This is what userspace passed to io_setup(), it's not used for
	 * anything but counting against the global max_reqs quota.
	 *
	 * The real limit is nr_events - 1, which will be larger (see
	 * aio_setup_ring())
	 */
	unsigned		max_reqs;

	/* Size of ringbuffer, in units of struct io_event */
	unsigned		nr_events;

	unsigned long		mmap_base;
	unsigned long		mmap_size;

	struct page		**ring_pages;
	long			nr_pages;

	struct rcu_head		rcu_head;
	struct work_struct	rcu_work;

	struct {
		atomic_t	reqs_active;
	} ____cacheline_aligned_in_smp;

	struct {
		spinlock_t	ctx_lock;
		struct list_head active_reqs;	/* used for cancellation */
	} ____cacheline_aligned_in_smp;

	struct {
		struct mutex	ring_lock;
		wait_queue_head_t wait;
	} ____cacheline_aligned_in_smp;

	struct {
		unsigned	tail;
		spinlock_t	completion_lock;
	} ____cacheline_aligned_in_smp;

	struct page		*internal_pages[AIO_RING_PAGES];
};

/*------ sysctl variables----*/
static DEFINE_SPINLOCK(aio_nr_lock);
unsigned long aio_nr;		/* current system wide number of aio requests */
unsigned long aio_max_nr = 0x10000; /* system wide maximum number of aio requests */
/*----end sysctl variables---*/

static struct kmem_cache	*kiocb_cachep;
static struct kmem_cache	*kioctx_cachep;

static struct workqueue_struct *aio_wq;

/* Used for rare fput completion. */
static void aio_fput_routine(struct work_struct *);
static DECLARE_WORK(fput_work, aio_fput_routine);

static DEFINE_SPINLOCK(fput_lock);
static LIST_HEAD(fput_head);

static void aio_kick_handler(struct work_struct *);
static void aio_queue_work(struct kioctx *);

/* aio_setup
 *	Creates the slab caches used by the aio routines, panic on
 *	failure as this is done early during the boot sequence.
 */
static int __init aio_setup(void)
{
	kiocb_cachep = KMEM_CACHE(kiocb, SLAB_HWCACHE_ALIGN|SLAB_PANIC);
	kioctx_cachep = KMEM_CACHE(kioctx,SLAB_HWCACHE_ALIGN|SLAB_PANIC);

	aio_wq = alloc_workqueue("aio", 0, 1);	/* used to limit concurrency */
	BUG_ON(!aio_wq);

	pr_debug("aio_setup: sizeof(struct page) = %d\n", (int)sizeof(struct page));

	return 0;
}
__initcall(aio_setup);

static void aio_free_ring(struct kioctx *ctx)
{
	long i;

	for (i = 0; i < ctx->nr_pages; i++)
		put_page(ctx->ring_pages[i]);

	if (ctx->mmap_size)
		vm_munmap(ctx->mmap_base, ctx->mmap_size);

	if (ctx->ring_pages && ctx->ring_pages != ctx->internal_pages)
		kfree(ctx->ring_pages);
}

static int aio_setup_ring(struct kioctx *ctx)
{
	struct aio_ring *ring;
	unsigned nr_events = ctx->max_reqs;
	unsigned long size;
	int nr_pages;

	if (current->personality & READ_IMPLIES_EXEC)
		return -EPERM;

	/* Compensate for the ring buffer's head/tail overlap entry */
	nr_events += 2;	/* 1 is required, 2 for good luck */

	size = sizeof(struct aio_ring);
	size += sizeof(struct io_event) * nr_events;
	nr_pages = (size + PAGE_SIZE-1) >> PAGE_SHIFT;

	if (nr_pages < 0)
		return -EINVAL;

	nr_events = (PAGE_SIZE * nr_pages - sizeof(struct aio_ring)) / sizeof(struct io_event);

	ctx->nr_events = 0;
	ctx->ring_pages = ctx->internal_pages;
	if (nr_pages > AIO_RING_PAGES) {
		ctx->ring_pages = kcalloc(nr_pages, sizeof(struct page *),
					  GFP_KERNEL);
		if (!ctx->ring_pages)
			return -ENOMEM;
	}

	ctx->mmap_size = nr_pages * PAGE_SIZE;
	pr_debug("attempting mmap of %lu bytes\n", ctx->mmap_size);
	down_write(&mm->mmap_sem);
	ctx->mmap_base = do_mmap_pgoff(NULL, 0, ctx->mmap_size,
				       PROT_READ|PROT_WRITE,
				       MAP_ANONYMOUS|MAP_PRIVATE, 0, &populate);
	if (IS_ERR((void *)ctx->mmap_base)) {
		up_write(&mm->mmap_sem);
		ctx->mmap_size = 0;
		aio_free_ring(ctx);
		return -EAGAIN;
	}

	pr_debug("mmap address: 0x%08lx\n", ctx->mmap_base);
	ctx->nr_pages = get_user_pages(current, mm, ctx->mmap_base, nr_pages,
				       1, 0, ctx->ring_pages, NULL);
	up_write(&mm->mmap_sem);

	if (unlikely(ctx->nr_pages != nr_pages)) {
		aio_free_ring(ctx);
		return -EAGAIN;
	}
	if (populate)
		mm_populate(ctx->mmap_base, populate);

	ctx->user_id = ctx->mmap_base;
	ctx->nr_events = nr_events; /* trusted copy */

	ring = kmap_atomic(ctx->ring_pages[0]);
	ring->nr = nr_events;	/* user copy */
	ring->id = ctx->user_id;
	ring->head = ring->tail = 0;
	ring->magic = AIO_RING_MAGIC;
	ring->compat_features = AIO_RING_COMPAT_FEATURES;
	ring->incompat_features = AIO_RING_INCOMPAT_FEATURES;
	ring->header_length = sizeof(struct aio_ring);
	kunmap_atomic(ring);
	flush_dcache_page(ctx->ring_pages[0]);

	return 0;
}


/* aio_ring_event: returns a pointer to the event at the given index from
 * kmap_atomic().  Release the pointer with put_aio_ring_event();
 */
#define AIO_EVENTS_PER_PAGE	(PAGE_SIZE / sizeof(struct io_event))
#define AIO_EVENTS_FIRST_PAGE	((PAGE_SIZE - sizeof(struct aio_ring)) / sizeof(struct io_event))
#define AIO_EVENTS_OFFSET	(AIO_EVENTS_PER_PAGE - AIO_EVENTS_FIRST_PAGE)

void kiocb_set_cancel_fn(struct kiocb *req, kiocb_cancel_fn *cancel)
{
	struct kioctx *ctx = req->ki_ctx;
	unsigned long flags;

	spin_lock_irqsave(&ctx->ctx_lock, flags);

	if (!req->ki_list.next)
		list_add(&req->ki_list, &ctx->active_reqs);

	req->ki_cancel = cancel;

	spin_unlock_irqrestore(&ctx->ctx_lock, flags);
}
EXPORT_SYMBOL(kiocb_set_cancel_fn);

static int kiocb_cancel(struct kioctx *ctx, struct kiocb *kiocb,
			struct io_event *res)
{
	kiocb_cancel_fn *old, *cancel;
	int ret = -EINVAL;

	/*
	 * Don't want to set kiocb->ki_cancel = KIOCB_CANCELLED unless it
	 * actually has a cancel function, hence the cmpxchg()
	 */

	cancel = ACCESS_ONCE(kiocb->ki_cancel);
	do {
		if (!cancel || cancel == KIOCB_CANCELLED)
			return ret;

		old = cancel;
		cancel = cmpxchg(&kiocb->ki_cancel, old, KIOCB_CANCELLED);
	} while (cancel != old);

	atomic_inc(&kiocb->ki_users);
	spin_unlock_irq(&ctx->ctx_lock);

	memset(res, 0, sizeof(*res));
	res->obj = (u64)(unsigned long)kiocb->ki_obj.user;
	res->data = kiocb->ki_user_data;
	ret = cancel(kiocb, res);

	spin_lock_irq(&ctx->ctx_lock);

	return ret;
}

static void free_ioctx_rcu(struct rcu_head *head)
{
	struct kioctx *ctx = container_of(head, struct kioctx, rcu_head);
	kmem_cache_free(kioctx_cachep, ctx);
}

/* __put_ioctx
 *	Called when the last user of an aio context has gone away,
 *	and the struct needs to be freed.
 */
static void __put_ioctx(struct kioctx *ctx)
{
	struct aio_ring *ring;
	struct io_event res;
	struct kiocb *req;
	unsigned head, avail;

	spin_lock_irq(&ctx->ctx_lock);

	while (!list_empty(&ctx->active_reqs)) {
		req = list_first_entry(&ctx->active_reqs,
				       struct kiocb, ki_list);

		list_del_init(&req->ki_list);
		kiocb_cancel(ctx, req, &res);
	}

	spin_unlock_irq(&ctx->ctx_lock);

	ring = kmap_atomic(ctx->ring_pages[0]);
	head = ring->head;
	kunmap_atomic(ring);

	while (atomic_read(&ctx->reqs_active) > 0) {
		wait_event(ctx->wait,
				head != ctx->tail ||
				atomic_read(&ctx->reqs_active) <= 0);

		avail = (head <= ctx->tail ? ctx->tail : ctx->nr_events) - head;

		atomic_sub(avail, &ctx->reqs_active);
		head += avail;
		head %= ctx->nr_events;
	}

	WARN_ON(atomic_read(&ctx->reqs_active) < 0);

	cancel_delayed_work_sync(&ctx->wq);
	aio_free_ring(ctx);
	mmdrop(ctx->mm);
	ctx->mm = NULL;
	if (nr_events) {
		spin_lock(&aio_nr_lock);
		BUG_ON(aio_nr - nr_events > aio_nr);
		aio_nr -= nr_events;
		spin_unlock(&aio_nr_lock);
	}
	pr_debug("__put_ioctx: freeing %p\n", ctx);
	call_rcu(&ctx->rcu_head, ctx_rcu_free);
}

static inline int try_get_ioctx(struct kioctx *kioctx)
{
	return atomic_inc_not_zero(&kioctx->users);
}

static inline void put_ioctx(struct kioctx *kioctx)
{
	BUG_ON(atomic_read(&kioctx->users) <= 0);
	if (unlikely(atomic_dec_and_test(&kioctx->users)))
		__put_ioctx(kioctx);
}

/* ioctx_alloc
 *	Allocates and initializes an ioctx.  Returns an ERR_PTR if it failed.
 */
static struct kioctx *ioctx_alloc(unsigned nr_events)
{
	struct mm_struct *mm;
	struct kioctx *ctx;
	int err = -ENOMEM;

	/* Prevent overflows */
	if ((nr_events > (0x10000000U / sizeof(struct io_event))) ||
	    (nr_events > (0x10000000U / sizeof(struct kiocb)))) {
		pr_debug("ENOMEM: nr_events too high\n");
		return ERR_PTR(-EINVAL);
	}

	if (!nr_events || (unsigned long)nr_events > aio_max_nr)
		return ERR_PTR(-EAGAIN);

	ctx = kmem_cache_zalloc(kioctx_cachep, GFP_KERNEL);
	if (!ctx)
		return ERR_PTR(-ENOMEM);

	ctx->max_reqs = nr_events;
	mm = ctx->mm = current->mm;
	atomic_inc(&mm->mm_count);

	atomic_set(&ctx->users, 2);
	spin_lock_init(&ctx->ctx_lock);
	spin_lock_init(&ctx->completion_lock);
	mutex_init(&ctx->ring_lock);
	init_waitqueue_head(&ctx->wait);

	INIT_LIST_HEAD(&ctx->active_reqs);
	INIT_LIST_HEAD(&ctx->run_list);
	INIT_DELAYED_WORK(&ctx->wq, aio_kick_handler);

	if (aio_setup_ring(ctx) < 0)
		goto out_freectx;

	/* limit the number of system wide aios */
	spin_lock(&aio_nr_lock);
	if (aio_nr + nr_events > aio_max_nr ||
	    aio_nr + nr_events < aio_nr) {
		spin_unlock(&aio_nr_lock);
		goto out_cleanup;
	}
	aio_nr += ctx->max_reqs;
	spin_unlock(&aio_nr_lock);

	/* now link into global list. */
	spin_lock(&mm->ioctx_lock);
	hlist_add_head_rcu(&ctx->list, &mm->ioctx_list);
	spin_unlock(&mm->ioctx_lock);

	pr_debug("allocated ioctx %p[%ld]: mm=%p mask=0x%x\n",
		 ctx, ctx->user_id, mm, ctx->nr_events);
	return ctx;

out_cleanup:
	err = -EAGAIN;
	aio_free_ring(ctx);
out_freectx:
	mmdrop(mm);
	kmem_cache_free(kioctx_cachep, ctx);
	dprintk("aio: error allocating ioctx %d\n", err);
	return ERR_PTR(err);
}

/* kill_ctx
 *	Cancels all outstanding aio requests on an aio context.  Used 
 *	when the processes owning a context have all exited to encourage 
 *	the rapid destruction of the kioctx.
 */
static void kill_ctx(struct kioctx *ctx)
{
	int (*cancel)(struct kiocb *, struct io_event *);
	struct task_struct *tsk = current;
	DECLARE_WAITQUEUE(wait, tsk);
	struct io_event res;

	spin_lock_irq(&ctx->ctx_lock);
	ctx->dead = 1;
	while (!list_empty(&ctx->active_reqs)) {
		struct list_head *pos = ctx->active_reqs.next;
		struct kiocb *iocb = list_kiocb(pos);
		list_del_init(&iocb->ki_list);
		cancel = iocb->ki_cancel;
		kiocbSetCancelled(iocb);
		if (cancel) {
			iocb->ki_users++;
			spin_unlock_irq(&ctx->ctx_lock);
			cancel(iocb, &res);
			spin_lock_irq(&ctx->ctx_lock);
		}
	}

	if (!ctx->reqs_active)
		goto out;

	add_wait_queue(&ctx->wait, &wait);
	set_task_state(tsk, TASK_UNINTERRUPTIBLE);
	while (ctx->reqs_active) {
		spin_unlock_irq(&ctx->ctx_lock);
		io_schedule();
		set_task_state(tsk, TASK_UNINTERRUPTIBLE);
		spin_lock_irq(&ctx->ctx_lock);
	}
	__set_task_state(tsk, TASK_RUNNING);
	remove_wait_queue(&ctx->wait, &wait);

out:
	spin_unlock_irq(&ctx->ctx_lock);
}

/* wait_on_sync_kiocb:
 *	Waits on the given sync kiocb to complete.
 */
ssize_t wait_on_sync_kiocb(struct kiocb *iocb)
{
	while (iocb->ki_users) {
		set_current_state(TASK_UNINTERRUPTIBLE);
		if (!iocb->ki_users)
			break;
		io_schedule();
	}
	__set_current_state(TASK_RUNNING);
	return iocb->ki_user_data;
}
EXPORT_SYMBOL(wait_on_sync_kiocb);

/* exit_aio: called when the last user of mm goes away.  At this point, 
 * there is no way for any new requests to be submited or any of the 
 * io_* syscalls to be called on the context.  However, there may be 
 * outstanding requests which hold references to the context; as they 
 * go away, they will call put_ioctx and release any pinned memory
 * associated with the request (held via struct page * references).
 */
void exit_aio(struct mm_struct *mm)
{
	struct kioctx *ctx;

	while (!hlist_empty(&mm->ioctx_list)) {
		ctx = hlist_entry(mm->ioctx_list.first, struct kioctx, list);
		hlist_del_rcu(&ctx->list);

		kill_ctx(ctx);

		if (1 != atomic_read(&ctx->users))
			printk(KERN_DEBUG
				"exit_aio:ioctx still alive: %d %d %d\n",
				atomic_read(&ctx->users), ctx->dead,
				ctx->reqs_active);
		/*
		 * We don't need to bother with munmap() here -
		 * exit_mmap(mm) is coming and it'll unmap everything.
		 * Since aio_free_ring() uses non-zero ->mmap_size
		 * as indicator that it needs to unmap the area,
		 * just set it to 0; aio_free_ring() is the only
		 * place that uses ->mmap_size, so it's safe.
		 * That way we get all munmap done to current->mm -
		 * all other callers have ctx->mm == current->mm.
		 */
		ctx->mmap_size = 0;

		if (!atomic_xchg(&ctx->dead, 1)) {
			hlist_del_rcu(&ctx->list);
			call_rcu(&ctx->rcu_head, kill_ioctx_rcu);
		}
	}
}

/* aio_get_req
 *	Allocate a slot for an aio request.  Increments the users count
 * of the kioctx so that the kioctx stays around until all requests are
 * complete.  Returns NULL if no requests are free.
 *
 * Returns with kiocb->users set to 2.  The io submit code path holds
 * an extra reference while submitting the i/o.
 * This prevents races between the aio code path referencing the
 * req (after submitting it) and aio_complete() freeing the req.
 */
static inline struct kiocb *aio_get_req(struct kioctx *ctx)
{
	struct kiocb *req;

	if (atomic_read(&ctx->reqs_active) >= ctx->nr_events)
		return NULL;

	if (atomic_inc_return(&ctx->reqs_active) > ctx->nr_events - 1)
		goto out_put;

	req = kmem_cache_alloc(kiocb_cachep, GFP_KERNEL|__GFP_ZERO);
	if (unlikely(!req))
		goto out_put;

	atomic_set(&req->ki_users, 2);
	req->ki_ctx = ctx;

	return req;
out_put:
	atomic_dec(&ctx->reqs_active);
	return NULL;
}

static inline void really_put_req(struct kioctx *ctx, struct kiocb *req)
{
	assert_spin_locked(&ctx->ctx_lock);

	if (req->ki_eventfd != NULL)
		eventfd_ctx_put(req->ki_eventfd);
	if (req->ki_dtor)
		req->ki_dtor(req);
	if (req->ki_iovec != &req->ki_inline_vec)
		kfree(req->ki_iovec);
	kmem_cache_free(kiocb_cachep, req);
	ctx->reqs_active--;

	if (unlikely(!ctx->reqs_active && ctx->dead))
		wake_up_all(&ctx->wait);
}

static void aio_fput_routine(struct work_struct *data)
{
	spin_lock_irq(&fput_lock);
	while (likely(!list_empty(&fput_head))) {
		struct kiocb *req = list_kiocb(fput_head.next);
		struct kioctx *ctx = req->ki_ctx;

		list_del(&req->ki_list);
		spin_unlock_irq(&fput_lock);

		/* Complete the fput(s) */
		if (req->ki_filp != NULL)
			fput(req->ki_filp);

		/* Link the iocb into the context's free list */
		rcu_read_lock();
		spin_lock_irq(&ctx->ctx_lock);
		really_put_req(ctx, req);
		/*
		 * at that point ctx might've been killed, but actual
		 * freeing is RCU'd
		 */
		spin_unlock_irq(&ctx->ctx_lock);
		rcu_read_unlock();

		spin_lock_irq(&fput_lock);
	}
	spin_unlock_irq(&fput_lock);
}

/* __aio_put_req
 *	Returns true if this put was the last user of the request.
 */
static int __aio_put_req(struct kioctx *ctx, struct kiocb *req)
{
	dprintk(KERN_DEBUG "aio_put(%p): f_count=%ld\n",
		req, atomic_long_read(&req->ki_filp->f_count));

	assert_spin_locked(&ctx->ctx_lock);

	req->ki_users--;
	BUG_ON(req->ki_users < 0);
	if (likely(req->ki_users))
		return 0;
	list_del(&req->ki_list);		/* remove from active_reqs */
	req->ki_cancel = NULL;
	req->ki_retry = NULL;

	/*
	 * Try to optimize the aio and eventfd file* puts, by avoiding to
	 * schedule work in case it is not final fput() time. In normal cases,
	 * we would not be holding the last reference to the file*, so
	 * this function will be executed w/out any aio kthread wakeup.
	 */
	if (unlikely(!fput_atomic(req->ki_filp))) {
		spin_lock(&fput_lock);
		list_add(&req->ki_list, &fput_head);
		spin_unlock(&fput_lock);
		schedule_work(&fput_work);
	} else {
		req->ki_filp = NULL;
		really_put_req(ctx, req);
	}
	return 1;
}

/* aio_put_req
 *	Returns true if this put was the last user of the kiocb,
 *	false if the request is still in use.
 */
int aio_put_req(struct kiocb *req)
{
	struct kioctx *ctx = req->ki_ctx;
	int ret;
	spin_lock_irq(&ctx->ctx_lock);
	ret = __aio_put_req(ctx, req);
	spin_unlock_irq(&ctx->ctx_lock);
	return ret;
}
EXPORT_SYMBOL(aio_put_req);

static struct kioctx *lookup_ioctx(unsigned long ctx_id)
{
	struct mm_struct *mm = current->mm;
	struct kioctx *ctx, *ret = NULL;
	struct hlist_node *n;

	rcu_read_lock();

	hlist_for_each_entry_rcu(ctx, n, &mm->ioctx_list, list) {
		/*
		 * RCU protects us against accessing freed memory but
		 * we have to be careful not to get a reference when the
		 * reference count already dropped to 0 (ctx->dead test
		 * is unreliable because of races).
		 */
		if (ctx->user_id == ctx_id && !ctx->dead && try_get_ioctx(ctx)){
			ret = ctx;
			break;
		}
	}

	rcu_read_unlock();
	return ret;
}

/*
 * Queue up a kiocb to be retried. Assumes that the kiocb
 * has already been marked as kicked, and places it on
 * the retry run list for the corresponding ioctx, if it
 * isn't already queued. Returns 1 if it actually queued
 * the kiocb (to tell the caller to activate the work
 * queue to process it), or 0, if it found that it was
 * already queued.
 */
static inline int __queue_kicked_iocb(struct kiocb *iocb)
{
	struct kioctx *ctx = iocb->ki_ctx;

	assert_spin_locked(&ctx->ctx_lock);

	if (list_empty(&iocb->ki_run_list)) {
		list_add_tail(&iocb->ki_run_list,
			&ctx->run_list);
		return 1;
	}
	return 0;
}

/* aio_run_iocb
 *	This is the core aio execution routine. It is
 *	invoked both for initial i/o submission and
 *	subsequent retries via the aio_kick_handler.
 *	Expects to be invoked with iocb->ki_ctx->lock
 *	already held. The lock is released and reacquired
 *	as needed during processing.
 *
 * Calls the iocb retry method (already setup for the
 * iocb on initial submission) for operation specific
 * handling, but takes care of most of common retry
 * execution details for a given iocb. The retry method
 * needs to be non-blocking as far as possible, to avoid
 * holding up other iocbs waiting to be serviced by the
 * retry kernel thread.
 *
 * The trickier parts in this code have to do with
 * ensuring that only one retry instance is in progress
 * for a given iocb at any time. Providing that guarantee
 * simplifies the coding of individual aio operations as
 * it avoids various potential races.
 */
static ssize_t aio_run_iocb(struct kiocb *iocb)
{
	struct kioctx	*ctx = iocb->ki_ctx;
	ssize_t (*retry)(struct kiocb *);
	ssize_t ret;

	if (!(retry = iocb->ki_retry)) {
		printk("aio_run_iocb: iocb->ki_retry = NULL\n");
		return 0;
	}

	/*
	 * We don't want the next retry iteration for this
	 * operation to start until this one has returned and
	 * updated the iocb state. However, wait_queue functions
	 * can trigger a kick_iocb from interrupt context in the
	 * meantime, indicating that data is available for the next
	 * iteration. We want to remember that and enable the
	 * next retry iteration _after_ we are through with
	 * this one.
	 *
	 * So, in order to be able to register a "kick", but
	 * prevent it from being queued now, we clear the kick
	 * flag, but make the kick code *think* that the iocb is
	 * still on the run list until we are actually done.
	 * When we are done with this iteration, we check if
	 * the iocb was kicked in the meantime and if so, queue
	 * it up afresh.
	 */

	kiocbClearKicked(iocb);

	/*
	 * This is so that aio_complete knows it doesn't need to
	 * pull the iocb off the run list (We can't just call
	 * INIT_LIST_HEAD because we don't want a kick_iocb to
	 * queue this on the run list yet)
	 */
	iocb->ki_run_list.next = iocb->ki_run_list.prev = NULL;
	spin_unlock_irq(&ctx->ctx_lock);

	/* Quit retrying if the i/o has been cancelled */
	if (kiocbIsCancelled(iocb)) {
		ret = -EINTR;
		aio_complete(iocb, ret, 0);
		/* must not access the iocb after this */
		goto out;
	}

	/*
	 * Now we are all set to call the retry method in async
	 * context.
	 */
	ret = retry(iocb);

	if (ret != -EIOCBRETRY && ret != -EIOCBQUEUED) {
		/*
		 * There's no easy way to restart the syscall since other AIO's
		 * may be already running. Just fail this IO with EINTR.
		 */
		if (unlikely(ret == -ERESTARTSYS || ret == -ERESTARTNOINTR ||
			     ret == -ERESTARTNOHAND || ret == -ERESTART_RESTARTBLOCK))
			ret = -EINTR;
		aio_complete(iocb, ret, 0);
	}
out:
	spin_lock_irq(&ctx->ctx_lock);

	if (-EIOCBRETRY == ret) {
		/*
		 * OK, now that we are done with this iteration
		 * and know that there is more left to go,
		 * this is where we let go so that a subsequent
		 * "kick" can start the next iteration
		 */

		/* will make __queue_kicked_iocb succeed from here on */
		INIT_LIST_HEAD(&iocb->ki_run_list);
		/* we must queue the next iteration ourselves, if it
		 * has already been kicked */
		if (kiocbIsKicked(iocb)) {
			__queue_kicked_iocb(iocb);

			/*
			 * __queue_kicked_iocb will always return 1 here, because
			 * iocb->ki_run_list is empty at this point so it should
			 * be safe to unconditionally queue the context into the
			 * work queue.
			 */
			aio_queue_work(ctx);
		}
	}
	return ret;
}

/*
 * __aio_run_iocbs:
 * 	Process all pending retries queued on the ioctx
 * 	run list.
 * Assumes it is operating within the aio issuer's mm
 * context.
 */
static int __aio_run_iocbs(struct kioctx *ctx)
{
	struct kiocb *iocb;
	struct list_head run_list;

	assert_spin_locked(&ctx->ctx_lock);

	list_replace_init(&ctx->run_list, &run_list);
	while (!list_empty(&run_list)) {
		iocb = list_entry(run_list.next, struct kiocb,
			ki_run_list);
		list_del(&iocb->ki_run_list);
		/*
		 * Hold an extra reference while retrying i/o.
		 */
		iocb->ki_users++;       /* grab extra reference */
		aio_run_iocb(iocb);
		__aio_put_req(ctx, iocb);
 	}
	if (!list_empty(&ctx->run_list))
		return 1;
	return 0;
}

static void aio_queue_work(struct kioctx * ctx)
{
	unsigned long timeout;
	/*
	 * if someone is waiting, get the work started right
	 * away, otherwise, use a longer delay
	 */
	smp_mb();
	if (waitqueue_active(&ctx->wait))
		timeout = 1;
	else
		timeout = HZ/10;
	queue_delayed_work(aio_wq, &ctx->wq, timeout);
}

/*
 * aio_run_all_iocbs:
 *	Process all pending retries queued on the ioctx
 *	run list, and keep running them until the list
 *	stays empty.
 * Assumes it is operating within the aio issuer's mm context.
 */
static inline void aio_run_all_iocbs(struct kioctx *ctx)
{
	spin_lock_irq(&ctx->ctx_lock);
	while (__aio_run_iocbs(ctx))
		;
	spin_unlock_irq(&ctx->ctx_lock);
}

/*
 * aio_kick_handler:
 * 	Work queue handler triggered to process pending
 * 	retries on an ioctx. Takes on the aio issuer's
 *	mm context before running the iocbs, so that
 *	copy_xxx_user operates on the issuer's address
 *      space.
 * Run on aiod's context.
 */
static void aio_kick_handler(struct work_struct *work)
{
	struct kioctx *ctx = container_of(work, struct kioctx, wq.work);
	mm_segment_t oldfs = get_fs();
	struct mm_struct *mm;
	int requeue;

	set_fs(USER_DS);
	use_mm(ctx->mm);
	spin_lock_irq(&ctx->ctx_lock);
	requeue =__aio_run_iocbs(ctx);
	mm = ctx->mm;
	spin_unlock_irq(&ctx->ctx_lock);
 	unuse_mm(mm);
	set_fs(oldfs);
	/*
	 * we're in a worker thread already; no point using non-zero delay
	 */
	if (requeue)
		queue_delayed_work(aio_wq, &ctx->wq, 0);
}


/*
 * Called by kick_iocb to queue the kiocb for retry
 * and if required activate the aio work queue to process
 * it
 */
static void try_queue_kicked_iocb(struct kiocb *iocb)
{
 	struct kioctx	*ctx = iocb->ki_ctx;
	unsigned long flags;
	int run = 0;

	spin_lock_irqsave(&ctx->ctx_lock, flags);
	/* set this inside the lock so that we can't race with aio_run_iocb()
	 * testing it and putting the iocb on the run list under the lock */
	if (!kiocbTryKick(iocb))
		run = __queue_kicked_iocb(iocb);
	spin_unlock_irqrestore(&ctx->ctx_lock, flags);
	if (run)
		aio_queue_work(ctx);
}

/*
 * kick_iocb:
 *      Called typically from a wait queue callback context
 *      to trigger a retry of the iocb.
 *      The retry is usually executed by aio workqueue
 *      threads (See aio_kick_handler).
 */
void kick_iocb(struct kiocb *iocb)
{
	/* sync iocbs are easy: they can only ever be executing from a 
	 * single context. */
	if (is_sync_kiocb(iocb)) {
		kiocbSetKicked(iocb);
	        wake_up_process(iocb->ki_obj.tsk);
		return;
	}

	try_queue_kicked_iocb(iocb);
}
EXPORT_SYMBOL(kick_iocb);

/* aio_complete
 *	Called when the io request on the given iocb is complete.
 *	Returns true if this is the last user of the request.  The 
 *	only other user of the request can be the cancellation code.
 */
int aio_complete(struct kiocb *iocb, long res, long res2)
{
	struct kioctx	*ctx = iocb->ki_ctx;
	struct aio_ring	*ring;
	struct io_event	*event;
	unsigned long	flags;
	unsigned long	tail;
	int		ret;

	/*
	 * Special case handling for sync iocbs:
	 *  - events go directly into the iocb for fast handling
	 *  - the sync task with the iocb in its stack holds the single iocb
	 *    ref, no other paths have a way to get another ref
	 *  - the sync task helpfully left a reference to itself in the iocb
	 */
	if (is_sync_kiocb(iocb)) {
		BUG_ON(iocb->ki_users != 1);
		iocb->ki_user_data = res;
		iocb->ki_users = 0;
		wake_up_process(iocb->ki_obj.tsk);
		return 1;
	}

	/*
	 * Take rcu_read_lock() in case the kioctx is being destroyed, as we
	 * need to issue a wakeup after decrementing reqs_active.
	 */
	rcu_read_lock();

	if (iocb->ki_list.next) {
		unsigned long flags;

		spin_lock_irqsave(&ctx->ctx_lock, flags);
		list_del(&iocb->ki_list);
		spin_unlock_irqrestore(&ctx->ctx_lock, flags);
	}

	/*
	 * cancelled requests don't get events, userland was given one
	 * when the event got cancelled.
	 */
	if (unlikely(xchg(&iocb->ki_cancel,
			  KIOCB_CANCELLED) == KIOCB_CANCELLED)) {
		atomic_dec(&ctx->reqs_active);
		/* Still need the wake_up in case free_ioctx is waiting */
		goto put_rq;
	}

	/*
	 * Add a completion event to the ring buffer. Must be done holding
	 * ctx->ctx_lock to prevent other code from messing with the tail
	 * pointer since we might be called from irq context.
	 */
	spin_lock_irqsave(&ctx->completion_lock, flags);

	tail = ctx->tail;
	pos = tail + AIO_EVENTS_OFFSET;

	if (++tail >= ctx->nr_events)
		tail = 0;

	ev_page = kmap_atomic(ctx->ring_pages[pos / AIO_EVENTS_PER_PAGE]);
	event = ev_page + pos % AIO_EVENTS_PER_PAGE;

	event->obj = (u64)(unsigned long)iocb->ki_obj.user;
	event->data = iocb->ki_user_data;
	event->res = res;
	event->res2 = res2;

	kunmap_atomic(ev_page);
	flush_dcache_page(ctx->ring_pages[pos / AIO_EVENTS_PER_PAGE]);

	pr_debug("%p[%u]: %p: %p %Lx %lx %lx\n",
		 ctx, tail, iocb, iocb->ki_obj.user, iocb->ki_user_data,
		 res, res2);

	/* after flagging the request as done, we
	 * must never even look at it again
	 */
	smp_wmb();	/* make event visible before updating tail */

	ctx->tail = tail;

	ring = kmap_atomic(ctx->ring_pages[0]);
	ring->tail = tail;
	put_aio_ring_event(event);
	kunmap_atomic(ring);
	flush_dcache_page(ctx->ring_pages[0]);

	spin_unlock_irqrestore(&ctx->completion_lock, flags);

	pr_debug("added to ring %p at [%u]\n", iocb, tail);

	/*
	 * Check if the user asked us to deliver the result through an
	 * eventfd. The eventfd_signal() function is safe to be called
	 * from IRQ context.
	 */
	if (iocb->ki_eventfd != NULL)
		eventfd_signal(iocb->ki_eventfd, 1);

put_rq:
	/* everything turned out well, dispose of the aiocb. */
	aio_put_req(iocb);

	/*
	 * We have to order our ring_info tail store above and test
	 * of the wait list below outside the wait lock.  This is
	 * like in wake_up_bit() where clearing a bit has to be
	 * ordered with the unlocked test.
	 */
	smp_mb();

	if (waitqueue_active(&ctx->wait))
		wake_up(&ctx->wait);

	rcu_read_unlock();
}
EXPORT_SYMBOL(aio_complete);

/* aio_read_evt
 *	Pull an event off of the ioctx's event ring.  Returns the number of 
 *	events fetched (0 or 1 ;-)
 *	FIXME: make this use cmpxchg.
 *	TODO: make the ringbuffer user mmap()able (requires FIXME).
 */
static int aio_read_evt(struct kioctx *ioctx, struct io_event *ent)
{
	struct aio_ring *ring;
	unsigned head, pos;
	long ret = 0;
	int copy_ret;

	mutex_lock(&ctx->ring_lock);

	ring = kmap_atomic(ctx->ring_pages[0]);
	head = ring->head;
	kunmap_atomic(ring);
	return ret;
	pr_debug("h%u t%u m%u\n", head, ctx->tail, ctx->nr_events);

	if (head == ctx->tail)
		goto out;

	to->timed_out = 1;
	wake_up_process(to->p);
}

		avail = (head <= ctx->tail ? ctx->tail : ctx->nr_events) - head;
		if (head == ctx->tail)
			break;

static inline void set_timeout(long start_jiffies, struct aio_timeout *to,
			       const struct timespec *ts)
{
	to->timer.expires = start_jiffies + timespec_to_jiffies(ts);
	if (time_after(to->timer.expires, jiffies))
		add_timer(&to->timer);
	else
		to->timed_out = 1;
}

		pos = head + AIO_EVENTS_OFFSET;
		page = ctx->ring_pages[pos / AIO_EVENTS_PER_PAGE];
		pos %= AIO_EVENTS_PER_PAGE;

static int read_events(struct kioctx *ctx,
			long min_nr, long nr,
			struct io_event __user *event,
			struct timespec __user *timeout)
{
	long			start_jiffies = jiffies;
	struct task_struct	*tsk = current;
	DECLARE_WAITQUEUE(wait, tsk);
	int			ret;
	int			i = 0;
	struct io_event		ent;
	struct aio_timeout	to;
	int			retry = 0;

	/* needed to zero any padding within an entry (there shouldn't be 
	 * any, but C is fun!
	 */
	memset(&ent, 0, sizeof(ent));
retry:
	ret = 0;
	while (likely(i < nr)) {
		ret = aio_read_evt(ctx, &ent);
		if (unlikely(ret <= 0))
			break;

		dprintk("read event: %Lx %Lx %Lx %Lx\n",
			ent.data, ent.obj, ent.res, ent.res2);

		/* Could we split the check in two? */
		ret = -EFAULT;
		if (unlikely(copy_to_user(event, &ent, sizeof(ent)))) {
			dprintk("aio: lost an event due to EFAULT.\n");
			break;
		}

		ret += avail;
		head += avail;
		head %= ctx->nr_events;
	}

	ring = kmap_atomic(ctx->ring_pages[0]);
	ring->head = head;
	kunmap_atomic(ring);
	flush_dcache_page(ctx->ring_pages[0]);

	pr_debug("%li  h%u t%u\n", ret, head, ctx->tail);

	atomic_sub(ret, &ctx->reqs_active);
out:
	mutex_unlock(&ctx->ring_lock);

	/* racey check, but it gets redone */
	if (!retry && unlikely(!list_empty(&ctx->run_list))) {
		retry = 1;
		aio_run_all_iocbs(ctx);
		goto retry;
	}

	init_timeout(&to);
	if (timeout) {
		struct timespec	ts;
		ret = -EFAULT;
		if (unlikely(copy_from_user(&ts, timeout, sizeof(ts))))
			goto out;

		set_timeout(start_jiffies, &to, &ts);
	}

	while (likely(i < nr)) {
		add_wait_queue_exclusive(&ctx->wait, &wait);
		do {
			set_task_state(tsk, TASK_INTERRUPTIBLE);
			ret = aio_read_evt(ctx, &ent);
			if (ret)
				break;
			if (min_nr <= i)
				break;
			if (unlikely(ctx->dead)) {
				ret = -EINVAL;
				break;
			}
			if (to.timed_out)	/* Only check after read evt */
				break;
			/* Try to only show up in io wait if there are ops
			 *  in flight */
			if (ctx->reqs_active)
				io_schedule();
			else
				schedule();
			if (signal_pending(tsk)) {
				ret = -EINTR;
				break;
			}
			/*ret = aio_read_evt(ctx, &ent);*/
		} while (1) ;

		set_task_state(tsk, TASK_RUNNING);
		remove_wait_queue(&ctx->wait, &wait);

		if (unlikely(ret <= 0))
			break;

		ret = -EFAULT;
		if (unlikely(copy_to_user(event, &ent, sizeof(ent)))) {
			dprintk("aio: lost an event due to EFAULT.\n");
			break;
		}

		/* Good, event copied to userland, update counts. */
		event ++;
		i ++;
	}

	if (timeout)
		clear_timeout(&to);
out:
	destroy_timer_on_stack(&to.timer);
	return i ? i : ret;
}

/* Take an ioctx and remove it from the list of ioctx's.  Protects 
 * against races with itself via ->dead.
 */
static void io_destroy(struct kioctx *ioctx)
{
	struct mm_struct *mm = current->mm;
	int was_dead;

	/* delete the entry from the list is someone else hasn't already */
	spin_lock(&mm->ioctx_lock);
	was_dead = ioctx->dead;
	ioctx->dead = 1;
	hlist_del_rcu(&ioctx->list);
	spin_unlock(&mm->ioctx_lock);

	dprintk("aio_release(%p)\n", ioctx);
	if (likely(!was_dead))
		put_ioctx(ioctx);	/* twice for the list */

	kill_ctx(ioctx);

	/*
	 * Wake up any waiters.  The setting of ctx->dead must be seen
	 * by other CPUs at this point.  Right now, we rely on the
	 * locking done by the above calls to ensure this consistency.
	 */
	wake_up_all(&ioctx->wait);
}

/* sys_io_setup:
 *	Create an aio_context capable of receiving at least nr_events.
 *	ctxp must not point to an aio_context that already exists, and
 *	must be initialized to 0 prior to the call.  On successful
 *	creation of the aio_context, *ctxp is filled in with the resulting 
 *	handle.  May fail with -EINVAL if *ctxp is not initialized,
 *	if the specified nr_events exceeds internal limits.  May fail 
 *	with -EAGAIN if the specified nr_events exceeds the user's limit 
 *	of available events.  May fail with -ENOMEM if insufficient kernel
 *	resources are available.  May fail with -EFAULT if an invalid
 *	pointer is passed for ctxp.  Will fail with -ENOSYS if not
 *	implemented.
 */
SYSCALL_DEFINE2(io_setup, unsigned, nr_events, aio_context_t __user *, ctxp)
{
	struct kioctx *ioctx = NULL;
	unsigned long ctx;
	long ret;

	ret = get_user(ctx, ctxp);
	if (unlikely(ret))
		goto out;

	ret = -EINVAL;
	if (unlikely(ctx || nr_events == 0)) {
		pr_debug("EINVAL: io_setup: ctx %lu nr_events %u\n",
		         ctx, nr_events);
		goto out;
	}

	ioctx = ioctx_alloc(nr_events);
	ret = PTR_ERR(ioctx);
	if (!IS_ERR(ioctx)) {
		ret = put_user(ioctx->user_id, ctxp);
		if (ret)
			io_destroy(ioctx);
		put_ioctx(ioctx);
	}

out:
	return ret;
}

/* sys_io_destroy:
 *	Destroy the aio_context specified.  May cancel any outstanding 
 *	AIOs and block on completion.  Will fail with -ENOSYS if not
 *	implemented.  May fail with -EINVAL if the context pointed to
 *	is invalid.
 */
SYSCALL_DEFINE1(io_destroy, aio_context_t, ctx)
{
	struct kioctx *ioctx = lookup_ioctx(ctx);
	if (likely(NULL != ioctx)) {
		io_destroy(ioctx);
		put_ioctx(ioctx);
		return 0;
	}
	pr_debug("EINVAL: io_destroy: invalid context id\n");
	return -EINVAL;
}

static void aio_advance_iovec(struct kiocb *iocb, ssize_t ret)
{
	struct iovec *iov = &iocb->ki_iovec[iocb->ki_cur_seg];

	BUG_ON(ret <= 0);

	while (iocb->ki_cur_seg < iocb->ki_nr_segs && ret > 0) {
		ssize_t this = min((ssize_t)iov->iov_len, ret);
		iov->iov_base += this;
		iov->iov_len -= this;
		iocb->ki_left -= this;
		ret -= this;
		if (iov->iov_len == 0) {
			iocb->ki_cur_seg++;
			iov++;
		}
	}

	/* the caller should not have done more io than what fit in
	 * the remaining iovecs */
	BUG_ON(ret > 0 && iocb->ki_left == 0);
}

typedef ssize_t (aio_rw_op)(struct kiocb *, const struct iovec *,
			    unsigned long, loff_t);

static ssize_t aio_rw_vect_retry(struct kiocb *iocb, int rw, aio_rw_op *rw_op)
{
	struct file *file = iocb->ki_filp;
	struct address_space *mapping = file->f_mapping;
	struct inode *inode = mapping->host;
	ssize_t ret = 0;

	/* This matches the pread()/pwrite() logic */
	if (iocb->ki_pos < 0)
		return -EINVAL;

	if (rw == WRITE)
		file_start_write(file);
	do {
		ret = rw_op(iocb, &iocb->ki_iovec[iocb->ki_cur_seg],
			    iocb->ki_nr_segs - iocb->ki_cur_seg,
			    iocb->ki_pos);
		if (ret > 0)
			aio_advance_iovec(iocb, ret);

	/* retry all partial writes.  retry partial reads as long as its a
	 * regular file. */
	} while (ret > 0 && iocb->ki_left > 0 &&
		 (rw == WRITE ||
		  (!S_ISFIFO(inode->i_mode) && !S_ISSOCK(inode->i_mode))));
	if (rw == WRITE)
		file_end_write(file);

	/* This means we must have transferred all that we could */
	/* No need to retry anymore */
	if ((ret == 0) || (iocb->ki_left == 0))
		ret = iocb->ki_nbytes - iocb->ki_left;

	/* If we managed to write some out we return that, rather than
	 * the eventual error. */
	if (rw == WRITE
	    && ret < 0 && ret != -EIOCBQUEUED
	    && iocb->ki_nbytes - iocb->ki_left)
		ret = iocb->ki_nbytes - iocb->ki_left;

	return ret;
}

static ssize_t aio_setup_vectored_rw(int rw, struct kiocb *kiocb, bool compat)
{
	ssize_t ret;

	kiocb->ki_nr_segs = kiocb->ki_nbytes;

#ifdef CONFIG_COMPAT
	if (compat)
		ret = compat_rw_copy_check_uvector(rw,
				(struct compat_iovec __user *)kiocb->ki_buf,
				kiocb->ki_nr_segs, 1, &kiocb->ki_inline_vec,
				&kiocb->ki_iovec);
	else
#endif
		ret = rw_copy_check_uvector(rw,
				(struct iovec __user *)kiocb->ki_buf,
				kiocb->ki_nr_segs, 1, &kiocb->ki_inline_vec,
				&kiocb->ki_iovec);
	if (ret < 0)
		return ret;

	/* ki_nbytes now reflect bytes instead of segs */
	kiocb->ki_nbytes = ret;
	return 0;
}

static ssize_t aio_setup_single_vector(int rw, struct kiocb *kiocb)
{
	if (unlikely(!access_ok(!rw, kiocb->ki_buf, kiocb->ki_nbytes)))
		return -EFAULT;

	kiocb->ki_iovec = &kiocb->ki_inline_vec;
	kiocb->ki_iovec->iov_base = kiocb->ki_buf;
	kiocb->ki_iovec->iov_len = kiocb->ki_nbytes;
	kiocb->ki_nr_segs = 1;
	return 0;
}

/*
 * aio_setup_iocb:
 *	Performs the initial checks and aio retry method
 *	setup for the kiocb at the time of io submission.
 */
static ssize_t aio_run_iocb(struct kiocb *req, bool compat)
{
	struct file *file = req->ki_filp;
	ssize_t ret;
	int rw;
	fmode_t mode;
	aio_rw_op *rw_op;

	switch (req->ki_opcode) {
	case IOCB_CMD_PREAD:
	case IOCB_CMD_PREADV:
		mode	= FMODE_READ;
		rw	= READ;
		rw_op	= file->f_op->aio_read;
		goto rw_common;

	case IOCB_CMD_PWRITE:
	case IOCB_CMD_PWRITEV:
		mode	= FMODE_WRITE;
		rw	= WRITE;
		rw_op	= file->f_op->aio_write;
		goto rw_common;
rw_common:
		if (unlikely(!(file->f_mode & mode)))
			return -EBADF;

		if (!rw_op)
			return -EINVAL;

		ret = (req->ki_opcode == IOCB_CMD_PREADV ||
		       req->ki_opcode == IOCB_CMD_PWRITEV)
			? aio_setup_vectored_rw(rw, req, compat)
			: aio_setup_single_vector(rw, req);
		if (ret)
			return ret;

		ret = rw_verify_area(rw, file, &req->ki_pos, req->ki_nbytes);
		if (ret < 0)
			return ret;

		req->ki_nbytes = ret;
		req->ki_left = ret;

		ret = aio_rw_vect_retry(req, rw, rw_op);
		break;

	case IOCB_CMD_FDSYNC:
		if (!file->f_op->aio_fsync)
			return -EINVAL;

		ret = file->f_op->aio_fsync(req, 1);
		break;

	case IOCB_CMD_FSYNC:
		if (!file->f_op->aio_fsync)
			return -EINVAL;

		ret = file->f_op->aio_fsync(req, 0);
		break;

	default:
		pr_debug("EINVAL: no operation provided\n");
		return -EINVAL;
	}

	if (ret != -EIOCBQUEUED) {
		/*
		 * There's no easy way to restart the syscall since other AIO's
		 * may be already running. Just fail this IO with EINTR.
		 */
		if (unlikely(ret == -ERESTARTSYS || ret == -ERESTARTNOINTR ||
			     ret == -ERESTARTNOHAND ||
			     ret == -ERESTART_RESTARTBLOCK))
			ret = -EINTR;
		aio_complete(req, ret, 0);
	}

	return 0;
}

static int io_submit_one(struct kioctx *ctx, struct iocb __user *user_iocb,
			 struct iocb *iocb, bool compat)
{
	struct kiocb *req;
	struct file *file;
	ssize_t ret;

	/* enforce forwards compatibility on users */
	if (unlikely(iocb->aio_reserved1 || iocb->aio_reserved2)) {
		pr_debug("EINVAL: io_submit: reserve field set\n");
		return -EINVAL;
	}

	/* prevent overflows */
	if (unlikely(
	    (iocb->aio_buf != (unsigned long)iocb->aio_buf) ||
	    (iocb->aio_nbytes != (size_t)iocb->aio_nbytes) ||
	    ((ssize_t)iocb->aio_nbytes < 0)
	   )) {
		pr_debug("EINVAL: io_submit: overflow check\n");
		return -EINVAL;
	}

	req = aio_get_req(ctx);
	if (unlikely(!req))
		return -EAGAIN;
	}
	req->ki_filp = file;
	if (iocb->aio_flags & IOCB_FLAG_RESFD) {
		/*
		 * If the IOCB_FLAG_RESFD flag of aio_flags is set, get an
		 * instance of the file* now. The file descriptor must be
		 * an eventfd() fd, and will be signaled for each completed
		 * event using the eventfd_signal() function.
		 */
		req->ki_eventfd = eventfd_ctx_fdget((int) iocb->aio_resfd);
		if (IS_ERR(req->ki_eventfd)) {
			ret = PTR_ERR(req->ki_eventfd);
			req->ki_eventfd = NULL;
			goto out_put_req;
		}
	}

	ret = put_user(KIOCB_KEY, &user_iocb->aio_key);
	if (unlikely(ret)) {
		dprintk("EFAULT: aio_key\n");
		goto out_put_req;
	}

	req->ki_obj.user = user_iocb;
	req->ki_user_data = iocb->aio_data;
	req->ki_pos = iocb->aio_offset;

	req->ki_buf = (char __user *)(unsigned long)iocb->aio_buf;
	req->ki_left = req->ki_nbytes = iocb->aio_nbytes;
	req->ki_opcode = iocb->aio_lio_opcode;

	ret = aio_run_iocb(req, compat);
	if (ret)
		goto out_put_req;

	aio_put_req(req);	/* drop extra ref to req */
	return 0;
out_put_req:
	atomic_dec(&ctx->reqs_active);
	aio_put_req(req);	/* drop extra ref to req */
	aio_put_req(req);	/* drop i/o ref to req */
	return ret;
}

long do_io_submit(aio_context_t ctx_id, long nr,
		  struct iocb __user *__user *iocbpp, bool compat)
{
	struct kioctx *ctx;
	long ret = 0;
	int i = 0;
	struct blk_plug plug;

	if (unlikely(nr < 0))
		return -EINVAL;

	if (unlikely(nr > LONG_MAX/sizeof(*iocbpp)))
		nr = LONG_MAX/sizeof(*iocbpp);

	if (unlikely(!access_ok(VERIFY_READ, iocbpp, (nr*sizeof(*iocbpp)))))
		return -EFAULT;

	ctx = lookup_ioctx(ctx_id);
	if (unlikely(!ctx)) {
		pr_debug("EINVAL: io_submit: invalid context id\n");
		return -EINVAL;
	}

	blk_start_plug(&plug);

	/*
	 * AKPM: should this return a partial result if some of the IOs were
	 * successfully submitted?
	 */
	for (i=0; i<nr; i++) {
		struct iocb __user *user_iocb;
		struct iocb tmp;

		if (unlikely(__get_user(user_iocb, iocbpp + i))) {
			ret = -EFAULT;
			break;
		}

		if (unlikely(copy_from_user(&tmp, user_iocb, sizeof(tmp)))) {
			ret = -EFAULT;
			break;
		}

		ret = io_submit_one(ctx, user_iocb, &tmp, compat);
		if (ret)
			break;
	}
	blk_finish_plug(&plug);

	put_ioctx(ctx);
	return i ? i : ret;
}

/* sys_io_submit:
 *	Queue the nr iocbs pointed to by iocbpp for processing.  Returns
 *	the number of iocbs queued.  May return -EINVAL if the aio_context
 *	specified by ctx_id is invalid, if nr is < 0, if the iocb at
 *	*iocbpp[0] is not properly initialized, if the operation specified
 *	is invalid for the file descriptor in the iocb.  May fail with
 *	-EFAULT if any of the data structures point to invalid data.  May
 *	fail with -EBADF if the file descriptor specified in the first
 *	iocb is invalid.  May fail with -EAGAIN if insufficient resources
 *	are available to queue any iocbs.  Will return 0 if nr is 0.  Will
 *	fail with -ENOSYS if not implemented.
 */
SYSCALL_DEFINE3(io_submit, aio_context_t, ctx_id, long, nr,
		struct iocb __user * __user *, iocbpp)
{
	return do_io_submit(ctx_id, nr, iocbpp, 0);
}

/* lookup_kiocb
 *	Finds a given iocb for cancellation.
 */
static struct kiocb *lookup_kiocb(struct kioctx *ctx, struct iocb __user *iocb,
				  u32 key)
{
	struct list_head *pos;

	assert_spin_locked(&ctx->ctx_lock);

	if (key != KIOCB_KEY)
		return NULL;

	/* TODO: use a hash or array, this sucks. */
	list_for_each(pos, &ctx->active_reqs) {
		struct kiocb *kiocb = list_kiocb(pos);
		if (kiocb->ki_obj.user == iocb)
			return kiocb;
	}
	return NULL;
}

/* sys_io_cancel:
 *	Attempts to cancel an iocb previously passed to io_submit.  If
 *	the operation is successfully cancelled, the resulting event is
 *	copied into the memory pointed to by result without being placed
 *	into the completion queue and 0 is returned.  May fail with
 *	-EFAULT if any of the data structures pointed to are invalid.
 *	May fail with -EINVAL if aio_context specified by ctx_id is
 *	invalid.  May fail with -EAGAIN if the iocb specified was not
 *	cancelled.  Will fail with -ENOSYS if not implemented.
 */
SYSCALL_DEFINE3(io_cancel, aio_context_t, ctx_id, struct iocb __user *, iocb,
		struct io_event __user *, result)
{
	int (*cancel)(struct kiocb *iocb, struct io_event *res);
	struct kioctx *ctx;
	struct kiocb *kiocb;
	u32 key;
	int ret;

	ret = get_user(key, &iocb->aio_key);
	if (unlikely(ret))
		return -EFAULT;

	ctx = lookup_ioctx(ctx_id);
	if (unlikely(!ctx))
		return -EINVAL;

	spin_lock_irq(&ctx->ctx_lock);
	ret = -EAGAIN;
	kiocb = lookup_kiocb(ctx, iocb, key);
	if (kiocb && kiocb->ki_cancel) {
		cancel = kiocb->ki_cancel;
		kiocb->ki_users ++;
		kiocbSetCancelled(kiocb);
	} else
		cancel = NULL;
	spin_unlock_irq(&ctx->ctx_lock);

	if (NULL != cancel) {
		struct io_event tmp;
		pr_debug("calling cancel\n");
		memset(&tmp, 0, sizeof(tmp));
		tmp.obj = (u64)(unsigned long)kiocb->ki_obj.user;
		tmp.data = kiocb->ki_user_data;
		ret = cancel(kiocb, &tmp);
		if (!ret) {
			/* Cancellation succeeded -- copy the result
			 * into the user's buffer.
			 */
			if (copy_to_user(result, &tmp, sizeof(tmp)))
				ret = -EFAULT;
		}
	} else
		ret = -EINVAL;

	put_ioctx(ctx);

	return ret;
}

/* io_getevents:
 *	Attempts to read at least min_nr events and up to nr events from
 *	the completion queue for the aio_context specified by ctx_id. If
 *	it succeeds, the number of read events is returned. May fail with
 *	-EINVAL if ctx_id is invalid, if min_nr is out of range, if nr is
 *	out of range, if timeout is out of range.  May fail with -EFAULT
 *	if any of the memory specified is invalid.  May return 0 or
 *	< min_nr if the timeout specified by timeout has elapsed
 *	before sufficient events are available, where timeout == NULL
 *	specifies an infinite timeout. Note that the timeout pointed to by
 *	timeout is relative.  Will fail with -ENOSYS if not implemented.
 */
SYSCALL_DEFINE5(io_getevents, aio_context_t, ctx_id,
		long, min_nr,
		long, nr,
		struct io_event __user *, events,
		struct timespec __user *, timeout)
{
	struct kioctx *ioctx = lookup_ioctx(ctx_id);
	long ret = -EINVAL;

	if (likely(ioctx)) {
		if (likely(min_nr <= nr && min_nr >= 0))
			ret = read_events(ioctx, min_nr, nr, events, timeout);
		put_ioctx(ioctx);
	}
	asmlinkage_protect(5, ret, ctx_id, min_nr, nr, events, timeout);
	return ret;
}
