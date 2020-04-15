#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "types.h"
#include "headers.h"
#include "io_uring.h"
#include "arch-x86_64.h"
#include "fls.h"
/*
 * Asynchronous IO engine.
 *
 * Two operations: read a page and write a page.
 * Both operations work closely with the page cache.
 *
 * Reading a page will first look if the page is cached. If it is, then it calls the callback synchronously.
 * If the page is not cached, a page will be allocated in the page cache and the callback will be called asynchronously.
 * The data is in callback->lru_entry->page.
 * e.g. aio_read_page_async(fd, page_num, my_callback)
 *      my_callback(cb) {
 *          cb->lru_entry // the page cache metadata of the page where the data has been loaded
 *          cb->lru_entry->page // the page that contains the data
 *      }
 *
 * Writing a page consists in flushing the content of the page cache to disk.
 * It means the page must be in memory, it is not possible to write a non cached page.
 * This could be easilly changed if need be.
 *
 * ASSUMPTIONS:
 *   The page cache is big enough to hold as many pages as concurrent buffered IOs.
 */

/*
 * Non asynchronous calls to ease some things
 */
static __thread char *disk_data;
// API slab.c
void *safe_pread(int fd, off_t offset) {
   if(!disk_data)
      disk_data = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
   int r = pread(fd, disk_data, PAGE_SIZE, offset);
   if(r != PAGE_SIZE)
      perr("pread failed! Read %d instead of %lu (offset %lu)\n", r, PAGE_SIZE, offset);
   return disk_data;
}

/*
 * Async API definition
 */
static int io_setup(unsigned nr, aio_context_t *ctxp) {
	return syscall(__NR_io_setup, nr, ctxp);
}

static int io_submit(aio_context_t ctx, long nr, struct iocb **iocbpp) {
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

static int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
		struct io_event *events, struct timespec *timeout) {
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}


/*
 * Definition of the context of an IO worker thread
 */
struct linked_callbacks {
   struct slab_callback *callback;
   struct linked_callbacks *next;
};
struct aio_engine_context {
   aio_context_t ctx __attribute__((aligned(64)));
   volatile size_t sent_io; //  add io_ctx->iocb aio_read_page_async() aio_write_page_async()
   volatile size_t processed_io; // io_ctx->processed_io += io_ctx->ios_sent_to_disk
   size_t max_pending_io; // entries
   size_t ios_sent_to_disk; // = io_submit
   struct iocb *iocb; // aio_read_page_async() aio_write_page_async()
   struct iocb **iocbs; //  iocb -> iocbs
   struct io_event *events;
   struct linked_callbacks *linked_callbacks;
};
// ioengine call access io_ctx->linked_callbacks
/*
 * After completing IOs we need to call all the "linked callbacks", i.e., reads done to a page that was already in the process of being fetched.
 */
static void aio_process_linked_callbacks(struct aio_engine_context *io_ctx) {
   declare_debug_timer;

   size_t nb_linked = 0;
   start_debug_timer {
      struct linked_callbacks *linked_cb;
      linked_cb = io_ctx->linked_callbacks;
        io_ctx->linked_callbacks = NULL; // reset the list

      while(linked_cb) {
         struct linked_callbacks *next = linked_cb->next;
         struct slab_callback *callback = linked_cb->callback;
         if(callback->lru_entry->contains_data) {
            callback->io_cb(callback);
            free(linked_cb);
         } else { // page has not been prefetched yet, it's likely in the list of pages that will be read during the next kernel call
            linked_cb->next = io_ctx->linked_callbacks;
             io_ctx->linked_callbacks = linked_cb; // re-link our callback
         }
         linked_cb = next;
         nb_linked++;
      }
   } stop_debug_timer(10000, "%lu linked callbacks\n", nb_linked);
}
// ioengine call access io_ctx
/*
 * Loop executed by worker threads
 */
static void aio_worker_do_io(struct aio_engine_context *io_ctx) {
   size_t pending = io_ctx->sent_io - io_ctx->processed_io;
   if(pending == 0) {
       io_ctx->ios_sent_to_disk = 0;
      return;
   }
   /*if(pending > QUEUE_DEPTH)
      pending = QUEUE_DEPTH;*/

   for(size_t i = 0; i < pending; i++) {
      struct slab_callback *callback;
       io_ctx->iocbs[i] = &io_ctx->iocb[(io_ctx->processed_io + i) % io_ctx->max_pending_io];
      callback = (void*)io_ctx->iocbs[i]->aio_data;
      callback->lru_entry->dirty = 0;  // reset the dirty flag *before* sending write orders otherwise following writes might be ignored
                                       // race condition if flag is reset after:
                                       //        io_submit
                                       //        flush done to disk
                                       //              |                           write page (no IO order because dirty = 1, see aio_write_page_async "if(lru_entry->dirty)" condition)
                                       //        complete ios
                                       //        (old value written to disk)

      add_time_in_payload(callback, 3);
   }

   // Submit requests to the kernel
   int ret = io_submit(io_ctx->ctx, pending, io_ctx->iocbs);
   if (ret != pending)
      perr("Couldn't submit all io requests! %d submitted / %lu (%lu sent, %lu processed)\n", ret, pending, io_ctx->sent_io, io_ctx->processed_io);
    io_ctx->ios_sent_to_disk = ret;
}


/*
 * do_io = wait for all requests to be completed
 */
void do_io(void) {
   // TODO
}
// ioengine call
/* We need a unique hash for each page for the page cache */
static uint64_t get_hash_for_page(int fd, uint64_t page_num) {
   return (((uint64_t)fd)<<40LU)+page_num; // Works for files less than 40EB
}
// api slab.c freelist.c access io_ctx
/* Enqueue a request to read a page */
char *aio_read_page_async(struct slab_callback *callback) {
   int alread_used;
   struct lru *lru_entry;
   void *disk_page;
   uint64_t page_num = item_page_num(callback->slab, callback->slab_idx);
   struct aio_engine_context *io_ctx = (struct aio_engine_context *)get_io_context(callback->slab->ctx);
   uint64_t hash = get_hash_for_page(callback->slab->fd, page_num);

   alread_used = get_page(get_pagecache(callback->slab->ctx), hash, &disk_page, &lru_entry);
   callback->lru_entry = lru_entry;
   if(lru_entry->contains_data) {   // content is cached already
      callback->io_cb(callback);       // call the callback directly
      return disk_page;
   }

   if(alread_used) { // Somebody else is already prefetching the same page!
      struct linked_callbacks *linked_cb = malloc(sizeof(*linked_cb));
      linked_cb->callback = callback;
      linked_cb->next = io_ctx->linked_callbacks;
       io_ctx->linked_callbacks = linked_cb; // link our callback
      return NULL;
   }

   int buffer_idx = io_ctx->sent_io % io_ctx->max_pending_io;
   struct iocb *_iocb = &io_ctx->iocb[buffer_idx];
   memset(_iocb, 0, sizeof(*_iocb));
   _iocb->aio_fildes = callback->slab->fd;
   _iocb->aio_lio_opcode = IOCB_CMD_PREAD;
   _iocb->aio_buf = (uint64_t)disk_page;
   _iocb->aio_data = (uint64_t)callback;
   _iocb->aio_offset = page_num * PAGE_SIZE;
   _iocb->aio_nbytes = PAGE_SIZE;
   if(io_ctx->sent_io - io_ctx->processed_io >= io_ctx->max_pending_io)
      die("Sent %lu ios, processed %lu (> %lu waiting), IO buffer is too full!\n", io_ctx->sent_io, io_ctx->processed_io, io_ctx->max_pending_io);
   io_ctx->sent_io++;

   return NULL;
}
// API slab.c  access io_ctx
/* Enqueue a request to write a page, the lru entry must contain the content of the page (obviously) */
char *aio_write_page_async(struct slab_callback *callback) {
   struct aio_engine_context *io_ctx = (struct aio_engine_context *)get_io_context(callback->slab->ctx);
   struct lru *lru_entry = callback->lru_entry;
   void *disk_page = lru_entry->page;
   uint64_t page_num = item_page_num(callback->slab, callback->slab_idx);

   if(!lru_entry->contains_data) {  // page is not in RAM! Abort!
      die("WTF?\n");
   }

   if(lru_entry->dirty) { // this is the second time we write the page, which means it already has been queued for writting
      struct linked_callbacks *linked_cb;
      linked_cb = malloc(sizeof(*linked_cb));
      linked_cb->callback = callback;
      linked_cb->next = io_ctx->linked_callbacks;
       io_ctx->linked_callbacks = linked_cb; // link our callback
      return disk_page;
   }

   lru_entry->dirty = 1;

   int buffer_idx = io_ctx->sent_io % io_ctx->max_pending_io;
   struct iocb *_iocb = &io_ctx->iocb[buffer_idx];
   memset(_iocb, 0, sizeof(*_iocb));
   _iocb->aio_fildes = callback->slab->fd;
   _iocb->aio_lio_opcode = IOCB_CMD_PWRITE;
   _iocb->aio_buf = (uint64_t)disk_page;
   _iocb->aio_data = (uint64_t)callback;
   _iocb->aio_offset = page_num * PAGE_SIZE;
   _iocb->aio_nbytes = PAGE_SIZE;
   if(io_ctx->sent_io - io_ctx->processed_io >= io_ctx->max_pending_io)
      die("Sent %lu ios, processed %lu (> %lu waiting), IO buffer is too full!\n", io_ctx->sent_io, io_ctx->processed_io, io_ctx->max_pending_io);
   io_ctx->sent_io++;

   return NULL;
}
// API access io_ctx
/*
 * Init an IO worker
 */
void *aio_worker_ioengine_init(struct slab_context *ctx) {
   int ret;
   struct aio_engine_context *io_ctx = calloc(1, sizeof(*io_ctx));
//    io_ctx->max_pending_io = ctx->max_pending_callbacks * 2; //  2x max_pending_callbacks/requests
    io_ctx->max_pending_io = 2*QUEUE_DEPTH; //  2x max_pending_callbacks/requests

    io_ctx->iocb = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->iocb));
    io_ctx->iocbs = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->iocbs));
    io_ctx->events = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->events));

   ret = io_setup(io_ctx->max_pending_io, &io_ctx->ctx);
   if(ret < 0)
      perr("Cannot create aio setup\n");
   ctx->io_ctx = io_ctx;
   return 0;
}
// API, access io_ctx
/* Enqueue requests into iocbs */
void aio_worker_ioengine_enqueue_ios(struct slab_context *ctx) {
    aio_worker_do_io(ctx->io_ctx); // Process IO queue // todo
}
// API access io_ctx
/* Get processed requests from disk and call callbacks */
void aio_worker_ioengine_get_completed_ios(struct slab_context *ctx) {
   int ret = 0;
   struct aio_engine_context *io_ctx = ctx->io_ctx;
   declare_debug_timer;

   if(io_ctx->ios_sent_to_disk == 0)
      return;

   start_debug_timer {
      ret = io_getevents(io_ctx->ctx, io_ctx->ios_sent_to_disk - ret, io_ctx->ios_sent_to_disk - ret, &io_ctx->events[ret], NULL);
      if(ret != io_ctx->ios_sent_to_disk)
         die("Problem: only got %d answers out of %lu enqueued IO requests\n", ret, io_ctx->ios_sent_to_disk);
   } stop_debug_timer(10000, "io_getevents took more than 10ms!!");
}

// api  access io_ctx
void aio_worker_ioengine_process_completed_ios(struct slab_context *ctx) {
    struct aio_engine_context *io_ctx = ctx->io_ctx;

    int ret = io_ctx->ios_sent_to_disk;
   declare_debug_timer;

   if(io_ctx->ios_sent_to_disk == 0)
      return;


   start_debug_timer {
      // Enqueue completed IO requests
      for(size_t i = 0; i < ret; i++) {
         struct iocb *cb = (void*)io_ctx->events[i].obj;
         struct slab_callback *callback = (void*)cb->aio_data;
         assert(io_ctx->events[i].res == 4096); // otherwise page hasn't been read
         callback->lru_entry->contains_data = 1;
         //callback->lru_entry->dirty = 0; // done before
         callback->io_cb(callback);
      }

      // We might have "linked callbacks" so process them
        aio_process_linked_callbacks(io_ctx);
   } stop_debug_timer(10000, "rest of aio_worker_ioengine_process_completed_ios (%d requests)", ret);

   // Ok, now the main thread can push more requests
    io_ctx->processed_io += io_ctx->ios_sent_to_disk;
}

//int aio_io_pending(struct aio_engine_context *ctx) {
//   return ctx->sent_io - ctx->processed_io;
//}
// api, ioengine call, access io_ctx
int aio_io_pending(struct slab_context *ctx) {
    struct aio_engine_context * io_ctx;
    io_ctx = (struct aio_engine_context *)ctx->io_ctx;
    return io_ctx->sent_io - io_ctx->processed_io;
}


struct ioengine_ops  libaio_ops = {
    .name = "libaio",
    .worker_ioengine_init = aio_worker_ioengine_init,
//    .safe_pread = safe_pread,
    .read_page_async = aio_read_page_async,
    .write_page_async = aio_write_page_async,
    .io_pending = aio_io_pending,
    .worker_ioengine_enqueue_ios = aio_worker_ioengine_enqueue_ios,
    .worker_ioengine_get_completed_ios = aio_worker_ioengine_get_completed_ios,
    .worker_ioengine_process_completed_ios = aio_worker_ioengine_process_completed_ios,
};


/*
 * io-uring ioengine implementaiion.
 */
struct iovec
{
    void	*iov_base;  /* Base address of a memory region for input or output */
    size_t	 iov_len;   /* The size of the memory pointed to by iov_base */
};


// io_uring.h

struct ioring_options {
    void *pad;
    unsigned int hipri;
//    unsigned int cmdprio_percentage;
    unsigned int fixedbufs;
    unsigned int registerfiles;
    unsigned int sqpoll_thread;
    unsigned int sqpoll_set;
    unsigned int sqpoll_cpu;
    unsigned int sqpoll_idle_set;
    unsigned int sqpoll_idle;
    unsigned int nonvectored;
    unsigned int uncached;
//    unsigned int td_o_iodepth; // actual size of sqe

    
    unsigned int td_o_odirect;
    unsigned int td_o_sync_file_range;
//    unsigned int td_o_nr_files;
    unsigned int td_o_open_files;
    unsigned int td_o_iodepth_batch_complete_min;
};

struct io_sq_ring {
    unsigned *head;
    unsigned *tail;
    unsigned *ring_mask;
    unsigned *ring_entries;
    unsigned *flags;
    unsigned *array;
};

struct io_cq_ring {
    unsigned *head;
    unsigned *tail;
    unsigned *ring_mask;
    unsigned *ring_entries;
    struct io_uring_cqe *cqes;
};

struct ioring_mmap {
    void *ptr;
    size_t len;
};

struct ioring_data {
    int ring_fd;

//    struct io_u **io_u_index;

    int *fds;

    struct io_sq_ring sq_ring; // ~=iocbs
//    unsigned int sq_ring_enqueue_tail; == sent_io
    struct io_uring_sqe *sqes; // ~=iocb


    struct iovec *iovecs;
    unsigned sq_ring_mask;
    unsigned sq_entries;
    unsigned cq_entries;
    struct io_cq_ring cq_ring; // ~=io_events
    unsigned cq_ring_mask;
    unsigned int td_o_iodepth; // actual size of sqe
    struct slab_callback **reaped_callbacks;
    int queued;
    int cq_ring_off;
//    unsigned iodepth;
//    bool ioprio_class_set;
//    bool ioprio_set;

    struct ioring_mmap mmap[3];

    volatile size_t sent_io; //  add io_ctx->iocb aio_read_page_async() aio_write_page_async()
    volatile size_t processed_io; // io_ctx->processed_io += io_ctx->ios_sent_to_disk
    size_t max_pending_io; // = iodepth  != td_o_iodepth
    size_t ios_sent_to_disk; // = io_submit
//    struct iocb *iocb; // aio_read_page_async() aio_write_page_async()
//    struct iocb **iocbs; //  iocb -> iocbs
//    struct io_event *events;
    struct linked_callbacks *linked_callbacks;

};

static struct ioring_options io_uring_options ={
        .hipri=1,
//        .cmdprio_percentage=1, ioprio = 0 for all
        .fixedbufs=0, // fixme support fixedbufs?
        .registerfiles=1, //  sqpoll_thread need registerfiles
        .sqpoll_thread=1,
        .sqpoll_set=1,
        .sqpoll_cpu=7, // poll cpu, valid if .sqpoll_set==1
        .sqpoll_idle_set=1,
        .sqpoll_idle=1000, //idle time in msec, valid if .sqpoll_idle_set==1
        .nonvectored=1, //??? nonvectored looks normal
        .uncached=1,
//        .td_o_iodepth=64, // 123 copyed to max_pending_io roundtopower2
};


// io_uring.c


static const int ddir_to_op[2][2] = {
        { IORING_OP_READV, IORING_OP_READ },
        { IORING_OP_WRITEV, IORING_OP_WRITE }
};

static const int fixed_ddir_to_op[2] = {
        IORING_OP_READ_FIXED,
        IORING_OP_WRITE_FIXED
};


static int io_uring_enter(struct ioring_data *ld, unsigned int to_submit,
                          unsigned int min_complete, unsigned int flags)
{
    return syscall(__NR_io_uring_enter, ld->ring_fd, to_submit,
                   min_complete, flags, NULL, 0);
}
static void iouring_process_linked_callbacks(struct ioring_data *io_ctx) {
    declare_debug_timer;

    size_t nb_linked = 0;
    start_debug_timer {
        struct linked_callbacks *linked_cb;
        linked_cb = io_ctx->linked_callbacks;
        io_ctx->linked_callbacks = NULL; // reset the list

        while(linked_cb) {
            struct linked_callbacks *next = linked_cb->next;
            struct slab_callback *callback = linked_cb->callback;
            if(callback->lru_entry->contains_data) {
                callback->io_cb(callback);
                free(linked_cb);
            } else { // page has not been prefetched yet, it's likely in the list of pages that will be read during the next kernel call
                linked_cb->next = io_ctx->linked_callbacks;
                io_ctx->linked_callbacks = linked_cb; // re-link our callback
            }
            linked_cb = next;
            nb_linked++;
        }
    } stop_debug_timer(10000, "%lu linked callbacks\n", nb_linked);
}

//static int fio_ioring_prep(struct slab_callback *callback)
//{
////    struct ioring_data *ld = ctx->io_ctx;
//    struct ioring_data *io_ctx = (struct ioring_data *)get_io_context(callback->slab->ctx);
//
//    struct ioring_options *o = &io_uring_options;
//    struct fio_file *f = io_u->file;
//    struct io_uring_sqe *sqe;
//    unsigned sqe_len=0;
//    int buffer_idx = io_ctx->sent_io % io_ctx->max_pending_io;
//
//    sqe = &io_ctx->sqes[  buffer_idx ]; // for iod=1. idx=0
//
//    /* zero out fields not used in this submission */
//    memset(sqe, 0, sizeof(*sqe));
//
//    if (o->registerfiles) {
//        sqe->fd = f->engine_pos;
//        sqe->flags = IOSQE_FIXED_FILE;
//    } else {
//        sqe->fd = f->fd;
//    }
//
//    if (io_u->ddir == DDIR_READ || io_u->ddir == DDIR_WRITE) {
//        if (o->fixedbufs) {
//            sqe->opcode = fixed_ddir_to_op[io_u->ddir];
//            sqe->addr = (unsigned long) io_u->xfer_buf;
//            sqe->len = io_u->xfer_buflen;
//            sqe->buf_index = io_u->index % o->td_o_iodepth;
//        } else {
//            sqe->opcode = ddir_to_op[io_u->ddir][!!o->nonvectored];
//            if (o->nonvectored) {
//                sqe->addr = (unsigned long)
//                        io_ctx->iovecs[io_u->index % o->td_o_iodepth].iov_base;
//                sqe->len = io_ctx->iovecs[io_u->index % o->td_o_iodepth].iov_len;
//            } else {
//                sqe->addr = (unsigned long) &io_ctx->iovecs[io_u->index % o->td_o_iodepth];
//                sqe->len = 1;
//            }
//        }
//        if (!o->td_o_odirect && o->uncached)
//            sqe->rw_flags = RWF_UNCACHED;
//        sqe->ioprio = 0;
////        if (ld->ioprio_class_set)
////            sqe->ioprio = td->o.ioprio_class << 13;
////        if (ld->ioprio_set)
////            sqe->ioprio |= td->o.ioprio;
//        sqe->off = io_u->offset;
//    } else if (ddir_sync(io_u->ddir)) {
//        if (io_u->ddir == DDIR_SYNC_FILE_RANGE) {
//            sqe->off = f->first_write;
//            sqe->len = f->last_write - f->first_write;
//            sqe->sync_range_flags = o->td_o_sync_file_range;
//            sqe->opcode = IORING_OP_SYNC_FILE_RANGE;
//        } else {
//            if (io_u->ddir == DDIR_DATASYNC)
//                sqe->fsync_flags |= IORING_FSYNC_DATASYNC;
//            sqe->opcode = IORING_OP_FSYNC;
//        }
//    }
//
//    sqe->user_data = (unsigned long) io_u;
//    pdebug("sqe->len %d", sqe->len );
//    sqe_len = sqe->len;
//    return 0;
//}

static struct slab_callback * fio_ioring_event(struct slab_context *ctx, int event)
{
    struct ioring_data *ld = ctx->io_ctx;
    struct io_uring_cqe *cqe;
//    struct io_u *io_u;
    unsigned index;
    unsigned cqe_res;
    struct slab_callback * callback;
    index = (event + ld->cq_ring_off) & ld->cq_ring_mask; // cq-ring-off = cqring->head
    pdebug( "segfault2 event %d index %d, ld->cq_ring_off %d\n", event, index,ld->cq_ring_off );

    cqe = &ld->cq_ring.cqes[index];
    pdebug( "segfault2 cqe get \n", index);

    callback = (struct slab_callback *) (uintptr_t) cqe->user_data;
    assert(callback);
    pdebug( "segfault2 io_u get \n", index);
    pdebug( "segfault2 cqe->user_data mem %p;\n", cqe->user_data);

//    pdebug( "segfault2 cqe->user_data io_u mem %p; io_u->xfer_buflen %d; cqe->res: %d; \n", io_u, io_u->xfer_buflen, cqe->res);
//    pdebug( "segfault2 cqe->user_data io_u mem %p; \n", io_u);
    cqe_res = cqe->res;
    if (cqe->res != PAGE_SIZE) {

//        pdebug( "segfault2 xfer_buflen cqe->res len disagree\n", io_u);
//
//        if (cqe->res > io_u->xfer_buflen)
//            io_u->error = -cqe->res;
//        else
//            io_u->resid = io_u->xfer_buflen - cqe->res;
        die("cqe->res returns %d, should read a single PAGE_SIZE", cqe->res);
    }

    return callback;
}

static int fio_ioring_cqring_reap(struct slab_context *ctx, unsigned int events,
                                  unsigned int max,struct slab_callback ** reaped_callbacks)
{
    struct ioring_data *ld = ctx->io_ctx;
    struct io_cq_ring *ring = &ld->cq_ring;
    unsigned head, reaped = 0;

    head = *ring->head;
    do {
        read_barrier();
        if (head == *ring->tail){
            pdebug( "fio_ioring_cqring_reap: head==tail exit\n");
            break;
        }
        reaped_callbacks[events+reaped] = fio_ioring_event(ctx,events+reaped);
        reaped++;
        head++;
        pdebug( "fio_ioring_cqring_reap: head++ reaped++\n");
    } while (reaped + events < max);

    *ring->head = head;
    write_barrier();
    return reaped;
}

static int fio_ioring_getevents(struct slab_context *ctx, unsigned int min,
                                unsigned int max,struct slab_callback ** reaped_callbacks)//, const struct timespec *t)
{
    struct ioring_data *ld = ctx->io_ctx;
    struct ioring_options *o = &io_uring_options;
    unsigned actual_min = o->td_o_iodepth_batch_complete_min == 0 ? 0 : min;
    struct io_cq_ring *ring = &ld->cq_ring;
    struct io_sq_ring *sq_ring = &ld->sq_ring;
//    reaped_callbacks = calloc(max, sizeof(struct slab_callback *) );
    unsigned events = 0;
    unsigned  sqe_idx = 0;
    sqe_idx = sq_ring->array[0];
    int r = 0;
    unsigned sqring_head, sqring_tail, io_u_idx;

    while(*(sq_ring->head) != *(sq_ring->tail)){
        nop;
    }
    sqring_head = *(sq_ring->head);
    sqring_tail = *(sq_ring->tail);
    ld->cq_ring_off = *ring->head;
    io_u_idx = sq_ring->array[*ring->tail & ld->sq_ring_mask];
    do {
        r = fio_ioring_cqring_reap(ctx, events, max,reaped_callbacks);
        if (r) {
            events += r;
            if (actual_min != 0)
                actual_min -= r;
            continue;
        }

        if (!o->sqpoll_thread) {
            r = io_uring_enter(ld, 0, actual_min,
                               IORING_ENTER_GETEVENTS);
            if (r < 0) {
                if (errno == EAGAIN || errno == EINTR)
                    continue;
                die("%d %s\n", errno,"io_uring_enter");
                break;
            }
        }
    } while (events < min);

    return r < 0 ? r : events; //reaped events
}

//static void fio_ioring_prio_prep(struct slab_context *ctx, struct io_u *io_u)
//{
//    struct ioring_options *o = &io_uring_options;
//    struct ioring_data *ld = ctx->io_ctx;
//    if (rand_between(&td->prio_state, 0, 99) < o->cmdprio_percentage) {
//        ld->sqes[io_u->index% o->td_o_iodepth].ioprio = IOPRIO_CLASS_RT << IOPRIO_CLASS_SHIFT;
//        io_u->flags |= IO_U_F_PRIORITY;
//    }
//    return;
//}

//static enum fio_q_status fio_ioring_queue(struct slab_context *ctx,
//                                          struct io_u *io_u)
//{
//    struct ioring_data *ld = ctx->io_ctx;
//    struct io_sq_ring *ring = &ld->sq_ring;
//    struct ioring_options *o = &io_uring_options;
//    unsigned tail, next_tail;
//
////    fio_ro_check(td, io_u);
//
//    if (ld->queued == ld->max_pending_io)
//        return FIO_Q_BUSY;
//
//    if (io_u->ddir == DDIR_TRIM) {
//        if (ld->queued)
//            return FIO_Q_BUSY;
//
//        do_io_u_trim(td, io_u);
//        io_u_mark_submit(td, 1);
//        io_u_mark_complete(td, 1);
//        return FIO_Q_COMPLETED;
//    }
//
//    tail = *ring->tail;
//    next_tail = tail + 1;
//    read_barrier();
//    if (next_tail == *ring->head)
//        return FIO_Q_BUSY;
//
////    if (o->cmdprio_percentage)
////        fio_ioring_prio_prep(td, io_u);
//    ring->array[tail & ld->sq_ring_mask] = io_u->index % o->td_o_iodepth;
//
//
//    *ring->tail = next_tail;
//    write_barrier();
//
//    ld->queued++;
//    return FIO_Q_QUEUED;
//}
//
//static void fio_ioring_queued(struct slab_context *ctx, int start, int nr)
//{
//    struct ioring_data *ld = ctx->io_ctx;
//    struct timespec now;
//
//    if (!fio_fill_issue_time(td))
//        return;
//
//    fio_gettime(&now, NULL);
//
//    while (nr--) {
//        struct io_sq_ring *ring = &ld->sq_ring;
//        int index = ring->array[start & ld->sq_ring_mask];
//        struct io_u *io_u = ld->io_u_index[index];
//
//        memcpy(&io_u->issue_time, &now, sizeof(now));
////        io_u_queued(td, io_u); // submit latency
//
//        start++;
//    }
//}

static int fio_ioring_commit(struct slab_context *ctx)
{
    struct ioring_data *io_ctx = ctx->io_ctx;
    struct ioring_options *o = &io_uring_options;
    int ret,ios_sent_to_disk=0;
//    *sq_ring->tail = io_ctx->sent_io;

    write_barrier();

    if (!io_ctx->queued)
        return 0;

    /*
     * Kernel side does submission. just need to check if the ring is
     * flagged as needing a kick, if so, call io_uring_enter(). This
     * only happens if we've been idle too long.
     */
    if (o->sqpoll_thread) {
        struct io_sq_ring *ring = &io_ctx->sq_ring;

        read_barrier();
        if (*ring->flags & IORING_SQ_NEED_WAKEUP)
            io_uring_enter(io_ctx, io_ctx->queued, 0,
                           IORING_ENTER_SQ_WAKEUP);
        // pretend submitted
        ios_sent_to_disk = io_ctx->queued;
        io_ctx->queued = 0;
        return ios_sent_to_disk;
    }

    do {
//        unsigned start = *io_ctx->sq_ring.head;
        long nr = io_ctx->queued;

        ret = io_uring_enter(io_ctx, nr, 0, IORING_ENTER_GETEVENTS);
        if (ret > 0) {
//            fio_ioring_queued(ctx, start, ret);
//            io_u_mark_submit(td, ret);

            io_ctx->queued -= ret;
            ios_sent_to_disk += ret;
//            ret = 0;
        } else if (!ret) {
//            io_u_mark_submit(td, ret);
            pdebug("io_uring_enter returns 0\n");
            continue;
        } else {
            if (errno == EAGAIN || errno == EINTR) {
//                ret = fio_ioring_cqring_reap(ctx, 0, io_ctx->queued);
//                if (ret)
//                    continue;
//                /* Shouldn't happen */
//                usleep(1);
                continue;
            }
            die("%d %s\n", errno,"io_uring_enter submit");
            break;
        }
    } while (io_ctx->queued);

    return ios_sent_to_disk;
}

//static void fio_ioring_unmap(struct ioring_data *ld)
//{
//    int i;
//
//    for (i = 0; i < ARRAY_SIZE(ld->mmap); i++)
//        munmap(ld->mmap[i].ptr, ld->mmap[i].len);
//    close(ld->ring_fd);
//}

//static void fio_ioring_cleanup(struct slab_context *ctx)
//{
//    struct ioring_data *ld = ctx->io_ctx;
//
//    if (ld) {
////        if (!(td->flags & TD_F_CHILD))
//        fio_ioring_unmap(ld);
//
////        free(ld->io_u_index);
////        free(ld->iovecs);
//        free(ld->fds);
//        free(ld);
//    }
//}

static int fio_ioring_mmap(struct ioring_data *ld, struct io_uring_params *p)
{
    struct io_sq_ring *sring = &ld->sq_ring;
    struct io_cq_ring *cring = &ld->cq_ring;
    void *ptr;

    ld->mmap[0].len = p->sq_off.array + p->sq_entries * sizeof(__u32);
    ptr = mmap(0, ld->mmap[0].len, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_POPULATE, ld->ring_fd,
               IORING_OFF_SQ_RING);
    ld->mmap[0].ptr = ptr;
    sring->head = ptr + p->sq_off.head;
    sring->tail = ptr + p->sq_off.tail;
    sring->ring_mask = ptr + p->sq_off.ring_mask;
    sring->ring_entries = ptr + p->sq_off.ring_entries;
    sring->flags = ptr + p->sq_off.flags;
    sring->array = ptr + p->sq_off.array;
    ld->sq_ring_mask = *sring->ring_mask;
    ld->sq_entries = p->sq_entries;
    ld->cq_entries = p->cq_entries;

    ld->mmap[1].len = p->sq_entries * sizeof(struct io_uring_sqe);
    ld->sqes = mmap(0, ld->mmap[1].len, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, ld->ring_fd,
                    IORING_OFF_SQES);
    ld->mmap[1].ptr = ld->sqes;

    ld->mmap[2].len = p->cq_off.cqes +
                      p->cq_entries * sizeof(struct io_uring_cqe);
    ptr = mmap(0, ld->mmap[2].len, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_POPULATE, ld->ring_fd,
               IORING_OFF_CQ_RING);
    ld->mmap[2].ptr = ptr;
    cring->head = ptr + p->cq_off.head;
    cring->tail = ptr + p->cq_off.tail;
    cring->ring_mask = ptr + p->cq_off.ring_mask;
    cring->ring_entries = ptr + p->cq_off.ring_entries;
    cring->cqes = ptr + p->cq_off.cqes;
    ld->cq_ring_mask = *cring->ring_mask;
    return 0;
}

static int fio_ioring_queue_init(struct ioring_data *ld )
{
//    struct ioring_data *ld = ctx->io_ctx;
    struct ioring_options *o = &io_uring_options;
    int depth = ld->td_o_iodepth;
    struct io_uring_params p;
    int ret;

    memset(&p, 0, sizeof(p));

    if (o->hipri)
        p.flags |= IORING_SETUP_IOPOLL;
    if (o->sqpoll_thread) {
        p.flags |= IORING_SETUP_SQPOLL;
        if (o->sqpoll_set) {
            p.flags |= IORING_SETUP_SQ_AFF;
            p.sq_thread_cpu = o->sqpoll_cpu;
        }
        if (o->sqpoll_idle_set) {
            p.sq_thread_idle = o->sqpoll_idle;
        }
    }

    ret = syscall(__NR_io_uring_setup, depth, &p);
    if (ret < 0)
        return ret;

    ld->ring_fd = ret;

    if (o->fixedbufs) {
        die("fixedbufs not supported");
//        struct rlimit rlim = {
//                .rlim_cur = RLIM_INFINITY,
//                .rlim_max = RLIM_INFINITY,
//        };
//
//        if (setrlimit(RLIMIT_MEMLOCK, &rlim) < 0)
//            return -1;
//
//        ret = syscall(__NR_io_uring_register, ld->ring_fd,
//                      IORING_REGISTER_BUFFERS, ld->iovecs, depth);
//        if (ret < 0)
//            return ret;
    }

    return fio_ioring_mmap(ld, &p);
}

static int fio_ioring_register_files(struct slab_context *ctx)
{
    struct ioring_data *ld = ctx->io_ctx;
    struct ioring_options *o = &io_uring_options;
    struct fio_file *f;
    unsigned int i;
    int ret;

    ld->fds = calloc(ctx->nb_slabs, sizeof(int));

    for(int i=0;i<ctx->nb_slabs;i++) {
//        ret = generic_open_file(td, f);
//        if (ret)
//            goto err;
        ld->fds[i] = ctx->slabs[i]->fd;
//        f->engine_pos = i;
        ctx->slabs[i]->iouring_fixed_fd = i;
    }

    ret = syscall(__NR_io_uring_register, ld->ring_fd,
                  IORING_REGISTER_FILES, ld->fds, ctx->nb_slabs);
    if (ret) {
        err:
        free(ld->fds);
        ld->fds = NULL;
    }

//    /*
//     * Pretend the file is closed again, and really close it if we hit
//     * an error.
//     */
//    for_each_file(td, f, i) {
//        if (ret) {
//            int fio_unused ret2;
//            ret2 = generic_close_file(td, f);
//        } else
//            f->fd = -1;
//    }

    return ret;
}

static int fio_ioring_post_init(struct slab_context *ctx )
{
    struct ioring_data *ld = ctx->io_ctx;
    struct ioring_options *o = &io_uring_options;
//    struct io_u *io_u;
    int err, i;
    pdebug( "fio_ioring_post_init loop iovec +\n");
    pdebug( "segfault11 o->td_o_iodepth: %d\n",ld->td_o_iodepth);

//    for (i = 0; i < o->td_o_iodepth; i++) {
//
//        pdebug( "segfault1 ring_fd: %d  &ld->iovecs[%d] mem_i %p\n", &ld->ring_fd,i ,&ld->iovecs[i]);
//
//        struct iovec *iov = &ld->iovecs[i];
//        pdebug( "segfault1 ring_fd: %d ld->io_u_index[%d] men_i %p\n", &ld->ring_fd,i, ld->io_u_index[i]);
//
//        io_u = ld->io_u_index[i];
//        pdebug( "segfault1 ring_fd: %d io_u->buf %i mem %p \n", &ld->ring_fd,i, io_u->buf );
//
//        iov->iov_base = io_u->buf;
//
//        iov->iov_len = td_max_bs(td);
//
//    }
    pdebug( "fio_ioring_post_init loop iovec -\n");

    pdebug( "fio_ioring_queue_init +\n");

    err = fio_ioring_queue_init(ld); // call io_uring_setup
    pdebug( "fio_ioring_queue_init -\n");

    if (err) {
        die("%d %s\n", errno,"io_queue_init");
        return 1;
    }


    if (o->registerfiles) {
        err = fio_ioring_register_files(ctx);
        if (err) {
            die("%d %s\n", errno,"ioring_register_files");
            return 1;
        }
    }

    return 0;
}

static unsigned roundup_pow2(unsigned depth)
{
    return 1UL << __fls(depth - 1);
}

static struct ioring_data *fio_ioring_init(struct slab_context *ctx)
{
    struct ioring_options *o = &io_uring_options;
    struct ioring_data *ld;
//    struct thread_options *to = &td->o;

    /* sqthread submission requires registered files */
    if (o->sqpoll_thread)
        o->registerfiles = 1;

//    if (o->registerfiles && o->td_o_nr_files != td->o.open_files) {
//        pdebug("fio: io_uring registered files require nr_files to "
//                "be identical to open_files\n");
//        return 1;
//    }

    ld = calloc(1, sizeof(*ld));

    /* ring depth must be a power-of-2 */
//    ld->max_pending_io = o->td_o_iodepth;
    ld->sent_io = 0;
    ld->ios_sent_to_disk=0;
    ld->processed_io = 0;

    ld->td_o_iodepth = roundup_pow2(ld->max_pending_io);

    /* io_u index */
//    ld->io_u_index = calloc(o->td_o_iodepth, sizeof(struct io_u *));
//    ld->iovecs = calloc(o->td_o_iodepth, sizeof(struct iovec));

    return ld;

    /*
     * Check for option conflicts
     */
//    if ((fio_option_is_set(to, ioprio) || fio_option_is_set(to, ioprio_class)) &&
//        o->cmdprio_percentage != 0) {
//        log_err("%s: cmdprio_percentage option and mutually exclusive "
//                "prio or prioclass option is set, exiting\n", to->name);
//        td_verror(td, EINVAL, "fio_io_uring_init");
//        return 1;
//    }

//    if (fio_option_is_set(&td->o, ioprio_class))
//        ld->ioprio_class_set = true;
//    if (fio_option_is_set(&td->o, ioprio))
//        ld->ioprio_set = true;

    return 0;
}

//static int fio_ioring_io_u_init(struct slab_context *ctx, struct io_u *io_u)
//{
//    struct ioring_data *ld = ctx->io_ctx;
//    struct ioring_options *o = &io_uring_options;
//    ld->io_u_index[io_u->index] = io_u ;
//    return 0;
//}

//static int fio_ioring_open_file(struct slab_context *ctx, struct fio_file *f)
//{
//    struct ioring_data *ld = ctx->io_ctx;
//    struct ioring_options *o = &io_uring_options;
//
//    if (!ld || !o->registerfiles)
//        return generic_open_file(td, f);
//
//    f->fd = ld->fds[f->engine_pos];
//    return 0;
//}

//static int fio_ioring_close_file(struct slab_context *ctx, struct fio_file *f)
//{
//    struct ioring_data *ld = ctx->io_ctx;
//    struct ioring_options *o = &io_uring_options;
//
//    if (!ld || !o->registerfiles)
//        return generic_close_file(td, f);
//
//    f->fd = -1;
//    return 0;
//}



void *iouring_worker_ioengine_init(struct slab_context *ctx){
//   die("todo");
    int ret;
    struct ioring_data *io_ctx = calloc(1, sizeof(*io_ctx));
    struct ioring_options *o = &io_uring_options;
//    struct thread_options *to = &td->o;

    /* sqthread submission requires registered files */
    if (o->sqpoll_thread)
        o->registerfiles = 1;

    io_ctx->sent_io = 0;
    io_ctx->ios_sent_to_disk=0;
    io_ctx->processed_io = 0;
    io_ctx->max_pending_io = ctx->max_pending_callbacks * 2; //  2x max_pending_callbacks/requests
    io_ctx->td_o_iodepth = roundup_pow2(io_ctx->max_pending_io);
    io_ctx->iovecs = calloc(io_ctx->td_o_iodepth, sizeof(struct iovec));

//    fio_ioring_init();
    ctx->io_ctx = io_ctx;

    ret = fio_ioring_post_init(ctx);
    if(ret)
        perr("Cannot create aio setup\n");


    return 0;

//    io_ctx->iocb = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->iocb));
//    io_ctx->iocbs = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->iocbs));
//    io_ctx->events = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->events));
//
//    ret = io_setup(io_ctx->max_pending_io, &io_ctx->ctx);
//    if(ret < 0)
//        perr("Cannot create aio setup\n");
//
//    return io_ctx;

//        .init			= fio_ioring_init, // init ioring_data / io_ctx
//        .post_init		= fio_ioring_post_init, // io_uring_setup

}

char *iouring_read_page_async(struct slab_callback *callback){

    int alread_used;
    struct lru *lru_entry;
    void *disk_page;
    uint64_t page_num = item_page_num(callback->slab, callback->slab_idx);
    struct ioring_data *io_ctx = (struct ioring_data *)get_io_context(callback->slab->ctx);
    uint64_t hash = get_hash_for_page(callback->slab->fd, page_num);
    struct io_uring_sqe *sqe;
    struct ioring_options *o = &io_uring_options;
    struct io_sq_ring *sq_ring = &io_ctx->sq_ring;


    alread_used = get_page(get_pagecache(callback->slab->ctx), hash, &disk_page, &lru_entry);
    callback->lru_entry = lru_entry;
    if(lru_entry->contains_data) {   // content is cached already
        callback->io_cb(callback);       // call the callback directly
        return disk_page;
    }

    if(alread_used) { // Somebody else is already prefetching the same page!
        struct linked_callbacks *linked_cb = malloc(sizeof(*linked_cb));
        linked_cb->callback = callback;
        linked_cb->next = io_ctx->linked_callbacks;
        io_ctx->linked_callbacks = linked_cb; // link our callback
        return NULL;
    }



    int buffer_idx = io_ctx->sent_io % io_ctx->max_pending_io;
    sqe = &io_ctx->sqes[  buffer_idx ]; // for iod=1. idx=0 FIXME need wait if sqes is full, avoid overwrite?? sqtail++
    memset(sqe, 0, sizeof(*sqe));
    if (o->registerfiles) {

        sqe->fd = callback->slab->iouring_fixed_fd;
        sqe->flags = IOSQE_FIXED_FILE;
//        die("registerfiles not implemented");
    } else {
        sqe->flags = 0;
        sqe->fd = callback->slab->fd;
    }

    if (o->fixedbufs) {
        sqe->opcode = IORING_OP_READ_FIXED;
        sqe->addr = (unsigned long) disk_page; // or (uint64_t)??
        sqe->len = PAGE_SIZE;
        sqe->buf_index = 0; //index
        die("fixedbufs not implemented");

    } else {
        // main branch
        if (o->nonvectored) {// fixme
            sqe->opcode = IORING_OP_READV;
//            sqe->addr = (unsigned long) disk_page; // or (uint64_t)??
//            sqe->addr = (unsigned long) malloc(PAGE_SIZE);
//            sqe->addr = (unsigned long) disk_page; // or (uint64_t)?? fixme
            assert(disk_page!=NULL);
            pdebug2("disk_page %p",disk_page);
            io_ctx->iovecs[buffer_idx].iov_base = disk_page;
            io_ctx->iovecs[buffer_idx].iov_len = PAGE_SIZE;
            sqe->addr = &io_ctx->iovecs[buffer_idx];
            sqe->len = 1;
            sqe->buf_index = 0;
        } else {
            sqe->opcode = IORING_OP_READV;
            sqe->addr = NULL;
            sqe->len = 999;
            die("vectored io not implemented");

        }
    }
//    if (!o->td_o_odirect && o->uncached)
//        sqe->rw_flags = RWF_UNCACHED;
    sqe->ioprio = 0;
//        if (ld->ioprio_class_set)
//            sqe->ioprio = td->o.ioprio_class << 13;
//        if (ld->ioprio_set)
//            sqe->ioprio |= td->o.ioprio;
    sqe->off = page_num * PAGE_SIZE;

    sqe->user_data = (unsigned long) callback;
    assert(callback);
    pdebug("sqe->len %d", sqe->len );


    if(io_ctx->sent_io - io_ctx->processed_io >= io_ctx->max_pending_io)
        die("Sent %lu ios, processed %lu (> %lu waiting), IO buffer is too full!\n", io_ctx->sent_io, io_ctx->processed_io, io_ctx->max_pending_io);


    sq_ring->array[io_ctx->sent_io & io_ctx->sq_ring_mask] = buffer_idx;
//    io_ctx->sq_ring_enqueue_tail++; // actual tail increase later
    io_ctx->sent_io++;

    return NULL;
}

char *iouring_write_page_async(struct slab_callback *callback){
    struct ioring_data *io_ctx = (struct ioring_data *)get_io_context(callback->slab->ctx);
    struct lru *lru_entry = callback->lru_entry;
    void *disk_page = lru_entry->page;
    uint64_t page_num = item_page_num(callback->slab, callback->slab_idx);
    struct io_uring_sqe *sqe;
    struct ioring_options *o = &io_uring_options;
    struct io_sq_ring *sq_ring = &io_ctx->sq_ring;

    if(!lru_entry->contains_data) {  // page is not in RAM! Abort!
        die("WTF?\n");
    }

    if(lru_entry->dirty) { // this is the second time we write the page, which means it already has been queued for writting
        struct linked_callbacks *linked_cb;
        linked_cb = malloc(sizeof(*linked_cb));
        linked_cb->callback = callback;
        linked_cb->next = io_ctx->linked_callbacks;
        io_ctx->linked_callbacks = linked_cb; // link our callback
        return disk_page;
    }

    lru_entry->dirty = 1;

    int buffer_idx = io_ctx->sent_io % io_ctx->max_pending_io;
    sqe = &io_ctx->sqes[  buffer_idx ]; // for iod=1. idx=0
    memset(sqe, 0, sizeof(*sqe));
    if (o->registerfiles) {

        sqe->fd = callback->slab->iouring_fixed_fd;
        sqe->flags = IOSQE_FIXED_FILE;
//        die("registerfiles not implemented");
    } else {
        sqe->fd = callback->slab->fd;
        sqe->flags = 0;
    }

    if (o->fixedbufs) {
        sqe->opcode = IORING_OP_WRITE_FIXED;
        sqe->addr = (unsigned long) disk_page; // or (uint64_t)??
        sqe->len = PAGE_SIZE;
        sqe->buf_index = 0;
        die("fixedbufs not implemented");

    } else {
        // main branch
        if (o->nonvectored) {
            sqe->opcode = IORING_OP_WRITEV;
//            sqe->addr = (unsigned long) disk_page; // or (uint64_t)??
//            sqe->len = PAGE_SIZE;

//            sqe->opcode = IORING_OP_READV;
//            sqe->addr = (unsigned long) disk_page; // or (uint64_t)??
//            sqe->addr = (unsigned long) malloc(PAGE_SIZE);
//            sqe->addr = (unsigned long) disk_page; // or (uint64_t)?? fixme
            assert(disk_page!=NULL);
            pdebug2("disk_page %p",disk_page);
            io_ctx->iovecs[buffer_idx].iov_base = disk_page;
//            io_ctx->iovecs[buffer_idx].iov_base = malloc(PAGE_SIZE;

            io_ctx->iovecs[buffer_idx].iov_len = PAGE_SIZE;
            sqe->addr = &io_ctx->iovecs[buffer_idx];
            sqe->len = 1;
            sqe->buf_index = 0;

        } else {
            sqe->opcode = IORING_OP_WRITEV;
            sqe->addr = NULL;
            sqe->len = 1;
            die("vectored io not implemented");

        }
    }
//    sqe->rw_flags = o;
//    if (!o->td_o_odirect && o->uncached) //fixme
//        sqe->rw_flags = RWF_UNCACHED;
    sqe->ioprio = 0;
//        if (ld->ioprio_class_set)
//            sqe->ioprio = td->o.ioprio_class << 13;
//        if (ld->ioprio_set)
//            sqe->ioprio |= td->o.ioprio;
    sqe->off = page_num * PAGE_SIZE;

    sqe->user_data = (unsigned long) callback;
    assert(callback);

    pdebug2("callback checked, io_ctx->sent_io = %d",io_ctx->sent_io );
    if(io_ctx->sent_io - io_ctx->processed_io >= io_ctx->max_pending_io)
        die("Sent %lu ios, processed %lu (> %lu waiting), IO buffer is too full!\n", io_ctx->sent_io, io_ctx->processed_io, io_ctx->max_pending_io);
    sq_ring->array[io_ctx->sent_io & io_ctx->sq_ring_mask] = buffer_idx;
    io_ctx->sent_io++;

//    io_ctx->sq_ring_enqueue_tail++; // actual tail increase later
    return NULL;

}

int iouring_io_pending(struct slab_context *ctx){
    struct ioring_data * ld;
    ld = (struct ioring_data *)ctx->io_ctx;
    return ld->sent_io - ld->processed_io;
}

void iouring_worker_ioengine_enqueue_ios(struct slab_context *ctx){
//    die("todo");
    struct ioring_data * io_ctx= ctx->io_ctx;
    struct ioring_options *o = &io_uring_options;
    size_t tail;
    struct io_sq_ring *sq_ring = &io_ctx->sq_ring;

    size_t pending = io_ctx->sent_io - io_ctx->processed_io;

    int buffer_idx;


    //        .commit1			= fio_ioring_commit, // io_uring_enter if need wakeup or IORING_ENTER_GETEVENTS

    if(pending == 0) {
        io_ctx->ios_sent_to_disk = 0;
        return;
    }
    /*if(pending > QUEUE_DEPTH)
       pending = QUEUE_DEPTH;*/
    tail = *sq_ring->tail;
    read_barrier();
    for(size_t i = tail; i <                                                                                                                                                                                                                                                                                                                                                                      io_ctx->sent_io; i++) {
        struct slab_callback *callback;
        buffer_idx = i % io_ctx->max_pending_io;

//        io_ctx->iocbs[i] = &io_ctx->iocb[(io_ctx->processed_io + i) % io_ctx->max_pending_io];
        callback = (void*)io_ctx->sqes[buffer_idx].user_data;
        if(!callback){
            die("callback is NULL");
        }

        callback->lru_entry->dirty = 0;  // reset the dirty flag *before* sending write orders otherwise following writes might be ignored
        // race condition if flag is reset after:
        //        io_submit
        //        flush done to disk
        //              |                           write page (no IO order because dirty = 1, see aio_write_page_async "if(lru_entry->dirty)" condition)
        //        complete ios
        //        (old value written to disk)

        add_time_in_payload(callback, 3);
    }

    io_ctx->queued += io_ctx->sent_io - *sq_ring->tail;

    pdebug2("*sq_ring->tail  %d", *sq_ring->tail );
    pdebug2("io_ctx->sent_io  %d", io_ctx->sent_io );
    read_barrier();
    *sq_ring->tail = io_ctx->sent_io; // todo move to commit()

    write_barrier();


    // Submit requests to the kernel
    int ret = fio_ioring_commit(ctx);
    if (ret != pending)
        perr("Couldn't submit all io requests! %d submitted / %lu (%lu sent, %lu processed)\n", ret, pending, io_ctx->sent_io, io_ctx->processed_io);
    io_ctx->ios_sent_to_disk = ret;

}
void iouring_worker_ioengine_get_completed_ios(struct slab_context *ctx){
    int ret = 0;
    struct ioring_data *io_ctx = ctx->io_ctx;
    declare_debug_timer;

    if(io_ctx->ios_sent_to_disk == 0)
        return;
    io_ctx->reaped_callbacks = calloc(io_ctx->ios_sent_to_disk, sizeof(struct slab_callback *));

    start_debug_timer {

        ret = fio_ioring_getevents( ctx, io_ctx->ios_sent_to_disk,
                                    io_ctx->ios_sent_to_disk, io_ctx->reaped_callbacks );

//        ret = io_getevents(io_ctx->ctx, io_ctx->ios_sent_to_disk - ret, io_ctx->ios_sent_to_disk - ret, &io_ctx->events[ret], NULL);
        if(ret != io_ctx->ios_sent_to_disk)
            die("Problem: only got %d answers out of %lu enqueued IO requests\n", ret, io_ctx->ios_sent_to_disk);
    } stop_debug_timer(10000, "io_getevents took more than 10ms!!");
}
void iouring_worker_ioengine_process_completed_ios(struct slab_context *ctx){
//    die("todo");
    struct ioring_data *io_ctx = ctx->io_ctx;
    struct slab_callback *callback;
    int ret = io_ctx->ios_sent_to_disk;
    declare_debug_timer;

    if(io_ctx->ios_sent_to_disk == 0)
        return;

//        .event			= fio_ioring_event, // given event/index get io_u from cqe->user_data and check res
// handle callback(res)  called by ios_completed()

    start_debug_timer {
        // Enqueue completed IO requests
        for(size_t i = 0; i < ret; i++) {
//            struct iocb *cb = (void*)io_ctx->events[i].obj;
            callback = io_ctx->reaped_callbacks[i];
//            assert(io_ctx->events[i].res == 4096); // otherwise page hasn't been read
            callback->lru_entry->contains_data = 1;
            //callback->lru_entry->dirty = 0; // done before
            callback->io_cb(callback);
        }
        free(io_ctx->reaped_callbacks);
        // We might have "linked callbacks" so process them
        iouring_process_linked_callbacks(io_ctx);
    } stop_debug_timer(10000, "rest of aio_worker_ioengine_process_completed_ios (%d requests)", ret);

    // Ok, now the main thread can push more requests
    io_ctx->processed_io += io_ctx->ios_sent_to_disk;
}


struct ioengine_ops  io_uring_ops = {
        .name = "io_uring",
        .worker_ioengine_init = iouring_worker_ioengine_init, //
//        .init			= fio_ioring_init, // init ioring_data / io_ctx
//        .post_init		= fio_ioring_post_init, // io_uring_setup
        .read_page_async = iouring_read_page_async, //
//        .prep			= fio_ioring_prep, // io_uring_sqe by io_u
//        .queue			= fio_ioring_queue, // sq_ring->tail ++
//        add callback
        .write_page_async = iouring_write_page_async, //
//        .prep			= fio_ioring_prep, // io_uring_sqe by io_u
//        .queue			= fio_ioring_queue, // sq_ring->tail ++
//        add callback
        .io_pending = iouring_io_pending, //
//  omitted
        .worker_ioengine_enqueue_ios = iouring_worker_ioengine_enqueue_ios,
//        .commit1			= fio_ioring_commit, // io_uring_enter if need wakeup or IORING_ENTER_GETEVENTS

        .worker_ioengine_get_completed_ios = iouring_worker_ioengine_get_completed_ios,
//        ??.commit2			= fio_ioring_commit, // io_uring_enter if need wakeup or IORING_ENTER_GETEVENTS
//        .getevents		= fio_ioring_getevents, // reap cq_ring->head++

        .worker_ioengine_process_completed_ios = iouring_worker_ioengine_process_completed_ios,
//        .event			= fio_ioring_event, // given event/index get io_u from cqe->user_data and check res
// handle callback(res)  called by ios_completed()

};
