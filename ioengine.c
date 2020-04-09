#include "headers.h"

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
void *aio_worker_ioengine_init(size_t nb_callbacks) {
   int ret;
   struct aio_engine_context *io_ctx = calloc(1, sizeof(*io_ctx));
    io_ctx->max_pending_io = nb_callbacks * 2;
    io_ctx->iocb = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->iocb));
    io_ctx->iocbs = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->iocbs));
    io_ctx->events = calloc(io_ctx->max_pending_io, sizeof(*io_ctx->events));

   ret = io_setup(io_ctx->max_pending_io, &io_ctx->ctx);
   if(ret < 0)
      perr("Cannot create aio setup\n");

   return io_ctx;
}
// API, access io_ctx
/* Enqueue requests */
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





void *iouring_worker_ioengine_init(size_t nb_callbacks){

}
char *iouring_read_page_async(struct slab_callback *callback){

}
char *iouring_write_page_async(struct slab_callback *callback){

}

int iouring_io_pending(struct slab_context *ctx){

}
void iouring_worker_ioengine_enqueue_ios(struct slab_context *ctx){

}
void iouring_worker_ioengine_get_completed_ios(struct slab_context *ctx){

}
void iouring_worker_ioengine_process_completed_ios(struct slab_context *ctx){

}


struct ioengine_ops  io_uring_ops = {
        .name = "io_uring",
        .worker_ioengine_init = iouring_worker_ioengine_init,
//    .safe_pread = safe_pread,
        .read_page_async = iouring_read_page_async,
        .write_page_async = iouring_write_page_async,
        .io_pending = iouring_io_pending,
        .worker_ioengine_enqueue_ios = iouring_worker_ioengine_enqueue_ios,
        .worker_ioengine_get_completed_ios = iouring_worker_ioengine_get_completed_ios,
        .worker_ioengine_process_completed_ios = iouring_worker_ioengine_process_completed_ios,
};
