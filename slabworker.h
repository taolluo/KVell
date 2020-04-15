#ifndef SLAB_WORKER_H
#define SLAB_WORKER_H 1

#include "pagecache.h"

struct slab_callback;
struct slab_context;
struct slab_context {
    size_t worker_id __attribute__((aligned(64)));        // ID
    struct slab **slabs;                                  // Files managed by this worker
    size_t nb_slabs;
    struct slab_callback **callbacks;                     // Callbacks associated with the requests
    volatile size_t buffered_callbacks_idx;               // Number of requests enqueued or in the process of being enqueued
    volatile size_t sent_callbacks;                       // Number of requests fully enqueued
    volatile size_t processed_callbacks;                  // Number of requests fully submitted and processed on disk
    size_t max_pending_callbacks;                         // Maximum number of enqueued requests
    struct pagecache *pagecache __attribute__((aligned(64)));

    struct ioengine_ops *io_ops;
    void *io_ctx; // aio context or io_uring_context todo1
    uint64_t rdt;                                         // Latest timestamp
    char *io_engine_name;
    int use_io_uring;

};

void kv_read_async(struct slab_callback *callback);
void kv_add_async(struct slab_callback *callback);
void kv_update_async(struct slab_callback *callback);
void kv_add_or_update_async(struct slab_callback *callback);
void kv_remove_async(struct slab_callback *callback);

typedef struct index_scan tree_scan_res_t;
tree_scan_res_t kv_init_scan(void *item, size_t scan_size);
void kv_read_async_no_lookup(struct slab_callback *callback, struct slab *s, size_t slab_idx);

size_t get_database_size(void);


void slab_workers_init(int nb_disks, int nb_workers_per_disk);
int get_nb_workers(void);
void *kv_read_sync(void *item); // Unsafe
struct pagecache *get_pagecache(struct slab_context *ctx);
//struct aio_engine_context *get_io_context(struct slab_context *ctx); // todo1
void *get_io_context(struct slab_context *ctx); // todo1
uint64_t get_rdt(struct slab_context *ctx);
void set_rdt(struct slab_context *ctx, uint64_t val);
int get_worker(struct slab *s);
int get_nb_disks(void);
struct slab *get_item_slab(int worker_id, void *item);
size_t get_item_size(char *item);
#endif
