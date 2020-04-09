#include "slabworker.h"

#ifndef IOENGINE_H
#define IOENGINE_H 1


//struct aio_engine_context *aio_worker_ioengine_init(size_t nb_callbacks);
void *aio_worker_ioengine_init(size_t nb_callbacks);

void *safe_pread(int fd, off_t offset);

typedef void (io_cb_t)(struct slab_callback *);
char *aio_read_page_async(struct slab_callback *callback);
char *aio_write_page_async(struct slab_callback *callback);

//int aio_io_pending(struct aio_engine_context *ctx);
int aio_io_pending(struct slab_context *ctx);
void aio_worker_ioengine_enqueue_ios(struct slab_context *ctx);
void aio_worker_ioengine_get_completed_ios(struct slab_context *ctx);
void aio_worker_ioengine_process_completed_ios(struct slab_context *ctx);


void *iouring_worker_ioengine_init(size_t nb_callbacks);
char *iouring_read_page_async(struct slab_callback *callback);
char *iouring_write_page_async(struct slab_callback *callback);
int iouring_io_pending(struct slab_context *ctx);
void iouring_worker_ioengine_enqueue_ios(struct slab_context *ctx);
void iouring_worker_ioengine_get_completed_ios(struct slab_context *ctx);
void iouring_worker_ioengine_process_completed_ios(struct slab_context *ctx);

struct ioengine_ops {
    const char *name;
    void *(*worker_ioengine_init)(size_t );

    void *(*safe_pread)(int , off_t );

    char *(*read_page_async)(struct slab_callback *);
    char *(*write_page_async)(struct slab_callback *);

//int aio_io_pending(struct aio_engine_context *ctx);
    int (*io_pending)(struct slab_context *);
    void (*worker_ioengine_enqueue_ios)(struct slab_context *);
    void (*worker_ioengine_get_completed_ios)(struct slab_context *);
    void (*worker_ioengine_process_completed_ios)(struct slab_context *);
};
struct ioengine_ops libaio_ops;
struct ioengine_ops io_uring_ops;

#endif
