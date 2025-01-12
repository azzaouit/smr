#ifndef SCRATCHPAD_H
#define SCRATCHPAD_H
#include <stdlib.h>
#include <string.h>

#define FOLLOWER(n, id) (id != n->c->host_id && n->active[id])
#define SP_HDRS(n) ((struct log_header *)(n)->buf)
#define SP_SLOTS(n) ((struct slot *)(SP_HDRS(n) + (n)->c->n))
#define SP_BUF(n) ((uint8_t *)(SP_SLOTS(n) + (n)->c->n * SMR_MAX_SLOTS))
#define LOG_HEADER(n, id) (SP_HDRS(n) + id)
#define SLOT(n, index, id) (SP_SLOTS(n) + id * SMR_MAX_SLOTS + index)
#define LOG_HEADER(n, id) (SP_HDRS(n) + id)
#define SLOT(n, index, id) (SP_SLOTS(n) + id * SMR_MAX_SLOTS + index)

#define SMR_ALLOC(p, l)                                                        \
  do {                                                                         \
    if (!((p) = calloc(1, l))) {                                               \
      perror("calloc");                                                        \
      return -errno;                                                           \
    }                                                                          \
  } while (0)

#endif /* SCRATCHPAD_H */
