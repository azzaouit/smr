#include "consensus.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>

/*
 * Scratch pad contents
 * [n log headers ... n slots ... slot buffer]
 */
#define SP_HDRS(n) ((struct log_header *)(n)->buf)
#define SP_SLOTS(n) ((struct slot *)(SP_HDRS(n) + (n)->c->n))
#define SP_BUF(n) ((uint8_t *)(SP_SLOTS(n) + (n)->c->n))

#define SMR_ALLOC(p, l)                                                        \
  do {                                                                         \
    if (!((p) = calloc(1, l))) {                                               \
      perror("calloc");                                                        \
      return -errno;                                                           \
    }                                                                          \
  } while (0)

extern void *__smr__leader_election_thread(void *);

int consensus_init(struct config *c, struct consensus *n, size_t log_size) {
  int ret;
  size_t npeers = c->n;

  /* Allocate log */
  size_t ls = sizeof(struct log) + log_size;
  SMR_ALLOC(n->log, ls);

  struct log_header *h = &n->log->h;
  h->capacity = log_size;
  h->buf = ((uint8_t *)n->log) + sizeof(struct log);
  n->c = c;

  /* Allocate background plane */
  size_t nb = (sizeof(uint64_t) + sizeof(uint8_t)) * npeers;
  SMR_ALLOC(n->bg, nb);

  /* Allocate log buffer */
  size_t ss =
      (sizeof(struct log_header) + sizeof(struct slot)) * npeers + SMR_MAX_BUF;
  SMR_ALLOC(n->buf, ss);

  size_t les = sizeof(int64_t) * npeers;
  SMR_ALLOC(n->le.hb, les);
  SMR_ALLOC(n->le.scores, les);

  if ((ret = rdma_init(n->c, &n->r))) {
    SMR_LOG_ERR("Failed to init RDMA");
    return ret;
  }

  /* Replication plane memory region */
  ret = rdma_add_mr(&n->r, n->log, ls, SMR_REP);
  if (ret) {
    SMR_LOG_ERR("Failed to register remote log");
    return ret;
  }

  /* Background plane memory region */
  ret = rdma_add_mr(&n->r, n->bg, nb, SMR_BG);
  if (ret) {
    SMR_LOG_ERR("Failed to register remote background metadata");
    return ret;
  }

  /* Scratchpad memory region */
  ret = rdma_add_mr(&n->r, n->buf, ss, SMR_SCRATCHPAD);
  if (ret) {
    SMR_LOG_ERR("Failed to register remote scratchpad");
    return ret;
  }

  /* QPs for rep/bg planes */
  for (size_t i = 0; i < SMR_NPLANES - 1; ++i)
    for (size_t j = 0; j < npeers; ++j)
      if ((ret = rdma_add_qp(&n->r, i, j))) {
        SMR_LOG_ERR("Failed to add qp");
        return ret;
      }

  n->fast_path_enabled = SMR_FAST_PATH_ENABLED;
  return 0;
}

/* Wait for k work requests to complete */
inline int consensus_wait(struct consensus *n, uint8_t plane, size_t k) {
  return rdma_wait(&n->r, plane, k);
}

/* Read a remote peer's log header into the local scratchpad buffer */
int __smr__consensus_read_log_header(struct consensus *n, uint16_t id) {
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)(SP_HDRS(n) + id),
                     .remote_plane = SMR_REP,
                     .remote_addr = 0,
                     .remote_offset = 0,
                     .len = sizeof(struct log_header)};
  return rdma_read(&n->r, &t, id);
}

/* Write a log header from the local scratchpad buffer to a remote peer */
int __smr__consensus_write_log_header(struct consensus *n, uint16_t id) {
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)(SP_HDRS(n) + id),
                     .remote_plane = SMR_REP,
                     .remote_addr = 0,
                     .remote_offset = 0,
                     .len = sizeof(struct log_header)};
  return rdma_write(&n->r, &t, id);
}

/* Read a remote peer's slot into the local scratchpad buffer */
int __smr__consensus_read_slot(struct consensus *n, size_t index, uint16_t id) {
  off_t offset = sizeof(struct slot) * index;
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)(SP_SLOTS(n) + id),
                     .remote_plane = SMR_REP,
                     .remote_addr = 0,
                     .remote_offset = sizeof(struct log_header) + offset,
                     .len = sizeof(struct log_header)};
  return rdma_read(&n->r, &t, id);
}

/* Read a remote peer's slot data into the local scratchpad buffer */
int __smr__consensus_read_slot_buffer(struct consensus *n, uint16_t id) {
  struct slot *s = SP_SLOTS(n) + id;
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)SP_BUF(n),
                     .remote_plane = SMR_REP,
                     .remote_addr = (uint64_t)s->buf,
                     .remote_offset = 0,
                     .len = s->len};
  return rdma_read(&n->r, &t, id);
}

/* Update a remote peer's slot from the local scratchpad buffer
 * This function makes 3 remote writes.
 * */
int __smr__consensus_update_slot(struct consensus *n, size_t index,
                                 uint16_t id) {
  int ret;
  struct slot *my_s = n->log->slots + index;
  struct slot *s = SP_SLOTS(n) + id;
  struct log_header *h = SP_HDRS(n) + id;
  off_t offset = sizeof(struct log_header) + sizeof(struct slot) * index;

  if (h->size + my_s->len > h->capacity) {
    SMR_LOG_ERR("Remote log out of space");
    return -ENOSPC;
  }

  s->len = my_s->len;
  s->propno = my_s->propno;
  s->buf = h->buf;

  // WRITE 1: payload for this slot
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)s,
                     .remote_plane = SMR_REP,
                     .remote_offset = offset,
                     .remote_addr = 0,
                     .len = sizeof(struct slot)};
  if ((ret = rdma_write(&n->r, &t, id))) {
    SMR_LOG_ERR("Remote slot write failed");
    return ret;
  }

  // WRITE 2: slot pointers for this slot
  t.local_plane = SMR_REP;
  t.local_addr = (uint64_t)n->log->slots[index].buf;
  t.remote_addr = (uint64_t)h->buf;
  t.remote_offset = 0;
  t.len = s->len;
  if ((ret = rdma_write(&n->r, &t, id))) {
    SMR_LOG_ERR("Remote log  write failed");
    return ret;
  }

  // WRITE 3: log header update
  h->buf += s->len;
  h->size += s->len;
  if ((ret = __smr__consensus_write_log_header(n, id))) {
    SMR_LOG_ERR("Failed to write log header");
    return ret;
  }

  return 0;
}

/* Insert a slot into the local log */
int __smr__log_insert(struct log *l, size_t idx, uint32_t propno, uint8_t *buf,
                      size_t len) {
  if (len > SMR_MAX_BUF)
    return -EINVAL;
  if (l->h.size + len > l->h.capacity)
    return -ENOSPC;
  struct slot *s = l->slots + idx;
  s->propno = propno;
  for (size_t i = 0; i < len; ++i)
    l->h.buf[i] = buf[i];
  s->buf = l->h.buf;
  s->len = len;
  l->h.buf += len;
  l->h.size += len;
  return 0;
}

/* Propose a value.
 * SEE: Listing 2: Basic Replication Algorithm of Mu
 * https://www.usenix.org/system/files/osdi20-aguilera.pdf
 */
int consensus_propose(struct consensus *n, uint8_t *buf, size_t len) {
  size_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;
  /* Fast path: If enabled, try to directly write the value. */
  if (n->fast_path_enabled)
    if (__smr__log_insert(n->log, n->log->h.fuo, ++n->log->h.minprop, buf,
                          len) == 0) {
      int write_count = 0;
      for (uint16_t i = 0; i < npeers; ++i) {
        if (i != host_id) {
          if (__smr__consensus_update_slot(n, n->log->h.fuo, i) == 0) {
            write_count += 3; // 3 writes: slot data + log data + log header
          } else {
            /* A write failed, abort fast path for this proposal */
            n->fast_path_enabled = false;
            SMR_LOG_ERR("Fast path write failed, falling back to slow path.");
            goto slow_path;
          }
        }
      }
      /* Wait for a majority of writes to complete */
      if (consensus_wait(n, SMR_REP, write_count) == 0) {
        n->log->h.fuo++;
        return 0;
      } else {
        n->fast_path_enabled = false;
        SMR_LOG_ERR("Fast path wait failed, falling back to slow path.");
      }
    }
slow_path:
  struct log_header *hdrs = SP_HDRS(n);
  struct slot *slots = SP_SLOTS(n);
  bool done = 0;
  int ret;

  while (!done) {
    /* Read remote minProposals */
    for (uint16_t i = 0; i < npeers; ++i)
      if (i != host_id)
        if (__smr__consensus_read_log_header(n, i)) {
          SMR_LOG_ERR("Failed to read remote proposal");
          return -errno;
        }

    if ((ret = consensus_wait(n, SMR_REP, npeers - 1))) {
      SMR_LOG_ERR("Failed to receive remote proposal");
      return ret;
    }
    uint32_t my_fuo = n->log->h.fuo;

    /* Choose a proposal number	larger
     * than we've seen so far */
    uint32_t minprop = n->log->h.minprop;
    for (uint16_t i = 0; i < npeers; ++i)
      if (i != host_id) {
        struct log_header *h = hdrs + i;
        if (h->minprop > minprop)
          minprop = h->minprop;
      }
    minprop += 1;

    /* Write remote minproposals */
    for (uint16_t i = 0; i < npeers; ++i)
      if (i != host_id) {
        struct log_header *h = hdrs + i;
        h->minprop = minprop;
        if ((ret = __smr__consensus_write_log_header(n, i))) {
          SMR_LOG_ERR("Failed to write remote proposal");
          return ret;
        }
        if ((ret = __smr__consensus_read_slot(n, my_fuo, i))) {
          SMR_LOG_ERR("Failed to receive FUO slot");
          return ret;
        }
      }
    n->log->h.minprop = minprop;
    consensus_wait(n, SMR_REP, (npeers - 1) * 2);

    /* Look for any non-⊥ values for this slot */
    struct slot *s = NULL;
    uint16_t slot_id = 0xffff;
    for (size_t i = 0; i < npeers; ++i) {
      struct slot *t = (i == host_id) ? n->log->slots + my_fuo : slots + i;
      if (t->buf && (!s || t->propno > s->propno)) {
        s = t;
        slot_id = i;
      }
    }

    /* If we read any non-⊥ values, we adopt the value
     * with the highest proposal number. */
    if (s && slot_id < npeers) {
      if (slot_id != host_id) {
        if ((ret = __smr__consensus_read_slot_buffer(n, slot_id))) {
          SMR_LOG_ERR("Failed to read slot buffer");
          return ret;
        }
        if ((ret = consensus_wait(n, SMR_REP, 1))) {
          SMR_LOG_ERR("Failed to receive slot buffer");
          return ret;
        }
        if ((ret = __smr__log_insert(n->log, my_fuo, s->propno, SP_BUF(n),
                                     s->len))) {
          SMR_LOG_ERR("Failed to insert slot");
          return ret;
        }
      }
      for (uint16_t i = 0; i < npeers; ++i)
        if (i != host_id && i != slot_id)
          if ((ret = __smr__consensus_update_slot(n, my_fuo, i))) {
            SMR_LOG_ERR("Failed to update remote slot");
            return ret;
          }
      if ((ret = consensus_wait(n, SMR_REP, npeers - 2))) {
        SMR_LOG_ERR("Failed to confirm remote slot update");
        return ret;
      }
    } else {
      /* Otherwise, we adopt our own initial value. */
      if ((ret = __smr__log_insert(n->log, my_fuo, minprop, buf, len))) {
        SMR_LOG_ERR("Failed to update local log");
        return ret;
      }
      for (uint16_t i = 0; i < npeers; ++i)
        if (i != host_id)
          if ((ret = __smr__consensus_update_slot(n, my_fuo, i))) {
            SMR_LOG_ERR("Failed to update remote log");
            return ret;
          }
      if ((ret = consensus_wait(n, SMR_REP, (npeers - 1) * 3))) {
        SMR_LOG_ERR("Failed to confirm remote log update");
        return ret;
      }
      done = 1;
    }

    ++n->log->h.fuo;
  }

  return 0;
}

/* Join leader election thread and free allocated memory */
void consensus_destroy(struct consensus *n) {
  pthread_cancel(n->le.tid);
  pthread_join(n->le.tid, NULL);
  rdma_destroy(&n->r);
  free(n->log);
  free(n->bg);
  free(n->buf);
  free(n->le.hb);
  free(n->le.scores);
}

int consensus_connect(struct consensus *n) {
  int ret;
  size_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;

  /* RDMA handshake initates peer discovery */
  if ((ret = rdma_handshake(&n->r))) {
    SMR_LOG_ERR("RDMA handshake failed");
    return ret;
  }

  /* Launch leader election thread in the background */
  if (pthread_create(&n->le.tid, NULL, __smr__leader_election_thread,
                     (void *)n)) {
    perror("pthread_create");
    return -errno;
  }

  /* Prefetch remote headers */
  if (host_id == 0) {
    for (uint16_t i = 0; i < npeers; ++i)
      if (i != host_id)
        if (__smr__consensus_read_log_header(n, i)) {
          SMR_LOG_ERR("Failed to read remote proposal");
          return -errno;
        }
    return consensus_wait(n, SMR_REP, npeers - 1);
  }

  return 0;
}
