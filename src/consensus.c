#include <errno.h>
#include <pthread.h>

#include "consensus.h"
#include "scratchpad.h"

#ifdef SMR_DEBUG
#include <time.h>
#endif

int __smr__consensus_read_log_header(struct consensus *n, uint16_t id) {
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)LOG_HEADER(n, id),
                     .remote_plane = SMR_REP,
                     .remote_addr = 0,
                     .remote_offset = 0,
                     .len = sizeof(struct log_header)};
  return rdma_read(&n->r, &t, id);
}

int __smr__consensus_read_slot(struct consensus *n, size_t index, uint16_t id) {
  off_t offset = sizeof(struct slot) * index;
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)SLOT(n, index, id),
                     .remote_plane = SMR_REP,
                     .remote_addr = 0,
                     .remote_offset = sizeof(struct log_header) + offset,
                     .len = sizeof(struct slot)};
  return rdma_read(&n->r, &t, id);
}

int __smr__consensus_read_slot_buffer(struct consensus *n, size_t index,
                                      uint16_t id) {
  struct slot *s = SLOT(n, id, index);
  struct mem_tx t = {.local_plane = SMR_SCRATCHPAD,
                     .local_addr = (uint64_t)SP_BUF(n),
                     .remote_plane = SMR_REP,
                     .remote_addr = (uint64_t)s->buf,
                     .remote_offset = 0,
                     .len = s->len};
  return rdma_read(&n->r, &t, id);
}

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

int __smr__consensus_update_slot(struct consensus *n, size_t index,
                                 uint16_t id) {
  struct mem_tx t[3];
  struct slot *my_s = n->log->slots + index;
  struct slot *s = SLOT(n, index, id);
  struct log_header *h = LOG_HEADER(n, id);
  off_t offset = sizeof(struct log_header) + sizeof(struct slot) * index;

  if (h->size + my_s->len > h->capacity) {
    SMR_LOG_ERR("Remote log out of space\n");
    return -ENOSPC;
  }

  /* Update slot */
  s->len = my_s->len;
  s->propno = my_s->propno;
  s->buf = h->buf;
  t[0].local_plane = SMR_SCRATCHPAD;
  t[0].local_addr = (uint64_t)s;
  t[0].remote_plane = SMR_REP;
  t[0].remote_offset = offset;
  t[0].remote_addr = 0;
  t[0].len = sizeof(struct slot);

  /* Copy buffer */
  t[1].local_plane = SMR_REP;
  t[1].local_addr = (uint64_t)n->log->slots[index].buf;
  t[1].remote_plane = SMR_REP;
  t[1].remote_addr = (uint64_t)h->buf;
  t[1].remote_offset = 0;
  t[1].len = s->len;

  /* Update log header */
  h->buf += s->len;
  h->size += s->len;
  t[2].local_plane = SMR_SCRATCHPAD;
  t[2].local_addr = (uint64_t)(SP_HDRS(n) + id);
  t[2].remote_plane = SMR_REP;
  t[2].remote_addr = 0;
  t[2].remote_offset = 0;
  t[2].len = sizeof(struct log_header);

#if SMR_BATCH_WRITES
  return rdma_write_batch(&n->r, t, 3, id);
#else
  int ret;
  for (int i = 0; i < 3; ++i)
    if ((ret = rdma_write(&n->r, t + i, id))) {
      SMR_LOG_ERR("Remote slot write failed\n");
      return ret;
    }
#endif

  return 0;
}

int consensus_propose(struct consensus *n, uint8_t *buf, size_t len) {
  if (n->le.leader != n->c->host_id)
    return -EINVAL;
  pthread_mutex_lock(&n->leader_lock);
  int ret;
  size_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;

#ifdef SMR_DEBUG
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
#endif

  for (;;) {
    uint32_t my_fuo = n->log->h.fuo++;

    /* Choose a proposal number	larger
     * than we've seen so far */
    uint32_t minprop = n->log->h.minprop;
    for (uint16_t i = 0; i < npeers; ++i)
      if (FOLLOWER(n, i) && LOG_HEADER(n, i)->minprop > minprop)
        minprop = LOG_HEADER(n, i)->minprop;
    minprop += 1;
    for (uint16_t i = 0; i < npeers; ++i)
      if (FOLLOWER(n, i)) {
        LOG_HEADER(n, i)->minprop = minprop;
        LOG_HEADER(n, i)->fuo = n->log->h.fuo;
      }
    n->log->h.minprop = minprop;

    /* Look for any non-⊥ values for this slot */
    struct slot *s = NULL;
    uint16_t slot_id = 0xffff;
    for (size_t i = 0; i < npeers; ++i) {
      struct slot *t =
          (i == host_id) ? n->log->slots + my_fuo : SLOT(n, my_fuo, i);
      if (t->buf && (!s || t->propno > s->propno)) {
        s = t;
        slot_id = i;
      }
    }

    /* If we read any non-⊥ values, we adopt the value
     * with the highest proposal number. */
    if (s && slot_id < npeers) {
      if (slot_id != host_id) {
        if ((ret = __smr__consensus_read_slot_buffer(n, my_fuo, slot_id))) {
          SMR_LOG_ERR("Failed to read slot buffer\n");
          goto done;
        }
        if ((ret = rdma_wait(&n->r, SMR_REP, 1))) {
          SMR_LOG_ERR("Remote slot buffer read failed\n");
          goto done;
        }
        if ((ret = __smr__log_insert(n->log, my_fuo, s->propno, SP_BUF(n),
                                     s->len))) {
          SMR_LOG_ERR("Failed to insert local slot\n");
          goto done;
        }
      }
      for (uint16_t i = 0; i < npeers; ++i)
        if (FOLLOWER(n, i) && i != slot_id)
          if ((ret = __smr__consensus_update_slot(n, my_fuo, i))) {
            SMR_LOG_ERR("Failed to update remote slot\n");
            goto done;
          }
#if SMR_CONFIRM_WRITES
      if ((ret = rdma_wait(&n->r, SMR_REP, (n->nactive - 1) * 3))) {
        SMR_LOG_ERR("Remote slot update failed\n");
        goto done;
      }
#endif
    } else {
      /* Otherwise, we adopt our own initial value. */
      if ((ret = __smr__log_insert(n->log, my_fuo, minprop, buf, len))) {
        SMR_LOG_ERR("Failed to update local log\n");
        goto done;
      }
      for (uint16_t i = 0; i < npeers; ++i)
        if (FOLLOWER(n, i))
          if ((ret = __smr__consensus_update_slot(n, my_fuo, i))) {
            SMR_LOG_ERR("Failed to update remote log\n");
            goto done;
          }
#if SMR_CONFIRM_WRITES
      if ((ret = rdma_wait(&n->r, SMR_REP, (n->nactive) * 3))) {
        SMR_LOG_ERR("Remote slot update failed\n");
        goto done;
      }
#endif
      break;
    }
  }

#ifdef SMR_DEBUG
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC, &end);
  uint64_t elapsed =
      end.tv_sec * 1e9 + end.tv_nsec - start.tv_sec * 1e9 - start.tv_nsec;
  printf("Elapsed time: %f microseconds\n", elapsed * 1e-3);
#endif

  ret = 0;
done:
  pthread_mutex_unlock(&n->leader_lock);
  return ret;
}

int consensus_read_remote_logs(struct consensus *n) {
  int ret;
  size_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;
  for (uint16_t i = 0; i < npeers; ++i)
    if (i != host_id) {
      if ((ret = __smr__consensus_read_log_header(n, i))) {
        SMR_LOG_ERR("Failed to read log header %hu: %d\n", i, ret);
        return ret;
      }
      if ((ret = rdma_wait(&n->r, SMR_REP, 1))) {
        SMR_LOG_ERR("Remote log header read %d failed: %d\n", i, ret);
        return ret;
      }
      for (int j = 0; j < SMR_MAX_SLOTS; ++j) {
        if ((ret = __smr__consensus_read_slot(n, j, i))) {
          SMR_LOG_ERR("Failed to read remote slot %d: %d\n", j, ret);
          return ret;
        }
        if ((ret = rdma_wait(&n->r, SMR_REP, 1))) {
          SMR_LOG_ERR("Remote slot read %d failed: %d\n", j, ret);
          return ret;
        }
      }
    }
  return 0;
}

int consensus_leader_init(struct consensus *n) {
  if (n->le.leader != n->c->host_id)
    return -EINVAL;

  /* Find first undecided offset */
  n->log->h.fuo = SMR_MAX_SLOTS;
  for (size_t i = 0; i < SMR_MAX_SLOTS; ++i)
    if (!n->log->slots[i].buf) {
      n->log->h.fuo = i;
      break;
    }

  n->role = SMR_ROLE_LEADER;
  return 0;
}

int consensus_init(struct config *c, struct consensus *n, size_t log_size) {
  int ret;
  size_t npeers = c->n;

  size_t ls = sizeof(struct log) + log_size;
  SMR_ALLOC(n->log, ls);

  struct log_header *h = &n->log->h;
  h->capacity = log_size;
  h->buf = ((uint8_t *)n->log) + sizeof(struct log);
  n->c = c;

  size_t nb = (sizeof(uint64_t) + sizeof(uint8_t)) * npeers;
  SMR_ALLOC(n->bg, nb);

  size_t ss =
      (sizeof(struct log_header) + sizeof(struct slot) * SMR_MAX_SLOTS) *
          npeers +
      SMR_MAX_BUF;
  SMR_ALLOC(n->buf, ss);

  size_t les = sizeof(uint64_t) * npeers;
  SMR_ALLOC(n->le.hb, les);
  SMR_ALLOC(n->le.scores, les);
  SMR_ALLOC(n->active, npeers);

  if ((ret = rdma_init(n->c, &n->r))) {
    SMR_LOG_ERR("Failed to init RDMA\n");
    return ret;
  }

  ret = rdma_add_mr(&n->r, n->log, ls, SMR_REP);
  if (ret) {
    SMR_LOG_ERR("Failed to register remote log\n");
    return ret;
  }

  ret = rdma_add_mr(&n->r, n->bg, nb, SMR_BG);
  if (ret) {
    SMR_LOG_ERR("Failed to register remote background metadata\n");
    return ret;
  }

  ret = rdma_add_mr(&n->r, n->buf, ss, SMR_SCRATCHPAD);
  if (ret) {
    SMR_LOG_ERR("Failed to register remote scratchpad\n");
    return ret;
  }

  for (size_t i = 0; i < SMR_NPLANES - 1; ++i)
    for (size_t j = 0; j < npeers; ++j)
      if ((ret = rdma_add_qp(&n->r, i, j))) {
        SMR_LOG_ERR("Failed to add qp\n");
        return ret;
      }

  pthread_mutex_init(&n->leader_lock, NULL);

  n->le.leader = 0;
  n->role = c->host_id ? SMR_ROLE_FOLLOWER : SMR_ROLE_LEADER;

  return 0;
}

int consensus_connect(struct consensus *n) {
  int ret;
  if ((ret = rdma_handshake(&n->r))) {
    SMR_LOG_ERR("RDMA handshake failed: %d\n", ret);
    return ret;
  }

  if ((ret = consensus_leader_election_init(n))) {
    SMR_LOG_ERR("Leader election init failed: %d\n", ret);
    return ret;
  }

  if ((ret = consensus_read_remote_logs(n))) {
    SMR_LOG_ERR("Remote log read failed: %d\n", ret);
    return ret;
  }

  for (uint16_t i = 0; i < n->c->n; ++i)
    if (i != n->c->host_id)
      n->active[i] = 1;
  n->nactive = n->c->n - 1;

  return n->role == SMR_ROLE_LEADER ? consensus_leader_init(n)
                                    : consensus_follower_init(n);
}

void consensus_destroy(struct consensus *n) {
  SMR_LOG("Node %hu exiting\n", n->c->host_id);
  if (n->role == SMR_ROLE_FOLLOWER)
    consensus_follower_destroy(n);
  consensus_leader_election_destroy(n);
  rdma_destroy(&n->r);
  free(n->log);
  free(n->bg);
  free(n->buf);
  free(n->le.hb);
  free(n->le.scores);
  free(n->active);
}
