#include <pthread.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "consensus.h"

#define BG_HB(n) ((uint64_t *)(n)->bg)
#define BG_PERMS(n) ((uint8_t *)(BG_HB(n) + n->c->n))
#define HEARTBEAT(n, id) (BG_HB(n) + (id))
#define PERMISSIONS(n, id) (BG_PERMS(n) + (id))

#define HB_WRITE_FREQ (2)
#define SMR_HB_TICK_FREQ (1 << 2)

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MAJ(np) ((uint16_t)((np - 1) / 2 + 1))

void *__smr__hb_tick(void *p) {
  struct consensus *n = (struct consensus *)p;
  uint16_t host_id = n->c->host_id;
  for (;;) {
    pthread_testcancel();
    ++*HEARTBEAT(n, host_id);
    sleep(1);
  }
}

void *__smr__hb_thread(void *p) {
  int ret, nfailed;
  struct consensus *n = (struct consensus *)p;
  uint16_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;
  for (;;) {
    pthread_testcancel();
    nfailed = 0;
    ++*HEARTBEAT(n, host_id);
    for (uint16_t i = 0; i < npeers; ++i)
      if (i != host_id) {
        struct mem_tx tx = {.local_plane = SMR_BG,
                            .local_addr = (uint64_t)HEARTBEAT(n, i),
                            .remote_plane = SMR_BG,
                            .remote_addr = 0,
                            .remote_offset = sizeof(uint64_t) * i,
                            .len = sizeof(uint64_t)};
        if ((ret = rdma_read(&n->r, &tx, i))) {
          SMR_LOG_ERR("Heartbeat %hu failed with status code %d\n", i, ret);
          ++nfailed;
        }
      }
    rdma_wait(&n->r, SMR_BG, npeers - 1 - nfailed);
  }
}

uint64_t __smr__req_ldr_perms(struct consensus *n) {
  uint64_t ret, nerr = 0;
  uint16_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;
  off_t offset = sizeof(uint64_t) * npeers + host_id;
  struct mem_tx tx = {.local_plane = SMR_BG,
                      .local_addr = (uint64_t)PERMISSIONS(n, host_id),
                      .remote_plane = SMR_BG,
                      .remote_addr = 0,
                      .remote_offset = offset,
                      .len = sizeof(uint8_t)};
  for (uint16_t i = 0; i < npeers; ++i)
    if (i != host_id && (ret = rdma_write(&n->r, &tx, i))) {
      SMR_LOG_ERR("Failed to write permission request to peer %hu\n", i);
      ++nerr;
    }
  return nerr >= MAJ(npeers);
}

inline void __smr__next_leader(struct consensus *n) {
  int npeers = n->c->n;
  uint16_t host_id = n->c->host_id;
  struct leader_election *l = &n->le;
  for (uint16_t i = 0; i < npeers; ++i) {
    uint16_t next = (l->leader + i) % npeers;
    if (next == host_id || l->scores[next] > SMR_HB_FAIL_THRESH) {
      l->leader = next;
      break;
    }
  }
  if (l->leader != host_id) {
    while (!PERMISSIONS(n, l->leader)) {
      SMR_LOG("Waiting for new leader to request write permissions...\n");
      sleep(1);
    }
  }
}

uint64_t __smr__leader_switch(struct consensus *n) {
  pthread_mutex_lock(&n->leader_lock);
  int ret;
  uint16_t host_id = n->c->host_id;
  struct leader_election *l = &n->le;
  uint16_t prev_leader = l->leader;
  __smr__next_leader(n);
  SMR_LOG("[%hu] Leader %hu down. Attempting leader switch to peer %d.\n",
          host_id, prev_leader, l->leader);
  if ((ret = rdma_modify_rights(&n->r, SMR_REP, l->leader))) {
    SMR_LOG_ERR("Failed to modify QP access rights\n");
    return ret;
  }
  if (l->leader == host_id) {
    consensus_follower_destroy(n);
    if ((ret = __smr__req_ldr_perms(n))) {
      SMR_LOG_ERR("Failed to request write permissions from a majority\n");
      goto done;
    }
    if ((ret = consensus_leader_init(n))) {
      SMR_LOG_ERR("Failed to init leader role.\n");
      goto done;
    }
  }
  ret = 0;
done:
  pthread_mutex_unlock(&n->leader_lock);
  return ret;
}

void *__smr__leader_election_thread(void *p) {
  struct consensus *n = (struct consensus *)p;
  struct leader_election *l = &n->le;
  uint64_t ret, *hb_remote = BG_HB(n);
  uint16_t host_id = n->c->host_id, npeers = n->c->n;

  SMR_LOG("Leader election thread starting\n");
  l->leader = 0;

  for (uint64_t tick = 0;; ++tick) {
    sleep(SMR_HB_TICK_FREQ);
    for (uint16_t i = 0; i < npeers; ++i) {
      if (i != host_id) {
        if (l->hb[i] == hb_remote[i])
          l->scores[i] = MAX(l->scores[i] - 1, SMR_MIN_SCORE);
        else
          l->scores[i] = MIN(l->scores[i] + 1, SMR_MAX_SCORE);
        SMR_LOG("[%hu] Heartbeat %hu: %lu %ld\n", host_id, i, hb_remote[i],
                l->scores[i]);
      }
    }

    for (uint16_t i = 0; i < npeers; ++i)
      if (i != host_id) {
        if (l->scores[i] < SMR_HB_FAIL_THRESH) {
          if (n->active[i]) {
            SMR_LOG("[%hu] Node %hu down. Removing from active list.\n",
                    host_id, i);
            --n->nactive;
            n->active[i] = 0;
          }
          if (i == l->leader && (ret = __smr__leader_switch(n))) {
            SMR_LOG_ERR("Leader switch failed with status %lu.\n", ret);
            goto err;
          }
        }
      }

    for (int i = 0; i < npeers; ++i)
      l->hb[i] = hb_remote[i];
  }

err:
  SMR_LOG("Leader election thread exiting with status %d\n", errno);
  pthread_exit((void *)ret);
}

int consensus_leader_election_init(struct consensus *n) {
  for (uint16_t i = 0; i < n->c->n; ++i)
    n->le.scores[i] = SMR_HB_FAIL_THRESH + 1;

  if (pthread_create(n->le.tid, NULL, __smr__hb_thread, (void *)n)) {
    perror("pthread_create");
    return -errno;
  }
  if (pthread_create(n->le.tid + 1, NULL, __smr__leader_election_thread,
                     (void *)n)) {
    perror("pthread_create");
    return -errno;
  }
  return 0;
}

void consensus_leader_election_destroy(struct consensus *n) {
  for (int i = 0; i < 2; ++i) {
    pthread_cancel(n->le.tid[i]);
    pthread_join(n->le.tid[i], NULL);
  }
}
