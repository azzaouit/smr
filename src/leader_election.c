#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include "consensus.h"

/* Heart beat tick frequency (in seconds) */
#define HB_TICK_FREQ (1)

/* Extract hb struct */
#define BG_HB(n) ((uint64_t *)(n)->bg)
#define BG_PERMS(n) ((uint8_t *)(BG_HB(n) + n->c->n))

/* Heart beat tick for one time step.
 * Responsible for incrementing remote heartbeat
 * for every peer node. */
uint64_t __smr__hb_tick(struct consensus *n) {
  uint64_t ret;
  uint16_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;
  off_t offset = sizeof(uint64_t) * host_id;

  struct mem_tx tx = {.local_plane = SMR_BG,
                      .local_addr = (uint64_t)n->bg + offset,
                      .remote_plane = SMR_BG,
                      .remote_addr = 0,
                      .remote_offset = offset,
                      .len = sizeof(uint64_t)};

  for (int i = 0; i < npeers; ++i)
    if (i != host_id && (ret = rdma_inc(&n->r, &tx, i))) {
      SMR_LOG_ERR("Failed to increment remote heartbeat");
      return ret;
    }

  return 0; // consensus_wait(n, SMR_BG, npeers - 1);
}

/* Leader election background thread.
 * Responsible for monitoring leader and
 * switching if the leader goes down.
 */
void *__smr__leader_election_thread(void *p) {
  struct consensus *n = (struct consensus *)p;
  struct leader_election *l = &n->le;
  uint64_t ret, *hb_remote = BG_HB(n);
  uint16_t host_id = n->c->host_id;
  uint16_t npeers = n->c->n;

  SMR_LOG("Leader election thread starting\n");
  l->leader = 0;

  while (1) {
    for (int i = 0; i < npeers; ++i)
      l->hb[i] = hb_remote[i];
    if ((ret = __smr__hb_tick(n)))
      goto err;
    for (int i = 0; i < npeers; ++i)
      if (i != host_id) {
        l->scores[i] += (l->hb[i] != hb_remote[i]) - (l->hb[i] == hb_remote[i]);
        /* Scores are capped by a maximum and minimum */
        if (l->scores[i] < SMR_HB_SCORE_MIN)
          l->scores[i] = SMR_HB_SCORE_MIN;
        if (l->scores[i] > SMR_HB_SCORE_MAX)
          l->scores[i] = SMR_HB_SCORE_MAX;
        SMR_LOG("Heartbeat %hu: %lu %ld\n", i, hb_remote[i], l->scores[i]);
      }
    sleep(HB_TICK_FREQ);
  }

err:
  SMR_LOG("Leader election thread exiting with status %d\n", errno);
  pthread_exit((void *)ret);
}
