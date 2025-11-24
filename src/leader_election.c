#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include "consensus.h"

/* Heart beat tick frequency (in seconds) */
#define HB_TICK_FREQ (1)

/* Extract hb struct */
#define BG_HB(n) ((uint64_t *)(n)->bg)
#define BG_PERMS(n) ((uint8_t *)(BG_HB(n) + n->c->n))

/* Leader catch up for leader switch */
extern int __smr__leader_catch_up(struct consensus *n);

/* Update QP permissions */
static inline int __smr__change_perms(struct rdma *r, uint16_t peer_id,
                                      int grant_write) {
    struct ibv_qp_attr attr = {0};
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           (grant_write & IBV_ACCESS_REMOTE_WRITE);
    return ibv_modify_qp(r->qp[peer_id], &attr, IBV_QP_ACCESS_FLAGS);
}

/* Heart beat tick for one time step. */
static inline void __smr__hb_tick(struct consensus *n) {
    uint64_t ret;
    uint16_t npeers = n->c->n;
    uint16_t host_id = n->c->host_id;
    struct leader_election *l = &n->le;

    // increment local heartbeat.
    ++BG_HB(n)[host_id];

    struct mem_tx tx = {.local_plane = SMR_BG,
                        .remote_plane = SMR_BG,
                        .remote_addr = 0,
                        .len = sizeof(uint64_t)};

    /* Read remote heartbeats */
    for (uint16_t i = 0; i < npeers; ++i)
        if (i != host_id) {
            off_t offset = sizeof(uint64_t) * i;
            tx.local_addr = (uint64_t)(n->bg + offset);
            tx.remote_offset = offset;
            uint64_t hb = BG_HB(n)[i];
            if ((ret = rdma_read(&n->r, &tx, i)) ||
                (ret = consensus_wait(n, SMR_BG, 1)))
                SMR_LOG("Failed to read remote heartbeat");
            // update score
            l->scores[i] += (BG_HB(n)[i] != hb) - (BG_HB(n)[i] == hb);
            // clamp score to [min,max]
            if (l->scores[i] < SMR_HB_SCORE_MIN)
                l->scores[i] = SMR_HB_SCORE_MIN;
            if (l->scores[i] > SMR_HB_SCORE_MAX)
                l->scores[i] = SMR_HB_SCORE_MAX;
            SMR_LOG("Heartbeat %hu: %lu %ld\n", i, BG_HB(n)[i], l->scores[i]);
        }
}

/* Leader election background thread.
 * Responsible for monitoring leader and
 * switching if the leader goes down.
 */
void *__smr__leader_election_thread(void *p) {
    struct consensus *n = (struct consensus *)p;
    struct leader_election *l = &n->le;
    uint16_t host_id = n->c->host_id;
    uint16_t npeers = n->c->n;

    SMR_LOG("Leader election thread starting");
    l->leader = INITIAL_LEADER;

    while (1) {
        // sleep in the begining to allow nodes to start up
        sleep(HB_TICK_FREQ);

        // Increment local and read remote
        __smr__hb_tick(n);

        // Leader failed
        if (l->leader != host_id && l->scores[l->leader] < SMR_HB_FAIL_THRESH) {
            SMR_LOG("Leader %hu failed (score=%ld), electing new leader",
                    l->leader, l->scores[l->leader]);

            // Find lowest replica with valid score
            uint16_t new_leader = host_id;
            for (uint16_t i = 0; i < npeers; ++i)
                if (i != host_id && l->scores[i] >= SMR_HB_FAIL_THRESH)
                    if (i < new_leader) new_leader = i;

            // Leader switch
            if (new_leader != l->leader) {
                SMR_LOG("Switching from leader %hu to leader %hu", l->leader,
                        new_leader);
                for (uint16_t i = 0; i < npeers; ++i)
                    if (i != host_id &&
                        __smr__change_perms(&n->r, i, (i == new_leader)))
                        SMR_LOG("Failed to change permission for peer %hu", i);
                l->leader = new_leader;
                if (new_leader == host_id && __smr__leader_catch_up(n))
                    SMR_LOG("Failed to catch up as new leader");
            }
        }
    }
}
