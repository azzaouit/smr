#ifndef CONFIG_H
#define CONFIG_H

#include <arpa/inet.h>
#include <stdio.h>

#include "log.h"

/* Max buffer size for each slot */
#define SMR_MAX_BUF (1 << 6)

/* Max number of log slots */
#define SMR_MAX_SLOTS (1 << 10)

/* Full log size */
#define SMR_LOG_SIZE (SMR_MAX_BUF * SMR_MAX_SLOTS)

/* Failure threshold. Leader switch happens if the leader's score drops below
 * this value. */
#define SMR_HB_FAIL_THRESH (2)

/* Minimum score value */
#define SMR_HB_SCORE_MIN (0)

/* Maximum score value */
#define SMR_HB_SCORE_MAX (15)

/* Replication planes */
#define SMR_NPLANES (3)
enum SMR_PLANE {
  SMR_REP = 0,    // replication plane
  SMR_BG,         // background plane
  SMR_SCRATCHPAD, // scratchpad
};

/* Peer entry */
struct peer_config {
  struct in_addr ip;  // peer ip addr
  uint16_t id;        // peer rank
  uint16_t tcp_port;  // peer tcp port
  uint16_t ib_port;   // peer ib device port
  uint16_t gid_index; // peer ib device global id
};

/* Peer configuration used for network discovery
 * during the initial bootstrapping phase.
 * Every peer should have a copy of this struct.
 * */
struct config {
  size_t n;              // number of peers
  uint16_t host_id;      // this peer's rank
  uint8_t rdma_device;   // index into rdma device list
  struct peer_config *p; // all peers
};

#endif /* CONFIG_H */
