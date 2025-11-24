#ifndef CONFIG_H
#define CONFIG_H

#include <arpa/inet.h>

#define DEBUG (1)

#ifdef DEBUG
#include <stdio.h>
#define SMR_LOG(fmt, ...)                                                      \
  do {                                                                         \
    time_t _now = time(NULL);                                                  \
    struct tm _tm;                                                             \
    localtime_r(&_now, &_tm);                                                  \
    char _buf[20];                                                             \
    strftime(_buf, sizeof(_buf), "%Y-%m-%d %H:%M:%S", &_tm);                   \
    fprintf(stderr, "[%s][%s:%d] " fmt "\n", _buf, __FILE__, __LINE__,         \
            ##__VA_ARGS__);                                                    \
  } while (0)
#else
#define SMR_LOG(MSG, ...)                                                      \
  do {                                                                         \
  } while (0)
#endif

/* Initial leader node */
#define INITIAL_LEADER (0)

/* Fast path enabled */
#define SMR_FAST_PATH_ENABLED (1)

/* Pin to core on RDMA NIC NUMA node*/
#define SMR_NUMA_AWARE (1)

/* Max buffer size for each slot */
#define SMR_MAX_BUF (1 << 5)

/* Max number of log slots */
#define SMR_MAX_SLOTS (1 << 10)

/* Full log size */
#define SMR_LOG_SIZE (SMR_MAX_BUF * SMR_MAX_SLOTS)

/* Failure threshold. Leader switch
 * happens if the leader's
 * score drops below this value. */
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
