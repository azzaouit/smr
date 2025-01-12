#ifndef CONFIG_H
#define CONFIG_H

#include <arpa/inet.h>
#include <stdio.h>

#define SMR_DEBUG

#define SMR_BATCH_WRITES (1)
#define SMR_CONFIRM_WRITES (1)
#define SMR_MIN_SCORE (0)
#define SMR_MAX_SCORE (15)
#define SMR_MAX_WR (32)
#define SMR_MAX_SGE (1)
#define SMR_MAX_INLINE (256)
#define SMR_MAX_BUF (32)
#define SMR_MAX_SLOTS (1000000)
#define SMR_NPLANES (3)
#define SMR_HB_FAIL_THRESH (2)
#define SMR_LOG_SIZE (SMR_MAX_BUF * SMR_MAX_SLOTS)
#define SMR_LOG(...) fprintf(stdout, "[smr] " __VA_ARGS__)
#define SMR_LOG_ERR(MSG, ...)                                                  \
  fprintf(stderr, "%s:%d " MSG, __FILE__, __LINE__, ##__VA_ARGS__)

enum SMR_PLANE {
  SMR_REP = 0,
  SMR_BG,
  SMR_SCRATCHPAD,
};

/* Peer entry */
struct peer_config {
  struct in_addr ip;
  uint16_t id;
  uint16_t tcp_port;
  uint16_t ib_port;
  uint16_t gid_index;
};

struct config {
  size_t n;
  uint16_t host_id;
  struct peer_config *p;
};

#endif /* CONFIG_H */
