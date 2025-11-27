// test multiple peers on the same host
#include <arpa/inet.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <unistd.h>

#include "bench.h"
#include "consensus.h"
#include "timer.h"

#define PORT (8000)
#define IB_PORT (1)
#define GID_INDEX (0)

#define HEADER_SIZE(n)

union ipv4 {
  char ip[4];
  uint32_t v;
};

static uint8_t *buf;

void *client_thread(void *p) {
  struct consensus n;
  struct config *c = (struct config *)p;

  if (!buf) {
    assert((buf = calloc(1, SMR_LOG_SIZE)));
    assert(getrandom(buf, SMR_LOG_SIZE, GRND_RANDOM) == SMR_LOG_SIZE);
  }

  assert(!consensus_init(c, &n, SMR_LOG_SIZE));
  assert(!consensus_connect(&n));

  if (n.c->host_id == 0) {
    double timing_vals[SMR_MAX_SLOTS];
    for (int i = 0; i < SMR_MAX_SLOTS; ++i) {
      uint8_t *offset = buf + i * SMR_MAX_BUF;
      int ret;
      TIME_BLOCK_US(ret = consensus_propose(&n, offset, SMR_MAX_BUF);
                    , timing_vals[i]);
      assert(!ret);
    }
    bench_report(timing_vals, SMR_MAX_SLOTS);
  } else
    while (((volatile struct log_header *)&n.log->h)->size != SMR_LOG_SIZE)
      usleep(10);

  struct log_header *h = &n.log->h;
  assert(h->size == SMR_LOG_SIZE);
  assert(h->capacity == SMR_LOG_SIZE);
  for (uint32_t i = 0; i < SMR_MAX_SLOTS; ++i) {
    struct slot *s = n.log->slots + i;
    assert(s->len == SMR_MAX_BUF);
    assert(s->propno == i + 1);
    uint8_t *offset = buf + i * SMR_MAX_BUF;
    assert(!memcmp(offset, s->buf, SMR_MAX_BUF));
  }

  consensus_destroy(&n);

  return NULL;
}
