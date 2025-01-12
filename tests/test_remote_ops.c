#include <arpa/inet.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>

#include <consensus.h>

#define NPEERS (2)
#define PORT (8000)
#define IB_PORT (1)
#define GID_INDEX (1)

union ipv4 {
  char ip[4];
  uint32_t v;
};

int main(int argc, char *argv[]) {
  struct consensus n;
  uint8_t *buf = NULL;
  struct peer_config p[NPEERS];
  union ipv4 host = {.ip = {0, 1, 10, 10}};

  if (argc != 2) {
    fprintf(stderr, "Usage %s <host id>\n", argv[0]);
    exit(EXIT_FAILURE);
  }

  for (int i = 0; i < NPEERS; ++i) {
    host.ip[0] = i + 1;
    p[i].ip.s_addr = (uint32_t)htonl(host.v);
    p[i].id = i;
    p[i].tcp_port = PORT;
    p[i].ib_port = IB_PORT;
    p[i].gid_index = GID_INDEX;
  }

  struct config c = {.n = NPEERS, .host_id = atoi(argv[1]), .p = p};

  assert(!consensus_init(&c, &n, SMR_LOG_SIZE));
  assert(!consensus_connect(&n));
  srand(42);

  if (n.c->host_id == 0) {
    buf = calloc(1, SMR_MAX_BUF);
    assert(buf);
    for (int i = 0; i < SMR_MAX_SLOTS; ++i) {
      for (int j = 0; j < SMR_MAX_BUF; ++j)
        buf[j] = i;
      assert(!consensus_propose(&n, buf, SMR_MAX_BUF));
    }
    free(buf);
  } else
    sleep(10);

  struct log_header *h = &n.log->h;
  assert(h->size == SMR_LOG_SIZE);
  assert(h->capacity == SMR_LOG_SIZE);
  for (uint32_t i = 0; i < SMR_MAX_SLOTS; ++i) {
    struct slot *s = n.log->slots + i;
    assert(s->len == SMR_MAX_BUF);
    assert(s->propno == i + 1);
    for (int j = 0; j < SMR_MAX_BUF; ++j)
      assert(s->buf[j] == (uint8_t)i);
  }

  consensus_destroy(&n);
  return 0;
}
