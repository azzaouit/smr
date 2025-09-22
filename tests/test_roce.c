// test two hosts using veth (RoCE)
#include "test.h"

// Two virtual interfaces (see Makefile)
#define NPEERS (2)

int main() {
  pthread_t tids[NPEERS];
  struct config c[NPEERS];
  struct peer_config p[NPEERS];
  union ipv4 host = {.ip = {0, 2, 168, 192}};

  init_buf();
  SMR_LOG_SET_VERBOSITY(SMR_LOG_ERROR);

  for (int i = 0; i < NPEERS; ++i) {
    host.ip[0] = i + 2;
    p[i].ip.s_addr = (uint32_t)htonl(host.v);
    p[i].id = i;
    p[i].tcp_port = PORT;
    p[i].ib_port = IB_PORT;
    p[i].gid_index = GID_INDEX;
    c[i].n = NPEERS;
    c[i].host_id = i;
    c[i].p = p;
    c[i].rdma_device = i;
  }

  for (int i = 0; i < NPEERS; ++i)
    assert(!pthread_create(tids + i, NULL, client_thread, (void *)(c + i)));

  for (int i = 0; i < NPEERS; ++i)
    pthread_join(tids[i], NULL);
  
  free(buf);
  return 0;
}
