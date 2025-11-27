// test multiple hosts using the same RDMA device (loopback)
#include <pthread.h>

#include "test.h"

#define NPEERS (4)

int main() {
    pthread_t tids[NPEERS];
    struct config c[NPEERS];
    struct node_config p[NPEERS];
    char host[] = {1, 0, 0, 127};

    for (int i = 0; i < NPEERS; ++i) {
        memcpy(p[i].ip, host, sizeof(host));
        p[i].id = i;
        p[i].tcp_port = PORT + i;
        p[i].ib_port = IB_PORT;
        p[i].gid_index = GID_INDEX;
        c[i].n = NPEERS;
        c[i].host_id = i;
        c[i].p = p;
        c[i].rdma_device = 0;
    }

    for (int i = 0; i < NPEERS; ++i)
        assert(!pthread_create(tids + i, NULL, client_thread, (void *)(c + i)));

    for (int i = 0; i < NPEERS; ++i) pthread_join(tids[i], NULL);

    free(buf);

    return 0;
}
