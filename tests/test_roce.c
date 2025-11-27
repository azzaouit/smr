// test two hosts using veth (RoCE)
#include "roce_map.h"
#include "test.h"

int main() {
    pthread_t tids[NPEERS];
    struct config c[] = {
        {.n = NPEERS,
         .host_id = 0,
         .p = (struct node_config *)net_cfg,
         .rdma_device = 0},
        {.n = NPEERS,
         .host_id = 1,
         .p = (struct node_config *)net_cfg,
         .rdma_device = 0},
    };

    for (int i = 0; i < NPEERS; ++i)
        assert(!pthread_create(tids + i, NULL, client_thread, (void *)(c + i)));

    for (int i = 0; i < NPEERS; ++i) pthread_join(tids[i], NULL);

    free(buf);

    return 0;
}
