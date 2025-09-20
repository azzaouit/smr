// test multiple peers on the same host
#include <arpa/inet.h>
#include <assert.h>
#include <consensus.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include "timer.h"

#define NPEERS (16)
#define PORT (8000)
#define IB_PORT (1)
#define GID_INDEX (0)

union ipv4 {
    char ip[4];
    uint32_t v;
};

void *client_thread(void *p) {
    struct consensus n;
    uint8_t *buf = NULL;
    struct config *c = (struct config *)p;
    assert(!consensus_init(c, &n, SMR_LOG_SIZE));
    assert(!consensus_connect(&n));
    srand(42);

    if (n.c->host_id == 0) {
        buf = calloc(1, SMR_MAX_BUF);
        assert(buf);
        for (int i = 0; i < SMR_MAX_SLOTS; ++i) {
            for (int j = 0; j < SMR_MAX_BUF; ++j) buf[j] = i;
            TIME_BLOCK_US(assert(!consensus_propose(&n, buf, SMR_MAX_BUF)););
        }
        free(buf);
    } else
        sleep(5);

    struct log_header *h = &n.log->h;
    assert(h->size == SMR_LOG_SIZE);
    assert(h->capacity == SMR_LOG_SIZE);
    for (uint32_t i = 0; i < SMR_MAX_SLOTS; ++i) {
        struct slot *s = n.log->slots + i;
        assert(s->len == SMR_MAX_BUF);
        assert(s->propno == i + 1);
        for (int j = 0; j < SMR_MAX_BUF; ++j) assert(s->buf[j] == i);
    }

    consensus_destroy(&n);
    pthread_exit(NULL);
}

int main() {
    pthread_t tids[NPEERS];
    struct config c[NPEERS];
    struct peer_config p[NPEERS];
    union ipv4 host = {.ip = {1, 0, 0, 127}};

    SMR_LOG_SET_VERBOSITY(SMR_LOG_ERROR);

    for (int i = 0; i < NPEERS; ++i) {
        p[i].ip.s_addr = (uint32_t)htonl(host.v);
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

    return 0;
}
