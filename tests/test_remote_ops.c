// test multiple remote hosts
#include <arpa/inet.h>
#include <assert.h>
#include <consensus.h>
#include <stdlib.h>
#include <unistd.h>

#include "bench.h"
#include "net_map.h"
#include "timer.h"

#define PORT (8000)
#define IB_PORT (1)
#define GID_INDEX (1)
#define RANDOM_SEED (42)

union ipv4 {
    char ip[4];
    uint32_t v;
};

int main(int argc, char *argv[]) {
    uint8_t *buf;
    struct consensus n;

    if (argc != 2) {
        fprintf(stderr, "Usage %s <host id>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    srand(RANDOM_SEED);

    struct config c = {.n = NPEERS,
                       .host_id = atoi(argv[1]),
                       .p = (struct node_config *)net_cfg,
                       .rdma_device = 0};

    assert(!consensus_init(&c, &n, SMR_LOG_SIZE));
    assert(!consensus_connect(&n));

    if (n.c->host_id == 0) {
        double timing_vals[SMR_MAX_SLOTS];
        assert((buf = calloc(1, SMR_MAX_BUF)));
        for (int i = 0; i < SMR_MAX_SLOTS; ++i) {
            for (int j = 0; j < SMR_MAX_BUF; ++j) buf[j] = rand() & 0xff;
            TIME_BLOCK_US(assert(!consensus_propose(&n, buf, SMR_MAX_BUF));
                          , timing_vals[i]);
        }
        bench_report(timing_vals, SMR_MAX_SLOTS);
        srand(RANDOM_SEED);
        free(buf);
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
        for (int j = 0; j < SMR_MAX_BUF; ++j)
            assert(s->buf[j] == (rand() & 0xff));
    }

    consensus_destroy(&n);
    return 0;
}
