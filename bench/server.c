#include <stdlib.h>
#include <unistd.h>

#include "consensus.h"
#include "net_map.h"
#include "timer.h"

#define LEADER_NODE (0)
#define HEADER_SIZE(n) (((volatile struct log_header *)&(n)->log->h)->size)

struct client_args {
    int fd;
    int ret;
    struct consensus *n;
};

void *handle_client(void *arg) {
    int ret = 0;
    char buf[SMR_MAX_BUF];
    struct client_args *args = (struct client_args *)arg;
    while (recv(args->fd, &buf, sizeof(buf), 0) == sizeof(buf)) {
        ret = consensus_propose(args->n, (unsigned char *)buf, SMR_MAX_BUF);
        if (ret) {
            SMR_LOG("consensus_propose_failed");
            break;
        }
        ret = send(args->fd, &args->ret, sizeof(args->ret), 0);
        if (ret != sizeof(args->ret)) {
            SMR_LOG("Failed to send client response");
            break;
        }
    }
    args->ret = ret;
    return arg;
}

int start_server(struct consensus *n) {
    int serverfd, fd, ret = 0, optval = 1;
    uint16_t host_id = INITIAL_LEADER;
    struct sockaddr_in client_addr,
        server_addr = {.sin_family = AF_INET,
                       .sin_addr.s_addr = htonl(net_cfg[LEADER_NODE].v),
                       .sin_port = htons(CLIENT_SERVICE_PORT)};
    size_t thread_count = 0, thread_cap = 8;
    socklen_t client_len = sizeof(client_addr);
    struct client_args *args;

    pthread_t *threads;

    if ((serverfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        return serverfd;
    }

    ret = setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
    if (ret < 0) {
        perror("setsockopt");
        goto done;
    }

    ret = bind(serverfd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    if (ret < 0) {
        perror("bind");
        goto done;
    }

    if ((ret = listen(serverfd, 100)) < 0) {
        perror("listen");
        goto done;
    }

    SMR_LOG("Node %d: Client service listening on %s:%d", host_id,
            inet_ntoa(server_addr.sin_addr), CLIENT_SERVICE_PORT);

    threads = calloc(8, sizeof(pthread_t));

    while (HEADER_SIZE(n) != SMR_LOG_SIZE) {
        fd = accept(serverfd, (struct sockaddr *)&client_addr, &client_len);
        if (fd < 0) {
            perror("accept");
            continue;
        }
        if (!(args = calloc(1, sizeof(struct client_args)))) {
            perror("calloc:");
            ret = errno;
            goto done;
        }
        args->fd = fd;
        args->ret = 0;
        args->n = n;
        if (thread_count >= thread_cap) {
            thread_cap <<= 1;
            if (!(threads = realloc(threads, thread_cap * sizeof(pthread_t)))) {
                perror("realloc:");
                ret = errno;
                goto done;
            }
        }
        pthread_create(threads + thread_count++, NULL, handle_client, args);
    }

    for (size_t i = 0; i < thread_count; ++i) {
        pthread_join(threads[i], (void **)args);
        if (args->ret)
            SMR_LOG("Client thread %zu returned with nonzero status %d\n", i,
                    args->ret);
        close(args->fd);
        free(args);
    }
    free(threads);

done:
    close(serverfd);
    return ret;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage %s <host id>\n", argv[0]);
        return 1;
    }
    struct consensus n;
    int host_id = atoi(argv[1]);
    struct config c = {.n = NPEERS,
                       .host_id = host_id,
                       .p = (struct node_config *)net_cfg,
                       .rdma_device = 0};

    if (consensus_init(&c, &n, SMR_LOG_SIZE)) {
        SMR_LOG("Failed to init consensus");
        return -1;
    }

    if (consensus_connect(&n)) {
        SMR_LOG("Failed to connect to network");
        consensus_destroy(&n);
        return -2;
    }

    if (host_id == LEADER_NODE) {
        srand(time(0));
        start_server(&n);
    } else
        while (HEADER_SIZE(&n) != SMR_LOG_SIZE) usleep(10);

    consensus_destroy(&n);
    return 0;
}
