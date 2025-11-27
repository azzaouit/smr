#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/random.h>
#include <unistd.h>

#include "bench.h"
#include "net_map.h"
#include "timer.h"

struct client_args {
    int ret;
    int thread_id;
    int num_requests;
    double *timing_vals;
};

void *client_thread(void *arg) {
    char *buf;
    static int slot = 0;
    double start, elapsed;
    struct client_args *args = (struct client_args *)arg;
    int s, ret, sockfd, num_requests = args->num_requests;
    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(net_cfg[INITIAL_LEADER].v),
        .sin_port = htons(CLIENT_SERVICE_PORT)};

    SMR_LOG("Client thread %d: starting with %d requests", args->thread_id,
            num_requests);

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        ret = errno;
        goto done;
    }

    ret = connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    if (ret < 0) {
        perror("connect");
        goto done;
    }

    SMR_LOG("Client thread %d connected. Making %d requests.", args->thread_id,
            args->num_requests);

    if (!(buf = calloc(1, SMR_MAX_BUF))) {
        perror("calloc:");
        ret = errno;
        goto done;
    }

    for (int i = 0; i < args->num_requests; ++i) {
        if (getrandom(buf, SMR_MAX_BUF, GRND_RANDOM) != SMR_MAX_BUF) {
            perror("getrandom");
            ret = 1;
            goto exit;
        }

        start = ts_us();
        if ((ret = send(sockfd, buf, SMR_MAX_BUF, 0)) != SMR_MAX_BUF) {
            perror("send");
            goto exit;
        }
        ret = recv(sockfd, &args->ret, sizeof(args->ret), 0);
        if (ret != sizeof(args->ret)) {
            perror("recv");
            goto exit;
        }
        elapsed = ts_us() - start;

        if (args->ret) {
            SMR_LOG("Remote propose call returned nonzero status %d", ret);
            goto exit;
        }
        s = __sync_fetch_and_add(&slot, 1);
        fprintf(stderr, "%d,%.2f\n", s, elapsed);
        fflush(stderr);
        args->timing_vals[s] = elapsed;
    }

    SMR_LOG("Client thread %d exited after completing %d requests",
            args->thread_id, num_requests);
exit:
    free(buf);
done:
    args->ret = ret;
    close(sockfd);
    return arg;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <num_threads> <requests_per_thread>\n",
                argv[0]);
        return 1;
    }

    int num_threads, requests_per_thread;
    struct client_args *args;
    double *timing_vals;
    pthread_t *threads;

    num_threads = atoi(argv[1]);
    requests_per_thread = atoi(argv[2]);

    if (!(timing_vals = calloc(SMR_MAX_SLOTS, sizeof(double)))) {
        perror("calloc:");
        return 1;
    }

    if (!(threads = calloc(num_threads, sizeof(pthread_t)))) {
        perror("calloc:");
        return 1;
    }

    if (!(args = calloc(num_threads, sizeof(struct client_args)))) {
        perror("calloc:");
        return 1;
    }

    for (int i = 0; i < num_threads; ++i) {
        args[i].thread_id = i;
        args[i].num_requests = requests_per_thread;
        args[i].timing_vals = timing_vals;
        if (pthread_create(threads + i, NULL, client_thread, args + i)) {
            perror("pthread_create");
            return -errno;
        }
        SMR_LOG("Launched thread %d", i);
    }

    for (int i = 0; i < num_threads; ++i) pthread_join(threads[i], NULL);

    bench_report(timing_vals, SMR_MAX_SLOTS);
    bench_save("latency.csv", timing_vals, SMR_MAX_SLOTS);

    free(args);
    free(threads);
    free(timing_vals);

    return 0;
}
