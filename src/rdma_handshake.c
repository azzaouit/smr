#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include "net.h"
#include "rdma.h"

// Max number of retries on failed connections
#define MAX_RETRIES (1 << 2)

// Max number of outstanding RDMA reads & atomic operations on the dest QP
#define MAX_RD_ATOMIC (1 << 3)

// Size of remote attribute struct (packed) sent over the network (TCP)
#define RX_LEN (sizeof(struct remote_attr))

/* Thread args for the RDMA handshake */
struct rdma_xchg_args {
    struct rdma *r;
    int id;
    int ret;
};

// Validate peer exists in the network config
int __smr__valid_peer(struct config *c, uint64_t ip, int id) {
    for (size_t i = 0; i < c->n; ++i)
        if (ip == c->p[i].ip.s_addr && c->p[i].id == id) return 1;
    return 0;
}

// Get local attributes for a given peer on this host
void __smr__get_local_attr(struct rdma *r, struct remote_attr *p,
                           enum SMR_PLANE plane, int id) {
    int index = id + (plane * r->c->n);
    p->addr = (uint64_t)r->mr[plane]->addr;
    p->rkey = r->mr[plane]->rkey;
    p->lid = r->lid;
    if (plane != SMR_SCRATCHPAD) p->qpn = r->qp[index]->qp_num;
    p->psn = 0;
#pragma GCC unroll 16
    for (int i = 0; i < 16; ++i) p->gid[i] = r->gid[i];
}

// Connect a remote QP
int __smr__qp_connect(struct rdma *r, int plane, int id) {
    int ret = 0, index = id + (plane * r->c->n);
    struct remote_attr *p = r->ra + index;
    uint16_t ib_port = r->c->p[r->c->host_id].ib_port;
    uint16_t gid_index = r->c->p[r->c->host_id].gid_index;
    struct ibv_qp_attr rtr_attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_1024,
        .max_dest_rd_atomic = MAX_RD_ATOMIC,
        .min_rnr_timer = 0x12,
        .ah_attr.is_global = 1,
        .ah_attr.sl = 0,
        .ah_attr.src_path_bits = 0,
        .ah_attr.grh.flow_label = 0,
        .ah_attr.grh.hop_limit = 1,
        .ah_attr.grh.traffic_class = 0,
        .ah_attr.port_num = ib_port,
        .ah_attr.grh.sgid_index = gid_index,
        .rq_psn = p->psn,
        .dest_qp_num = p->qpn,
        .ah_attr.dlid = p->lid,
    };
    for (int i = 0; i < 16; ++i) rtr_attr.ah_attr.grh.dgid.raw[i] = p->gid[i];

    // set QP to RTR state
    ret = ibv_modify_qp(r->qp[index], &rtr_attr,
                        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (ret) {
        SMR_LOG_ERR("Failed to set QP to RTR state");
        return ret;
    }

    struct ibv_qp_attr rts_attr;
    memset(&rts_attr, 0, sizeof(rts_attr));
    rts_attr.qp_state = IBV_QPS_RTS;
    rts_attr.timeout = 0x12;
    rts_attr.retry_cnt = 7;
    rts_attr.rnr_retry = 7;
    rts_attr.sq_psn = p->psn;
    rts_attr.max_rd_atomic = MAX_RD_ATOMIC;

    // set QP to RTS state
    ret = ibv_modify_qp(r->qp[index], &rts_attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) SMR_LOG_ERR("Failed to set QP to RTS state");

    return ret;
}

// Server loop: accepts connections from higher-ranked peers
void *__smr__server_thread(void *ptr) {
    struct remote_attr local;
    struct sockaddr_in server, client;
    socklen_t clientlen = sizeof(client);
    int serverfd, clientfd, nbytes, optval = 1;
    struct rdma *r = ((struct rdma_xchg_args *)ptr)->r;
    int *ret = &((struct rdma_xchg_args *)ptr)->ret;

    struct config *c = r->c;
    uint16_t host_port = c->p[c->host_id].tcp_port;

    if ((serverfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket:");
        *ret = -errno;
        pthread_exit(NULL);
    }

    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
    memset(&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(host_port);

    if (bind(serverfd, (struct sockaddr *)&server, sizeof(server)) < 0) {
        perror("bind:");
        goto err;
    }

    if (listen(serverfd, c->n) < 0) {
        perror("listen:");
        goto err;
    }

    SMR_LOG("[tcp/server] Server listening on port %d\n", host_port);

    // Accept connections from higher-ranked peers
    size_t expected_clients = c->n - c->host_id - 1;
    for (size_t i = 0; i < expected_clients; ++i) {
        clientfd = accept(serverfd, (struct sockaddr *)&client, &clientlen);
        if (clientfd < 0)
            perror("accept:");
        else {
            SMR_LOG("Established connection with %s\n",
                    inet_ntoa(client.sin_addr));

            // Read and validate incoming peer ID.
            uint16_t id = 0;
            if ((nbytes = read(clientfd, &id, sizeof id)) != sizeof id) {
                perror("read");
                continue;
            }
            id = ntohs(id);
            if (!__smr__valid_peer(c, client.sin_addr.s_addr, id)) {
                SMR_LOG_ERR("Invalid peer");
                continue;
            }

            // Exchange attributes for rep/bg planes
            for (int plane = 0; plane < SMR_NPLANES - 1; ++plane) {
                off_t offset = (plane * r->c->n) + id;

                // get local attributes for this host
                __smr__get_local_attr(r, &local, plane, id);

                // write local attributes to remote peer
                RA_TO_NET(&local);
                if ((nbytes = write(clientfd, &local, RX_LEN)) != RX_LEN) {
                    perror("write");
                    close(clientfd);
                    *ret = errno;
                    goto err;
                }

                // read remote attributes from remote peer
                if ((nbytes = read(clientfd, r->ra + offset, RX_LEN)) !=
                    RX_LEN) {
                    perror("read");
                    close(clientfd);
                    *ret = errno;
                    goto err;
                }

                // connect queue pairs
                RA_FROM_NET(r->ra + offset);
                if (__smr__qp_connect(r, plane, id)) {
                    SMR_LOG_ERR("QP connection failed");
                    close(clientfd);
                    *ret = 2;
                    goto err;
                }
            }

            close(clientfd);
        }
    }

    close(serverfd);
    pthread_exit(NULL);

err:
    close(serverfd);
    *ret = -errno;
    pthread_exit(NULL);
}

// Client thread connects to a lower ranked peer
void *__smr__client_thread(void *ptr) {
    int i, sockfd, nbytes;
    struct remote_attr local;
    struct sockaddr_in serveraddr;
    struct rdma *r = ((struct rdma_xchg_args *)ptr)->r;
    int id = ((struct rdma_xchg_args *)ptr)->id;
    int *ret = &((struct rdma_xchg_args *)ptr)->ret;
    struct peer_config *p = r->c->p + id;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        *ret = -errno;
        pthread_exit(NULL);
    }

    memset(&serveraddr, 0, sizeof serveraddr);
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = p->ip.s_addr;
    serveraddr.sin_port = htons(p->tcp_port);

    for (i = 0; i < MAX_RETRIES; ++i) {
        sleep(5);
        if (!connect(sockfd, (struct sockaddr *)&serveraddr,
                     sizeof(serveraddr))) {
            break;
        } else {
            perror("connect");
            SMR_LOG_ERR("Connection failed. Retrying...");
        }
    }

    if (i >= MAX_RETRIES) {
        SMR_LOG_ERR("Host unreachable.");
        *ret = 1;
        goto exit;
    }

    // write peer id to server
    uint16_t hostid = htons(r->c->host_id);
    if ((nbytes = write(sockfd, &hostid, sizeof hostid)) != sizeof hostid) {
        perror("write");
        *ret = -errno;
        goto exit;
    }

    // Exchange attributes for rep/bg planes
    for (int plane = 0; plane < SMR_NPLANES - 1; ++plane) {
        off_t offset = (plane * r->c->n) + id;

        // get local attributes for this host
        __smr__get_local_attr(r, &local, plane, id);

        // write local attributes to remote peer
        RA_TO_NET(&local);
        if ((nbytes = write(sockfd, &local, RX_LEN)) != RX_LEN) {
            perror("write");
            *ret = errno;
            goto exit;
        }

        // read remote attributes from remote peer
        if ((nbytes = read(sockfd, r->ra + offset, RX_LEN)) != RX_LEN) {
            perror("read");
            *ret = errno;
            goto exit;
        }

        // connect queue pairs
        RA_FROM_NET(r->ra + offset);
        if (__smr__qp_connect(r, plane, id)) {
            SMR_LOG_ERR("QP connection failed");
            *ret = 2;
            goto exit;
        }
    }

exit:
    close(sockfd);
    pthread_exit(NULL);
}

/* RDMA handshake:
 *  - Peers act as servers for higher peers
 *  - Peers act as clients for lower peer
 *  - Each node makes roughly n/2 connections
 *  - Total TCP Connections Required: n(n-1)/2
 */
int rdma_handshake(struct rdma *r) {
    struct config *c = r->c;
    pthread_t st, ct[c->host_id];
    struct rdma_xchg_args sa = {.r = r, .ret = 0}, ca[c->host_id];
    int server = c->host_id != c->n - 1;

    /* Highest ranking peer doesn't serve */
    if (server &&
        pthread_create(&st, NULL, __smr__server_thread, (void *)&sa)) {
        perror("pthread_create:");
        return -errno;
    }

    /* Connect to lower peers */
    for (size_t i = 0; i < c->host_id; ++i) {
        ca[i].r = r;
        ca[i].id = i;
        ca[i].ret = 0;
        if (pthread_create(ct + i, NULL, __smr__client_thread,
                           (void *)(ca + i))) {
            perror("pthread_create:");
            return -errno;
        }
    }

    /* Client threads block here */
    for (size_t i = 0; i < c->host_id; ++i) {
        pthread_join(ct[i], NULL);
        if (ca[i].ret) {
            SMR_LOG_ERR("Client thread exited with nonzero status");
            return ca[i].ret;
        }
    }

    /* Server loop blocks here */
    if (server) pthread_join(st, NULL);

    return sa.ret;
}
