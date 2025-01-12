#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include "net.h"
#include "rdma.h"

#define MAX_RETRIES (4)
#define MAX_RD_ATOMIC (16)
#define RX_LEN (sizeof(struct remote_attr))

/* Thread args for the RDMA handshake */
struct rdma_xchg_args {
  struct rdma *r;
  int id;
  int ret;
};

int __smr__valid_peer(struct config *c, uint64_t ip, int id) {
  for (size_t i = 0; i < c->n; ++i)
    if (ip == c->p[i].ip.s_addr && c->p[i].id == id)
      return 1;
  return 0;
}

void __smr__get_local_attr(struct rdma *r, struct remote_attr *p,
                           enum SMR_PLANE plane, int id) {
  int index = id + (plane * r->c->n);
  p->addr = (uint64_t)r->mr[plane]->addr;
  p->rkey = r->mr[plane]->rkey;
  p->lid = r->lid;
  if (plane != SMR_SCRATCHPAD)
    p->qpn = r->qp[index]->qp_num;
  p->psn = 0;
#pragma GCC unroll 16
  for (int i = 0; i < 16; ++i)
    p->gid[i] = r->gid[i];
}

int __smr__qp_connect(struct rdma *r, int plane, int id) {
  int ret = 0, index = id + (plane * r->c->n);
  struct remote_attr *p = r->ra + index;
  uint16_t ib_port = r->c->p[r->c->host_id].ib_port;
  uint16_t gid_index = r->c->p[r->c->host_id].gid_index;
  struct ibv_qp_attr rtr_attr = {
      .qp_state = IBV_QPS_RTR,
      .path_mtu = IBV_MTU_4096,
      .max_dest_rd_atomic = MAX_RD_ATOMIC,
      .min_rnr_timer = 12,
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
  for (int i = 0; i < 16; ++i)
    rtr_attr.ah_attr.grh.dgid.raw[i] = p->gid[i];

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
  rts_attr.timeout = 14;
  rts_attr.retry_cnt = 7;
  rts_attr.rnr_retry = 7;
  rts_attr.sq_psn = p->psn;
  rts_attr.max_rd_atomic = MAX_RD_ATOMIC;

  ret = ibv_modify_qp(r->qp[index], &rts_attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC);
  if (ret)
    SMR_LOG_ERR("Failed to set QP to RTS state");

  return ret;
}

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

  for (size_t npeers = 0; npeers < c->n - 1; ++npeers) {
    clientfd = accept(serverfd, (struct sockaddr *)&client, &clientlen);
    if (clientfd < 0)
      perror("accept:");
    else {
      char *hostaddrp = inet_ntoa(client.sin_addr);
      SMR_LOG("Established connection with %s\n", hostaddrp);
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

      __smr__get_local_attr(r, &local, SMR_REP, id);
      RA_TO_NET(&local);
      if ((nbytes = write(clientfd, &local, RX_LEN)) != RX_LEN)
        perror("write");

      __smr__get_local_attr(r, &local, SMR_BG, id);
      RA_TO_NET(&local);
      if ((nbytes = write(clientfd, &local, RX_LEN)) != RX_LEN)
        perror("write");

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

void *__smr__client_thread(void *ptr) {
  int i, sockfd, nbytes;
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
    if (!connect(sockfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr))) {
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

  uint16_t hostid = htons(r->c->host_id);
  if ((nbytes = write(sockfd, &hostid, sizeof hostid)) != sizeof hostid) {
    perror("write");
    *ret = -errno;
    goto exit;
  }

  for (int plane = 0; plane < SMR_NPLANES - 1; ++plane) {
    off_t offset = (plane * r->c->n) + id;
    if ((nbytes = read(sockfd, r->ra + offset, RX_LEN)) != RX_LEN) {
      perror("read");
      *ret = errno;
      goto exit;
    }
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

int rdma_handshake(struct rdma *r) {
  struct config *c = r->c;
  pthread_t st, ct[c->n - 1];
  struct rdma_xchg_args sa = {.r = r, .ret = 0}, ca[c->n - 1];

  if (pthread_create(&st, NULL, __smr__server_thread, (void *)&sa)) {
    perror("pthread_create:");
    return -errno;
  }

  for (size_t i = 0, j = 0; i < c->n; ++i)
    if (i != c->host_id) {
      ca[j].r = r;
      ca[j].id = i;
      ca[j].ret = 0;
      if (pthread_create(ct + j, NULL, __smr__client_thread,
                         (void *)(ca + j))) {
        perror("pthread_create:");
        return -errno;
      }
      ++j;
    }

  for (size_t i = 0, j = 0; i < c->n; ++i)
    if (i != c->host_id) {
      pthread_join(ct[j], NULL);
      if (ca[j].ret) {
        SMR_LOG_ERR("Client thread exited with nonzero status");
        return ca[j].ret;
      }
      ++j;
    }

  /* server loop blocks here */
  pthread_join(st, NULL);
  return sa.ret;
}
