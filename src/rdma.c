#include "rdma.h"
#include <stdlib.h>

#define FOLLOWER_RIGHTS (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ)
#define LEADER_RIGHTS (FOLLOWER_RIGHTS | IBV_ACCESS_REMOTE_WRITE)

int rdma_wait(struct rdma *r, enum SMR_PLANE plane, size_t n) {
  int ret = 0;
  size_t count = 0;
  struct ibv_wc wc[n];
  while (count < n) {
    do {
      if ((ret = ibv_poll_cq(r->cq[plane], n - count, wc + count)) < 0) {
        SMR_LOG_ERR("ibv_poll_cq failed: %d\n", ret);
        return errno;
      }
    } while (!ret);
    count += ret;
  }

  if (plane != SMR_BG)
    for (size_t i = 0; i < n; ++i)
      if (wc[i].status != IBV_WC_SUCCESS) {
        SMR_LOG_ERR("WC returned with error status %d %s\n", wc[i].status,
                    ibv_wc_status_str(wc[i].status));
        return wc[i].status;
      }
  return 0;
}

int rdma_init(struct config *c, struct rdma *r) {
  int i;
  union ibv_gid gid;
  struct ibv_port_attr pa;
  struct ibv_device **dev_list;
  uint16_t port_num = (c->p + c->host_id)->ib_port;
  uint16_t gid_index = (c->p + c->host_id)->gid_index;

  if (!(dev_list = ibv_get_device_list(NULL))) {
    SMR_LOG_ERR("ibv_get_device_list failed\n");
    goto exit;
  }

  if (!(r->ctx = ibv_open_device(dev_list[0]))) {
    SMR_LOG_ERR("ibv_open_device failed\n");
    ibv_free_device_list(dev_list);
    goto exit;
  }
  ibv_free_device_list(dev_list);

  if (ibv_query_gid(r->ctx, port_num, gid_index, &gid)) {
    SMR_LOG_ERR("ibv_query_gid failed\n");
    goto exit;
  }

#pragma GCC unroll 16
  for (int i = 0; i < 16; ++i)
    r->gid[i] = gid.raw[i];

  if (ibv_query_port(r->ctx, port_num, &pa)) {
    SMR_LOG_ERR("ibv_query_port failed\n");
    goto exit;
  }
  r->lid = pa.lid;

  if (!(r->pd = ibv_alloc_pd(r->ctx))) {
    SMR_LOG_ERR("ibv_alloc_pd failed\n");
    goto exit;
  }

  for (i = 0; i < SMR_NPLANES - 1; ++i)
    if (!(r->cq[i] = ibv_create_cq(r->ctx, 1024, NULL, NULL, 0))) {
      SMR_LOG_ERR("ibv_create_cq failed\n");
      goto errpd;
    }

  r->c = c;
  uint64_t nn = c->n << 1;
  if (!(r->qp = calloc(nn, sizeof(struct ibv_qp *)))) {
    perror("calloc:");
    goto errcq;
  }

  if (!(r->ra = calloc(nn, sizeof(struct remote_attr)))) {
    perror("calloc");
    goto errra;
  }

  r->wr_id = 0;
  pthread_mutex_init(&r->wr_id_lock, NULL);
  return 0;

errra:
  free(r->qp);
errcq:
  for (int j = 0; j < i; ++j) {
    ibv_destroy_cq(r->cq[j]);
    r->cq[j] = NULL;
  }
errpd:
  ibv_dealloc_pd(r->pd);
  r->pd = NULL;
exit:
  ibv_close_device(r->ctx);
  r->ctx = NULL;
  return -errno;
}

int rdma_add_mr(struct rdma *r, void *addr, size_t len, enum SMR_PLANE plane) {
  r->mr[plane] = ibv_reg_mr(r->pd, (void *)addr, len, LEADER_RIGHTS);
  if (!r->mr[plane]) {
    SMR_LOG_ERR("ibv_reg_mr failed\n");
    return -errno;
  }
  return 0;
}

int rdma_add_qp(struct rdma *r, enum SMR_PLANE plane, int id) {
  int index = id + (plane * r->c->n);
  int port_num = (r->c->p + r->c->host_id)->ib_port;

  struct ibv_qp_init_attr init_attr = {
      .qp_type = IBV_QPT_RC,
      .send_cq = r->cq[plane],
      .recv_cq = r->cq[plane],
      .cap = {.max_send_wr = SMR_MAX_WR,
              .max_recv_wr = SMR_MAX_WR,
              .max_send_sge = SMR_MAX_SGE,
              .max_recv_sge = SMR_MAX_SGE,
              .max_inline_data = SMR_MAX_INLINE}};
  struct ibv_qp_attr attr = {
      .qp_state = IBV_QPS_INIT,
      .pkey_index = 0,
      .qp_access_flags = id ? FOLLOWER_RIGHTS : LEADER_RIGHTS,
      .port_num = port_num,
  };

  if (!(r->qp[index] = ibv_create_qp(r->pd, &init_attr))) {
    SMR_LOG_ERR("ibv_create_qp failed\n");
    return -errno;
  }
  if (ibv_modify_qp(r->qp[index], &attr,
                    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                        IBV_QP_ACCESS_FLAGS)) {
    ibv_destroy_qp(r->qp[index]);
    SMR_LOG_ERR("ibv_modify_qp failed\n");
    return -errno;
  }
  return 0;
}

int rdma_query_active(struct rdma *r, enum SMR_PLANE plane, uint8_t *active) {
  int ret, n = 0;
  struct ibv_qp_attr attr;
  struct ibv_qp_init_attr init_attr;

  for (uint16_t i = 0; i < r->c->n; ++i)
    if (i != r->c->host_id) {
      int index = plane * r->c->n + i;
      if ((ret = ibv_query_qp(r->qp[index], &attr, IBV_QP_STATE, &init_attr))) {
        SMR_LOG_ERR("ibv_query_qp failed with status code %d\n", ret);
        return -ret;
      }
      active[i] = (attr.qp_state == IBV_QPS_RTS);
      n += active[i];
    }

  return n;
}

int rdma_modify_rights(struct rdma *r, enum SMR_PLANE plane, uint16_t leader) {
  int ret, index;
  struct ibv_qp_attr attr;
  for (uint16_t i = 0; i < r->c->n; ++i)
    if (i != r->c->host_id) {
      index = plane * r->c->n + i;
      attr.qp_access_flags = (i == leader) ? LEADER_RIGHTS : FOLLOWER_RIGHTS;
      if ((ret = ibv_modify_qp(r->qp[index], &attr, IBV_QP_ACCESS_FLAGS))) {
        SMR_LOG_ERR("ibv_modify_qp failed with status %d\n", ret);
        return ret;
      }
    }
  return 0;
}

inline void __smr__rdma_sge(struct rdma *r, struct mem_tx *t,
                            struct ibv_sge *list) {
  list->addr = t->local_addr;
  list->length = t->len;
  list->lkey = r->mr[t->local_plane]->lkey;
}

void __smr__rdma_wr(struct rdma *r, struct mem_tx *t, uint16_t id,
                    enum ibv_wr_opcode o, struct ibv_send_wr *wr) {
  int index = id + (t->remote_plane * r->c->n);
  struct remote_attr *a = r->ra + index;
  uint64_t ra;
  wr->opcode = o;
  wr->send_flags = IBV_SEND_SIGNALED;
  if (o == IBV_WR_RDMA_WRITE && t->len <= SMR_MAX_INLINE)
    wr->send_flags |= IBV_SEND_INLINE;
  ra = (t->remote_addr ? t->remote_addr : a->addr) + t->remote_offset;
  if (o == IBV_WR_ATOMIC_FETCH_AND_ADD) {
    wr->wr.atomic.remote_addr = ra;
    wr->wr.atomic.rkey = a->rkey;
    wr->wr.atomic.compare_add = 1ULL;
  } else {
    wr->wr.rdma.remote_addr = ra;
    wr->wr.rdma.rkey = a->rkey;
  }

  pthread_mutex_lock(&r->wr_id_lock);
  t->id = r->wr_id++;
  pthread_mutex_unlock(&r->wr_id_lock);
  wr->wr_id = t->id;
}

int __smr__rdma_remote_op(struct rdma *r, struct mem_tx *t, uint16_t id,
                          enum ibv_wr_opcode o) {
  int index = id + (t->remote_plane * r->c->n);
  struct ibv_send_wr *bad_wr = NULL, wr = {};
  struct ibv_qp *q = r->qp[index];
  struct ibv_sge list = {};
  __smr__rdma_sge(r, t, &list);
  __smr__rdma_wr(r, t, id, o, &wr);
  wr.num_sge = 1;
  wr.sg_list = &list;

  int ret = ibv_post_send(q, &wr, &bad_wr);
  if (bad_wr)
    SMR_LOG_ERR("WR %lu failed\n", bad_wr->wr_id);
  return ret;
}

inline int rdma_write(struct rdma *r, struct mem_tx *t, uint16_t id) {
  return __smr__rdma_remote_op(r, t, id, IBV_WR_RDMA_WRITE);
}

int rdma_write_batch(struct rdma *r, struct mem_tx *t, size_t n, uint16_t id) {
  struct ibv_sge list[n];
  struct ibv_send_wr wr[n], *bad_wr = NULL;
  int index = id + (t->remote_plane * r->c->n);
  struct ibv_qp *q = r->qp[index];
  for (size_t i = 0; i < n; ++i) {
    __smr__rdma_sge(r, t + i, list + i);
    __smr__rdma_wr(r, t + i, id, IBV_WR_RDMA_WRITE, wr + i);
    wr[i].sg_list = list + i;
    wr[i].num_sge = 1;
    wr[i].next = (i == n - 1) ? NULL : wr + i + 1;
  }
  int ret = ibv_post_send(q, wr, &bad_wr);
  if (bad_wr)
    SMR_LOG_ERR("WR %lu failed with error %d\n", bad_wr->wr_id, ret);
  return ret;
}

inline int rdma_read(struct rdma *r, struct mem_tx *t, uint16_t id) {
  return __smr__rdma_remote_op(r, t, id, IBV_WR_RDMA_READ);
}

inline int rdma_inc(struct rdma *r, struct mem_tx *t, uint16_t id) {
  return __smr__rdma_remote_op(r, t, id, IBV_WR_ATOMIC_FETCH_AND_ADD);
}

void rdma_destroy(struct rdma *r) {
  for (size_t i = 0; i < SMR_NPLANES; ++i)
    if (r->mr[i]) {
      ibv_dereg_mr(r->mr[i]);
      r->mr[i] = NULL;
    }
  if (r->qp) {
    for (size_t i = 0; i < (r->c->n) << 1; ++i)
      if (r->qp[i]) {
        ibv_destroy_qp(r->qp[i]);
        r->qp[i] = NULL;
      }
    free(r->qp);
    r->qp = NULL;
  }
  for (size_t i = 0; i < SMR_NPLANES - 1; ++i)
    if (r->cq[i]) {
      ibv_destroy_cq(r->cq[i]);
      r->cq[i] = NULL;
    }
  if (r->pd) {
    ibv_dealloc_pd(r->pd);
    r->pd = NULL;
  }
  if (r->ctx) {
    ibv_close_device(r->ctx);
    r->ctx = NULL;
  }
  free(r->ra);
  r->ra = NULL;
  r->c = NULL;
}
