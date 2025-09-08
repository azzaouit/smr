#include "rdma.h"

#include <stdio.h>
#include <stdlib.h>

/* Max Work requests */
#define MAX_WR (1 << 3)

/* Max scatter-gather entries */
#define MAX_SGE (1 << 3)

/* Default RDMA send work request ID */
#define RDMA_SEND_WRID (1 << 0)

/* Default RDMA recv work request ID */
#define RDMA_RECV_WRID (1 << 1)

int rdma_init(struct config *c, struct rdma *r) {
    int i;
    union ibv_gid gid;
    struct ibv_port_attr pa;
    struct ibv_device **dev_list;
    uint16_t port_num = (c->p + c->host_id)->ib_port;
    uint16_t gid_index = (c->p + c->host_id)->gid_index;

    if (!(dev_list = ibv_get_device_list(NULL))) {
        SMR_LOG_ERR("ibv_get_device_list failed");
        goto exit;
    }

    // open rdma device
    if (!(r->ctx = ibv_open_device(dev_list[c->rdma_device]))) {
        SMR_LOG_ERR("ibv_open_device failed");
        ibv_free_device_list(dev_list);
        goto exit;
    }
    ibv_free_device_list(dev_list);

    if (ibv_query_gid(r->ctx, port_num, gid_index, &gid)) {
        SMR_LOG_ERR("ibv_query_gid failed");
        goto exit;
    }

#pragma GCC unroll 16
    for (int i = 0; i < 16; ++i) r->gid[i] = gid.raw[i];

    if (ibv_query_port(r->ctx, port_num, &pa)) {
        SMR_LOG_ERR("ibv_query_port failed");
        goto exit;
    }
    r->lid = pa.lid;

    if (!(r->pd = ibv_alloc_pd(r->ctx))) {
        SMR_LOG_ERR("ibv_alloc_pd failed");
        goto exit;
    }

    // allocate completion queues for rep/bg planes
    for (i = 0; i < SMR_NPLANES - 1; ++i)
        if (!(r->cq[i] = ibv_create_cq(r->ctx, 1024, NULL, NULL, 0))) {
            SMR_LOG_ERR("ibv_create_cq failed");
            goto errpd;
        }

    r->c = c;

    // allocate queue-pairs for rep/bg planes
    size_t nb = (sizeof(struct ibv_qp *) * r->c->n) * 2;
    if (!(r->qp = calloc(1, nb))) {
        perror("calloc:");
        goto errcq;
    }

    // allocate remote attribute structs for rep/bg planes
    if (!(r->ra = calloc(1, sizeof(struct remote_attr) * c->n * 2))) {
        perror("calloc");
        goto errra;
    }
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
    r->mr[plane] =
        ibv_reg_mr(r->pd, (void *)addr, len,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                       IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    if (!r->mr[plane]) {
        SMR_LOG_ERR("ibv_reg_mr failed");
        return -errno;
    }
    return 0;
}

int rdma_add_qp(struct rdma *r, enum SMR_PLANE plane, int id) {
    int index = id + (plane * r->c->n);
    // read this host's device port num from network config
    int port_num = (r->c->p + r->c->host_id)->ib_port;

    struct ibv_qp_init_attr init_attr = {.qp_type = IBV_QPT_RC,
                                         .send_cq = r->cq[plane],
                                         .recv_cq = r->cq[plane],
                                         .cap = {.max_send_wr = MAX_WR,
                                                 .max_recv_wr = MAX_WR,
                                                 .max_send_sge = MAX_SGE,
                                                 .max_recv_sge = MAX_SGE}};
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_INIT,
        .pkey_index = 0,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ,
        .port_num = port_num,
    };

    // rep plane: grant write access only to leader (initialy rank 0)
    // bg plane: all peers given atomic write access to update their heartbeats
    if (id == 0 || plane == SMR_BG)
        attr.qp_access_flags |=
            (IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);

    // create qp
    if (!(r->qp[index] = ibv_create_qp(r->pd, &init_attr))) {
        SMR_LOG_ERR("ibv_create_qp failed");
        return -errno;
    }

    // set qp to init state
    if (ibv_modify_qp(r->qp[index], &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
        ibv_destroy_qp(r->qp[index]);
        SMR_LOG_ERR("ibv_modify_qp failed");
        return -errno;
    }

    return 0;
}

// remote rw operation
int __smr__rdma_remote_op(struct rdma *r, struct mem_tx *t, uint16_t id,
                          enum ibv_wr_opcode o) {
    int index = id + (t->remote_plane * r->c->n);
    struct ibv_send_wr *bad_wr = NULL, wr = {};
    struct remote_attr *a = r->ra + index;
    struct ibv_qp *q = r->qp[index];
    struct ibv_sge list = {};
    uint64_t ra;

    // local addr
    list.addr = t->local_addr;
    // length of the buffer
    list.length = t->len;
    // lkey for this memory region
    list.lkey = r->mr[t->local_plane]->lkey;

    wr.wr_id = RDMA_SEND_WRID;
    wr.sg_list = &list;
    wr.opcode = o;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.num_sge = 1;

    ra = (t->remote_addr ? t->remote_addr : a->addr) + t->remote_offset;
    if (o == IBV_WR_ATOMIC_FETCH_AND_ADD) {
        wr.wr.atomic.remote_addr = ra;
        wr.wr.atomic.rkey = a->rkey;
        wr.wr.atomic.compare_add = 1ULL;
    } else {
        wr.wr.rdma.remote_addr = ra;
        wr.wr.rdma.rkey = a->rkey;
    }

    return ibv_post_send(q, &wr, &bad_wr);
}

// remote write
inline int rdma_write(struct rdma *r, struct mem_tx *t, uint16_t id) {
    return __smr__rdma_remote_op(r, t, id, IBV_WR_RDMA_WRITE);
}

// remote read
inline int rdma_read(struct rdma *r, struct mem_tx *t, uint16_t id) {
    return __smr__rdma_remote_op(r, t, id, IBV_WR_RDMA_READ);
}

// remote inc (used for incrementing heartbeat)
inline int rdma_inc(struct rdma *r, struct mem_tx *t, uint16_t id) {
    return __smr__rdma_remote_op(r, t, id, IBV_WR_ATOMIC_FETCH_AND_ADD);
}

// block for n work request completions on a plane
int rdma_wait(struct rdma *r, enum SMR_PLANE plane, size_t n) {
    int ret = 0;
    size_t count = 0;
    struct ibv_wc wc[n];
    while (count < n) {
        do {
            if ((ret = ibv_poll_cq(r->cq[plane], n - count, wc + count)) < 0) {
                SMR_LOG_ERR("ibv_poll_cq failed");
                return errno;
            }
        } while (!ret);
        count += ret;
    }
    for (size_t i = 0; i < n; ++i)
        if (wc[i].status != IBV_WC_SUCCESS) {
            fprintf(stderr, "%d %s\n", wc[i].status,
                    ibv_wc_status_str(wc[i].status));
            SMR_LOG_ERR("WC returned with error status");
            return wc[i].status;
        }
    return 0;
}

// destroy rdma resources
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
