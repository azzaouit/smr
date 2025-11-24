#define _GNU_SOURCE
#include "rdma.h"

#include <sched.h>
#include <stdio.h>
#include <stdlib.h>

int rdma_init(struct config *c, struct rdma *r) {
    int i;
    union ibv_gid gid;
    struct ibv_port_attr pa;
    struct ibv_device **dev_list;
    uint16_t port_num = (c->p + c->host_id)->ib_port;
    uint16_t gid_index = (c->p + c->host_id)->gid_index;

    if (!(dev_list = ibv_get_device_list(NULL))) {
        SMR_LOG("ibv_get_device_list failed");
        goto exit;
    }

    // open rdma device
    if (!(r->ctx = ibv_open_device(dev_list[c->rdma_device]))) {
        SMR_LOG("ibv_open_device failed");
        ibv_free_device_list(dev_list);
        goto exit;
    }
    ibv_free_device_list(dev_list);

    if (ibv_query_gid(r->ctx, port_num, gid_index, &gid)) {
        SMR_LOG("ibv_query_gid failed");
        goto exit;
    }

#pragma GCC unroll 16
    for (int i = 0; i < 16; ++i) r->gid[i] = gid.raw[i];

    if (ibv_query_port(r->ctx, port_num, &pa)) {
        SMR_LOG("ibv_query_port failed");
        goto exit;
    }
    r->lid = pa.lid;

    if (!(r->pd = ibv_alloc_pd(r->ctx))) {
        SMR_LOG("ibv_alloc_pd failed");
        goto exit;
    }

    // allocate completion queues for rep/bg planes
    for (i = 0; i < SMR_NPLANES - 1; ++i)
        if (!(r->cq[i] = ibv_create_cq(r->ctx, 1024, NULL, NULL, 0))) {
            SMR_LOG("ibv_create_cq failed");
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

    r->max_inline = 0;
    r->numa.enabled = SMR_NUMA_AWARE;
    r->numa.cpu_affinity = 0;
    return rdma_set_cpu_affinity(r);

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
        SMR_LOG("ibv_reg_mr failed");
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
    if (id == INITIAL_LEADER || plane == SMR_BG)
        attr.qp_access_flags |=
            (IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);

    // create qp
    if (!(r->qp[index] = ibv_create_qp(r->pd, &init_attr))) {
        SMR_LOG("ibv_create_qp failed");
        return -errno;
    }

    // set qp to init state
    if (ibv_modify_qp(r->qp[index], &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
        ibv_destroy_qp(r->qp[index]);
        SMR_LOG("ibv_modify_qp failed");
        return -errno;
    }

    // Determine max inline data on this QP
    if (!ibv_query_qp(r->qp[index], &attr, IBV_QP_CAP, &init_attr))
        r->max_inline = init_attr.cap.max_inline_data;

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
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    if (o == IBV_WR_RDMA_WRITE && t->len < r->max_inline)
        wr.send_flags |= IBV_SEND_INLINE;

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

// wait for n work request completions on a plane
int rdma_wait(struct rdma *r, enum SMR_PLANE plane, size_t n) {
    size_t count = 0;
    struct ibv_wc wc[64];
    int polls_without_progress = 0;
    const int max_empty_polls = 1000000;  // Prevent infinite busy loop

    while (count < n) {
        int ncheck = (n - count > 64) ? 64 : (n - count);
        int ret = ibv_poll_cq(r->cq[plane], ncheck, wc);
        if (ret > 0) {
            /* Check completions for errors */
            for (int i = 0; i < ret; i++) {
                if (wc[i].status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "Work completion error: %s",
                            ibv_wc_status_str(wc[i].status));
                    return wc[i].status;
                }
            }
            count += ret;
            polls_without_progress = 0;
        } else if (ret < 0) {
            SMR_LOG("ibv_poll_cq failed");
            return errno;
        } else {
            /* No completions ready - continue busy polling */
            polls_without_progress++;
            if (polls_without_progress > max_empty_polls) {
                SMR_LOG("Busy poll timeout - no completions after %d polls",
                        max_empty_polls);
                return -ETIMEDOUT;
            }
            // spin loop hint
            __asm__ __volatile__("pause" ::: "memory");
        }
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

int rdma_set_cpu_affinity(struct rdma *r) {
    if (!r->numa.enabled) return 0;

    // Get device NUMA node
    char path[256];
    snprintf(path, sizeof(path), "/sys/class/infiniband/%s/device/numa_node",
             ibv_get_device_name(r->ctx->device));

    FILE *fp = fopen(path, "r");
    if (fp != NULL) {
        fscanf(fp, "%d", &r->numa.cpu_affinity);
        fclose(fp);
    }

    // Set thread affinity
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(r->numa.cpu_affinity, &cpuset);

    int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    if (!ret)
        SMR_LOG("RDMA: Set CPU affinity to core %d (NUMA node of NIC)",
                r->numa.cpu_affinity);

    return ret;
}
