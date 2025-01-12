#ifndef RDMA_H
#define RDMA_H

#include <infiniband/verbs.h>
#include <pthread.h>

#include "config.h"
#include "net.h"

/* RDMA context */
struct rdma {
  /* Network configuration */
  struct config *c;
  /* IB context */
  struct ibv_context *ctx;
  /* Protection domain used for both memory regions */
  struct ibv_pd *pd;
  /* MRs for replication/background planes */
  struct ibv_mr *mr[SMR_NPLANES];
  /* QPs for replication/background planes */
  struct ibv_qp **qp;
  /* Shared CQs for replication/background planes */
  struct ibv_cq *cq[SMR_NPLANES - 1];
  /* Global wr_id */
  uint64_t wr_id;
  /* wr_id lock */
  pthread_mutex_t wr_id_lock;
  /* Remote attributes */
  struct remote_attr *ra;
  /* Local addr */
  uint16_t lid;
  /* Global addr */
  uint8_t gid[16];
};

struct mem_tx {
  uint64_t id;
  enum SMR_PLANE local_plane;
  uint64_t local_addr;
  enum SMR_PLANE remote_plane;
  uint64_t remote_addr;
  off_t remote_offset;
  size_t len;
};

/* Initialize an RDMA context */
int rdma_init(struct config *c, struct rdma *r);

/* Add memory region */
int rdma_add_mr(struct rdma *r, void *addr, size_t len, enum SMR_PLANE plane);

/* Add a queue pair */
int rdma_add_qp(struct rdma *r, enum SMR_PLANE plane, int id);

/* Active QPs */
int rdma_query_active(struct rdma *r, enum SMR_PLANE plane, uint8_t *active);

/* Exchange QP info and connect remote MRs */
int rdma_handshake(struct rdma *r);

/* Write to a remote MR */
int rdma_write(struct rdma *r, struct mem_tx *t, uint16_t id);

/* Batch write to a remote QP */
int rdma_write_batch(struct rdma *r, struct mem_tx *t, size_t n, uint16_t id);

/* Read from a remote MR */
int rdma_read(struct rdma *r, struct mem_tx *t, uint16_t id);

/* Increment a remote value */
int rdma_inc(struct rdma *r, struct mem_tx *t, uint16_t id);

/* Wait for n work completions */
int rdma_wait(struct rdma *r, enum SMR_PLANE plane, size_t n);

/* Change QP access rights */
int rdma_modify_rights(struct rdma *r, enum SMR_PLANE plane, uint16_t leader);

/* Release any resources held by the RDMA context*/
void rdma_destroy(struct rdma *r);

#endif /* RDMA_H */
