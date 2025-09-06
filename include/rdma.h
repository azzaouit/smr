#ifndef RDMA_H
#define RDMA_H

#include <infiniband/verbs.h>

#include "config.h"
#include "net.h"

/* RDMA context */
struct rdma {
  struct config *c;        // Network config
  struct ibv_context *ctx; // ib context
  struct ibv_pd *pd;       // protection domain used for both memory regions
  struct ibv_mr *mr[SMR_NPLANES];     // MRs for rep/bg planes
  struct ibv_qp **qp;                 // QPs for rep/bg planes
  struct ibv_cq *cq[SMR_NPLANES - 1]; // Shared CQs for rep/bg planes
  struct remote_attr *ra;             // remote attributes
  uint16_t lid;                       // local device id
  uint8_t gid[16];                    // global device id
};

/* Memory transaction defines remote read/write operation. */
struct mem_tx {
  enum SMR_PLANE local_plane;  // LOCAL plane
  uint64_t local_addr;         // LOCAL addr
  enum SMR_PLANE remote_plane; // REMOTE plane
  uint64_t remote_addr;        // REMOTE addr
  off_t remote_offset;         // REMOTE offset
                               // full addr = remote_addr + remote_offset
  size_t len;                  // size of payload
};

/* Initialize an RDMA context */
int rdma_init(struct config *c, struct rdma *r);

/* Add memory region */
int rdma_add_mr(struct rdma *r, void *addr, size_t len, enum SMR_PLANE plane);

/* Add a queue pair */
int rdma_add_qp(struct rdma *r, enum SMR_PLANE plane, int id);

/* Exchange QP info and connect remote MRs */
int rdma_handshake(struct rdma *r);

/* Write to a remote MR */
int rdma_write(struct rdma *r, struct mem_tx *t, uint16_t id);

/* Read from a remote MR */
int rdma_read(struct rdma *r, struct mem_tx *t, uint16_t id);

/* Increment a remote value */
int rdma_inc(struct rdma *r, struct mem_tx *t, uint16_t id);

/* Wait for n work completions */
int rdma_wait(struct rdma *r, enum SMR_PLANE plane, size_t n);

/* Release any resources held by the RDMA context*/
void rdma_destroy(struct rdma *r);

#endif /* RDMA_H */
