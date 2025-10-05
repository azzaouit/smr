#ifndef RDMA_H
#define RDMA_H

#include <infiniband/verbs.h>
#include <stdbool.h>

#include "config.h"
#include "net.h"

/* Max Work requests */
#define MAX_WR (1 << 3)

/* Max scatter-gather entries */
#define MAX_SGE (1 << 3)

/* Default RDMA send work request ID */
#define RDMA_SEND_WRID (1 << 0)

/* Default RDMA recv work request ID */
#define RDMA_RECV_WRID (1 << 1)

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
  uint32_t max_inline;                // max inline data (QP attribute)
#if SMR_NUMA_AWARE
  int cpu_affinity; // CPU affinity of the NIC's NUMA node
#endif
};

/* Memory transaction defines remote read/write operation. */
struct mem_tx {
  enum SMR_PLANE local_plane;  // LOCAL plane
  uint64_t local_addr;         // LOCAL addr
  enum SMR_PLANE remote_plane; // REMOTE plane
  uint64_t remote_addr;        // REMOTE addr
  off_t remote_offset;         // REMOTE offset
                               // full addr = remote_addr + remote_offset
#if SMR_RDMA_CAS_ENABLED
  uint64_t compare_addr; // Address of compare values for CAS
  uint64_t swap_addr;    // Address of swap values for CAS
#endif
  size_t len; // size of payload
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

#if SMR_RDMA_CAS_ENABLED
/* Remote compare-and-swap operation */
int rdma_cas(struct rdma *r, struct mem_tx *t, uint16_t id);
#endif

#if SMR_NUMA_AWARE
/* Set CPU affinity to NIC's NUMA node */
int rdma_set_cpu_affinity(struct rdma *r);
#endif

/* Release any resources held by the RDMA context*/
void rdma_destroy(struct rdma *r);

#endif /* RDMA_H */
