#ifndef CONSENSUS_H
#define CONSENSUS_H

#include "rdma.h"
#include <stdbool.h>

/* Log slot description */
struct slot {
  size_t len;      // nbytes stored in this slot
  uint8_t *buf;    // ptr to data in this slot
  uint32_t propno; // proposal number
} __attribute__((packed, aligned(8)));

/* Log header description */
struct log_header {
  size_t size;      // size of the log (this is always <= capacity)
  uint32_t fuo;     // first undecided offset
  size_t capacity;  // log capacity
  uint32_t minprop; // minimum proposal number
  uint8_t *buf;     // log data of size (capacity * sizeof(slot))
} __attribute__((packed, aligned(64)));

/* Full log replicated across peers */
struct log {
  struct log_header h;              // log header
  struct slot slots[SMR_MAX_SLOTS]; // slots
  uint8_t data[];                   // log data
} __attribute__((packed, aligned(64)));

/* Leader election handle */
struct leader_election {
  uint16_t leader; // current leader
  uint64_t *hb;    // all heartbeats
  int64_t *scores; // all scores
  pthread_t tid;   // leader election thread handle
};

/* Main consensus handle */
struct consensus {
  struct config *c;          // Network configuration
  struct rdma r;             // RDMA context
  struct log *log;           // Replication log
  uint8_t *bg;               // Background metadata
  uint8_t *buf;              // log buffer
  struct leader_election le; // leader election context
  bool fast_path_enabled;    // flag to enable fast path optimization
};

/* Initialize consensus */
int consensus_init(struct config *c, struct consensus *n, size_t log_size);

/* Connect to remote peers */
int consensus_connect(struct consensus *n);

/* Propose a value */
int consensus_propose(struct consensus *n, uint8_t *buf, size_t len);

/* Flush k work requests */
int consensus_wait(struct consensus *n, uint8_t plane, size_t k);

/* Free resources and close RDMA device */
void consensus_destroy(struct consensus *);

/* Print full log to a file */
void consensus_dump_log(FILE *fp, struct consensus *);

#endif /* CONSENSUS_H */
