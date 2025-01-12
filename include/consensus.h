#ifndef CONSENSUS_H
#define CONSENSUS_H

#include "rdma.h"
#include <pthread.h>
#include <time.h>

struct slot {
  size_t len;
  uint8_t *buf;
  uint32_t propno;
} __attribute__((packed));

struct log_header {
  size_t size;
  uint32_t fuo;
  size_t capacity;
  uint32_t minprop;
  uint8_t *buf;
} __attribute__((packed));

struct log {
  struct log_header h;
  struct slot slots[SMR_MAX_SLOTS];
  uint8_t data[];
} __attribute__((packed));

struct leader_election {
  uint16_t leader;
  uint64_t *hb;
  int64_t *scores;
  pthread_t tid[3];
};

enum SMR_ROLE { SMR_ROLE_LEADER = 0, SMR_ROLE_FOLLOWER };

/* Main consensus handle */
struct consensus {
  /* Network configuration */
  struct config *c;
  /* RDMA context */
  struct rdma r;
  /* Replication log */
  struct log *log;
  /* Background metadata */
  uint8_t *bg;
  /* Log buffer */
  uint8_t *buf;
  /* Active nodes */
  uint8_t *active;
  /* Number of active nodes */
  size_t nactive;
  /* Leader lock */
  pthread_mutex_t leader_lock;
  /* Leader election context */
  struct leader_election le;
  pthread_t follower_thread;
  enum SMR_ROLE role;
};

/* Initialize consensus */
int consensus_init(struct config *c, struct consensus *n, size_t log_size);

/* Initialize leader context */
int consensus_leader_init(struct consensus *n);

/* Initialize follower context */
int consensus_follower_init(struct consensus *n);

/* Initialize leader election context */
int consensus_leader_election_init(struct consensus *n);

/* Connect to remote peers */
int consensus_connect(struct consensus *n);

/* Propose a value */
int consensus_propose(struct consensus *n, uint8_t *buf, size_t len);

/* Join follower threads */
void consensus_follower_destroy(struct consensus *n);

/* Join leader election threads */
void consensus_leader_election_destroy(struct consensus *n);

/* Free resources and close RDMA device */
void consensus_destroy(struct consensus *);

#endif /* CONSENSUS_H */
