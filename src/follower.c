#include <pthread.h>
#include <unistd.h>

#include "consensus.h"
#include "scratchpad.h"

void *__smr__follower_thread(void *p) {
  SMR_LOG("Follower thread starting\n");
  struct consensus *n = (struct consensus *)p;
  struct log_header *h = &n->log->h;
  uint16_t npeers = n->c->n;
  uint16_t host_id = n->c->host_id;
  size_t prev_size = 0;
  for (;;) {
    pthread_testcancel();
    if (h->size != prev_size) {
      for (uint16_t i = 0; i < npeers; ++i)
        if (i != host_id) {
          LOG_HEADER(n, i)->size = h->size;
          LOG_HEADER(n, i)->fuo = h->fuo;
          LOG_HEADER(n, i)->capacity = h->capacity;
          LOG_HEADER(n, i)->minprop = h->minprop;
          LOG_HEADER(n, i)->buf += h->size;
        }
    }
    prev_size = h->size;
    sleep(1);
  }
}

int consensus_follower_init(struct consensus *n) {
  if (pthread_create(&n->follower_thread, NULL, __smr__follower_thread,
                     (void *)n)) {
    perror("pthread_create");
    return -errno;
  }
  return 0;
}

void consensus_follower_destroy(struct consensus *n) {
  SMR_LOG("Follower thread exiting\n");
  pthread_cancel(n->follower_thread);
  pthread_join(n->follower_thread, NULL);
}
