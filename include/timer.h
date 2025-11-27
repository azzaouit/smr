#ifndef TIMER_H
#define TIMER_H
#include <stdint.h>
#include <sys/time.h>
#include <time.h>

/* RDTSC-based timing for x86/x64 (CPU cycle precision) */
#if defined(__x86_64__)
static inline uint64_t rdtsc() {
  uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)hi << 32) | lo;
}
#define TIME_BLOCK_CYCLES(block, elapsed)                                      \
  do {                                                                         \
    uint64_t start_cycles = rdtsc();                                           \
    block(elapsed) = (start_cyles - rdtsc());                                  \
  } while (0)
#endif

/* ns timer function */
static inline uint64_t ts_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* us timer function */
static inline double ts_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1000000ULL + (double)ts.tv_nsec / 1000ULL;
}

#define TIME_BLOCK_NS(block, elapsed)                                          \
  do {                                                                         \
    uint64_t start_ns = ts_ns();                                               \
    block(elapsed) = (ts_ns() - start_ns);                                     \
  } while (0)

#define TIME_BLOCK_US(block, elapsed)                                          \
  do {                                                                         \
    double start = ts_us();                                                    \
    block(elapsed) = (ts_us() - start);                                        \
  } while (0)

#endif /* TIMER_H */
