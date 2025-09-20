#ifndef TIMER_H
#define TIMER_H
#define _GNU_SOURCE

#include <stdint.h>
#include <stdio.h>
#include <time.h>

/* Platform-specific includes for highest resolution timing */
#ifdef _WIN32
#include <windows.h>
#elif defined(__MACH__)
#include <mach/mach_time.h>
#else
#include <sys/time.h>
#endif

/* Cross-platform high-resolution timer function */
static inline uint64_t get_nanoseconds() {
#ifdef _WIN32
  LARGE_INTEGER frequency, counter;
  QueryPerformanceFrequency(&frequency);
  QueryPerformanceCounter(&counter);
  return (uint64_t)((double)counter.QuadPart * 1000000000.0 /
                    frequency.QuadPart);
#elif defined(__MACH__)
  static mach_timebase_info_data_t timebase_info = {0, 0};
  if (timebase_info.denom == 0) {
    mach_timebase_info(&timebase_info);
  }
  return mach_absolute_time() * timebase_info.numer / timebase_info.denom;
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

/* RDTSC-based timing for x86/x64 (CPU cycle precision) */
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) ||             \
    defined(_M_IX86)
static inline uint64_t rdtsc() {
#ifdef _MSC_VER
  return __rdtsc();
#else
  uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)hi << 32) | lo;
#endif
}
#endif

#define TIME_BLOCK_NS(block)                                                   \
  do {                                                                         \
    uint64_t start_ns = get_nanoseconds();                                     \
    block uint64_t end_ns = get_nanoseconds();                                 \
    uint64_t elapsed_ns = end_ns - start_ns;                                   \
    printf("Block executed in %llu nanoseconds (%.3f μs, %.6f ms)\n",          \
           (unsigned long long)elapsed_ns, elapsed_ns / 1000.0,                \
           elapsed_ns / 1000000.0);                                            \
  } while (0)

#define TIME_BLOCK_CYCLES(block)                                               \
  do {                                                                         \
    uint64_t start_cycles = rdtsc();                                           \
    block uint64_t end_cycles = rdtsc();                                       \
    uint64_t elapsed_cycles = end_cycles - start_cycles;                       \
    printf("Block executed in %llu CPU cycles\n",                              \
           (unsigned long long)elapsed_cycles);                                \
  } while (0)

/* Ultra-precise statistical timing */
#define TIME_BLOCK_NS_STATS(label, iterations, block)                          \
  do {                                                                         \
    uint64_t total_ns = 0, min_ns = UINT64_MAX, max_ns = 0;                    \
    for (int _i = 0; _i < iterations; _i++) {                                  \
      uint64_t start_ns = get_nanoseconds();                                   \
      block uint64_t end_ns = get_nanoseconds();                               \
      uint64_t elapsed_ns = end_ns - start_ns;                                 \
      total_ns += elapsed_ns;                                                  \
      if (elapsed_ns < min_ns)                                                 \
        min_ns = elapsed_ns;                                                   \
      if (elapsed_ns > max_ns)                                                 \
        max_ns = elapsed_ns;                                                   \
    }                                                                          \
    double avg_ns = (double)total_ns / iterations;                             \
    printf("%s stats (%d runs):\n", label, iterations);                        \
    printf("  avg: %.1f ns (%.3f μs)\n", avg_ns, avg_ns / 1000.0);             \
    printf("  min: %llu ns (%.3f μs)\n", (unsigned long long)min_ns,           \
           min_ns / 1000.0);                                                   \
    printf("  max: %llu ns (%.3f μs)\n", (unsigned long long)max_ns,           \
           max_ns / 1000.0);                                                   \
  } while (0)

/* Microsecond precision (alternative for systems without nanosecond support) */
#define TIME_BLOCK_US(block)                                                   \
  do {                                                                         \
    struct timeval start, end;                                                 \
    gettimeofday(&start, NULL);                                                \
    block gettimeofday(&end, NULL);                                            \
    uint64_t elapsed_us = (end.tv_sec - start.tv_sec) * 1000000ULL +           \
                          (end.tv_usec - start.tv_usec);                       \
    printf("Block executed in %llu microseconds (%.3f ms)\n",                  \
           (unsigned long long)elapsed_us, elapsed_us / 1000.0);               \
  } while (0)

/* Benchmark macro for very fast operations */
#define BENCHMARK_NS(label, repeats, block)                                    \
  do {                                                                         \
    printf("Benchmarking %s (%d iterations)...\n", label, repeats);            \
    uint64_t start_ns = get_nanoseconds();                                     \
    for (int _i = 0; _i < repeats; _i++) {                                     \
      block                                                                    \
    }                                                                          \
    uint64_t end_ns = get_nanoseconds();                                       \
    uint64_t total_ns = end_ns - start_ns;                                     \
    double avg_ns = (double)total_ns / repeats;                                \
    printf("  Total: %llu ns (%.3f ms)\n", (unsigned long long)total_ns,       \
           total_ns / 1000000.0);                                              \
    printf("  Per iteration: %.1f ns (%.3f μs)\n", avg_ns, avg_ns / 1000.0);   \
  } while (0)

#endif /* TIMER_H */
