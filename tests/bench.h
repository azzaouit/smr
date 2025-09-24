#ifndef BENCH_H
#define BENCH_H

#include <float.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

/* Holds the calculated statistics for a benchmark run. */
struct __stats {
  double min;
  double max;
  double mean;
  double std_dev;
  long count;
  // Percentiles
  float p50; // Median
  float p90;
  float p95;
  float p99;
  float p999;
};

static inline int __compare_dbl(const void *a, const void *b) {
  double da = *(const double *)a;
  double db = *(const double *)b;
  return (da > db) - (da < db);
}

void __compute_stats(struct __stats *s, double *data, size_t len) {
  if (len == 0)
    return;

  double sum = 0.0;
  s->min = DBL_MAX;
  s->max = -DBL_MAX;

  for (size_t i = 0; i < len; ++i) {
    sum += data[i];
    if (data[i] < s->min)
      s->min = data[i];
    if (data[i] > s->max)
      s->max = data[i];
  }

  s->mean = sum / len;

  double sum_sq_diff = 0.0;
  for (size_t i = 0; i < len; ++i) {
    sum_sq_diff += (data[i] - s->mean) * (data[i] - s->mean);
  }
  s->std_dev = sqrt(sum_sq_diff / len);

  // Sort a copy of the data to calculate percentiles
  double *sorted_data = malloc(len * sizeof(double));
  if (!sorted_data) {
    perror("Failed to allocate buffer for sorting");
    return;
  }
  memcpy(sorted_data, data, len * sizeof(double));
  qsort(sorted_data, len, sizeof(double), __compare_dbl);

  s->p50 = sorted_data[(long)(len * 0.50)];
  s->p90 = sorted_data[(long)(len * 0.90)];
  s->p95 = sorted_data[(long)(len * 0.95)];
  s->p99 = sorted_data[(long)(len * 0.99)];
  s->p999 = sorted_data[(long)(len * 0.999)];

  free(sorted_data);
}

void bench_report(double *data, size_t len) {
  if (!data || !len)
    return;
  struct __stats s;
  __compute_stats(&s, data, len);
  printf("\n===========================================\n");
  printf("        Benchmark Report\n");
  printf("===========================================\n");
  printf("--- Summary ---\n");
  printf("Count:     %ld\n", len);
  printf("Mean:      %.4f\n", s.mean);
  printf("Std Dev:   %.4f\n", s.std_dev);
  printf("Min:       %.4f\n", s.min);
  printf("Max:       %.4f\n", s.max);
  printf("\n--- Percentiles ---\n");
  printf("50th (Median): %.4f\n", s.p50);
  printf("90th:          %.4f\n", s.p90);
  printf("95th:          %.4f\n", s.p95);
  printf("99th:          %.4f\n", s.p99);
  printf("99.9th:        %.4f\n", s.p999);
  printf("===========================================\n\n");
}

#endif // BENCH_H
