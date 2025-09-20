#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <time.h>

typedef enum {
  SMR_LOG_ERROR,
  SMR_LOG_WARN,
  SMR_LOG_INFO,
  SMR_LOG_DEBUG
} smr_log_level_t;

static int smr_log_verbosity = 0; // 0=ERROR, 1=WARN, 2=INFO, 3=DEBUG

#define SMR_LOG_SET_VERBOSITY(level) (smr_log_verbosity = (level))

static inline const char *smr_log_level_str(smr_log_level_t lvl) {
  switch (lvl) {
  case SMR_LOG_ERROR:
    return "ERROR";
  case SMR_LOG_WARN:
    return "WARN";
  case SMR_LOG_INFO:
    return "INFO";
  case SMR_LOG_DEBUG:
    return "DEBUG";
  default:
    return "LOG";
  }
}

#define __SMR_LOG(level, fmt, ...)                                             \
  do {                                                                         \
    if ((level) <= smr_log_verbosity) {                                        \
      time_t _now = time(NULL);                                                \
      struct tm _tm;                                                           \
      localtime_r(&_now, &_tm);                                                \
      char _buf[20];                                                           \
      strftime(_buf, sizeof(_buf), "%Y-%m-%d %H:%M:%S", &_tm);                 \
      fprintf(stderr, "[%s][%s][%s:%d] " fmt "\n", _buf,                       \
              smr_log_level_str(level), __FILE__, __LINE__, ##__VA_ARGS__);    \
    }                                                                          \
  } while (0)

#define SMR_LOG_ERR(MSG, ...) __SMR_LOG(SMR_LOG_ERROR, MSG, ##__VA_ARGS__)
#define SMR_LOG(MSG, ...) __SMR_LOG(SMR_LOG_INFO, MSG, ##__VA_ARGS__)

#endif /* LOG_H */
