#ifndef NET_H
#define NET_H

/* Convert bytes to network order */
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define htonll(x) (((uint64_t)htonl(x) << 32) | htonl((x) >> 32))
#define ntohll(x) (((uint64_t)ntohl(x) << 32) | ntohl((x) >> 32))
#else
#define htonll(x) (x)
#define ntohll(x) (x)
#endif

/* Convert remote attribute struct from host order to network order */
#define RA_TO_NET(r)                                                           \
  do {                                                                         \
    (r)->addr = htonll((r)->addr);                                             \
    (r)->rkey = htonl((r)->rkey);                                              \
    (r)->lid = htons((r)->lid);                                                \
    (r)->qpn = htonl((r)->qpn);                                                \
    (r)->psn = htonl((r)->psn);                                                \
  } while (0)

/* Convert remote attribute struct from network order to host order */
#define RA_FROM_NET(r)                                                         \
  do {                                                                         \
    (r)->addr = ntohll((r)->addr);                                             \
    (r)->rkey = ntohl((r)->rkey);                                              \
    (r)->lid = ntohs((r)->lid);                                                \
    (r)->qpn = ntohl((r)->qpn);                                                \
    (r)->psn = ntohl((r)->psn);                                                \
  } while (0)

/* Dump remote attribute to stdout */
#define PRINT_RA(r)                                                            \
  do {                                                                         \
    printf("%lx %x %x %x %x\n", (r)->addr, (r)->rkey, (r)->lid, (r)->qpn,      \
           (r)->psn);                                                          \
  } while (0)

/* Remote QP attributes. Exchanged over TCP during the RDMA handshake */
struct remote_attr {
  uint64_t addr;
  uint32_t rkey;
  uint16_t lid;
  uint32_t qpn;
  uint32_t psn;
  uint8_t gid[16];
} __attribute__((packed));

#endif /* NET_H */
