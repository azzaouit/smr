# libsmr

State Machine Replication over RDMA

# HW Requirements

RDMA NIC, OFED drivers

# Dependencies

To install dependencies

```bash
sudo apt-get install rdma-core libibverbs-dev librdmacm-dev ibverbs-utils
```

# Build

```sh
make
sudo make install
```

# Tests

Test binaries are found in `tests/`

# References

Aguilera, Marcos K., et al. "Microsecond consensus for microsecond applications." 14th USENIX Symposium on Operating Systems Design and Implementation (OSDI 20). 2020.
