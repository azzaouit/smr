# libsmr

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

# Docker

```sh
# Build image
docker build -t smr .

# Pass specific RDMA devices
docker run --device=/dev/infiniband/uverbs0 \
  --device=/dev/infiniband/rdma_cm \
  --cap-add=IPC_LOCK \
  --cap-add=SYS_RESOURCE \
  -it smr
```

# Tests

Test binaries are found in `tests/`

RoCE can be setup for testing with

```sh
sudo make roce
```

and removed with

```sh
sudo make roce-clean
```

# References

Aguilera, Marcos K., et al. "Microsecond consensus for microsecond applications." 14th USENIX Symposium on Operating Systems Design and Implementation (OSDI 20). 2020.
