CC=gcc
CFLAGS=-fPIC -O3 -Wall -Wextra -Werror -Iinclude
LDFLAGS=-libverbs -shared
LIB=libsmr.so
PREFIX=/usr/local

SRC=$(wildcard src/*.c)
OBJ=$(SRC:.c=.o)

TEST_CFLAGS=-g -fno-omit-frame-pointer ${CFLAGS}
TEST_LDFLAGS=-Wl,-rpath,$(shell pwd) $(LIB)
TESTS=$(patsubst %.c, %, $(wildcard tests/*.c))

all: build tests

# Run as root to setup roce
roce:
	modprobe rdma_rxe
	rdma link add rxe0 type rxe netdev $(iface)

# Clean up roce
roce-clean:
	rdma link delete rxe0
	modprobe -r rdma_rxe

build: $(OBJ)
	$(CC) -o $(LIB) $(OBJ) $(LDFLAGS)

tests: build $(TESTS)

$(TESTS): %: %.c
	$(CC) $< $(TEST_CFLAGS) -o $@ $(TEST_LDFLAGS)

src/%.o: src/%.c
	$(CC) -c $< -o $@ $(CFLAGS)

install: all
	sudo cp $(LIB) $(PREFIX)/lib
	sudo cp -r include $(PREFIX)/include/smr

uninstall:
	sudo rm -f $(PREFIX)/lib/$(LIB)
	sudo rm -rf $(PREFIX)/include/smr

clean:
	rm -rf $(OBJ) $(TESTS) $(LIB)

format:
	find . -type f -name "*.c" -o -name "*.h" -exec clang-format -i {} \;
