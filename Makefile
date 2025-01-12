CC:=gcc
CFLAGS:=-fstack-protector -fPIC -O3 -Wall -Wextra -Werror -Iinclude
LDFLAGS:=-libverbs -shared
LIB=libsmr.so
PREFIX=/usr/local

SRC=$(wildcard src/*.c)
OBJ=$(SRC:.c=.o)

TEST_CFLAGS:=-g3 -fno-omit-frame-pointer ${CFLAGS} #-fsanitize=address
TEST_LDFLAGS:=-Wl,-rpath,$(shell pwd) $(LIB)
TESTS=$(patsubst %.c, %, $(wildcard tests/*.c))

all: build tests

build: $(OBJ)
	$(CC) -o $(LIB) $(OBJ) $(LDFLAGS)

tests: build $(TESTS)

$(TESTS): %: %.c
	$(CC) $< $(TEST_CFLAGS) -o $@ $(TEST_LDFLAGS)

src/%.o: src/%.c
	$(CC) -c $< -o $@ $(CFLAGS)

install: all
	cp $(LIB) $(PREFIX)/lib
	cp -r include $(PREFIX)/include/smr

uninstall:
	rm -f $(PREFIX)/lib/$(LIB)
	rm -rf $(PREFIX)/include/smr

clean:
	rm -rf $(OBJ) $(TESTS) $(LIB)

memcheck: tests
	valgrind --leak-check=full --fair-sched=yes tests/test_loopback
