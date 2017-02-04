CC=gcc
CFLAGS=-Wall -g -O3 -std=gnu99

all: iotester

iotester: iotester.o
	$(CC) $(CFLAGS) iotester.c -o iotester

clean:
	rm -f iotester
