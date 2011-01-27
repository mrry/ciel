CFLAGS = -Os -Wall -g -m32
LDFLAGS = -m32

all: timespin

timespin: timespin.o
	gcc $(LDFLAGS) $^ -o $@

%.o: %.c
	gcc $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o timespin
