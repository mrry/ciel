all: libtest1.so libtest2a.so libtest2b.so

libtest1.so: test1.c
	gcc -std=gnu99 -I../include -fpic -shared -Wl,-soname,libtest1.so -o libtest1.so test1.c

libtest2a.so: test2a.c
	gcc -std=gnu99 -I../include -fpic -shared -Wl,-soname,libtest2a.so -o libtest2a.so test2a.c

libtest2b.so: test2b.c
	gcc -std=gnu99 -I../include -fpic -shared -Wl,-soname,libtest2b.so -o libtest2b.so test2b.c

stream_consumer: stream_consumer.o libciel.o
	gcc stream_consumer.o libciel.o -o stream_consumer -ljansson

stream_producer: stream_producer.o libciel.o
	gcc stream_producer.o libciel.o -o stream_producer -ljansson

stream_consumer.o: stream_consumer.c
	gcc stream_consumer.c -std=gnu99 -c -o stream_consumer.o

stream_producer.o: stream_producer.c
	gcc stream_producer.c -std=gnu99 -c -o stream_producer.o

libciel.o: libciel.c
	gcc libciel.c -std=gnu99 -c -o libciel.o
