default: binomial-fc stream_consumer stream_producer

FC_LD_FLAGS=-L../../../../ext/jansson-install/lib -ljansson -L../../src -lciel -Wl,-rpath,'$$ORIGIN/../../../../ext/jansson-install/lib' -Wl,-rpath,'$$ORIGIN/../../src'

FC_C_FLAGS=-I../../../../ext/jansson-install/include -I../../include -D_FILE_OFFSET_BITS=64

binomial-fc.o: binomial-fc.c
	gcc binomial-fc.c -c -o binomial-fc.o -std=gnu99 $(FC_C_FLAGS)

binomial-fc: binomial-fc.o
	gcc binomial-fc.o -o binomial-fc $(FC_LD_FLAGS) -lm

stream_consumer.o: stream_consumer.c
	gcc stream_consumer.c -std=gnu99 -c -o stream_consumer.o $(FC_C_FLAGS)

stream_producer.o: stream_producer.c
	gcc stream_producer.c -std=gnu99 -c -o stream_producer.o $(FC_C_FLAGS)

stream_consumer: stream_consumer.o
	gcc stream_consumer.o -o stream_consumer $(FC_LD_FLAGS)

stream_producer: stream_producer.o
	gcc stream_producer.o -o stream_producer $(FC_LD_FLAGS)

clean:
	-rm *.o binomial-fc stream_consumer stream_producer