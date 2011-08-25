default: loader libciel.so

loader: loader.c
	gcc -std=gnu99 -o loader -I../include -ldl loader.c

streamfs: streamfs.c
	gcc -Wall -Werror -D_FILE_OFFSET_BITS=64 -o streamfs streamfs.c -lfuse -lpthread

libciel.o: libciel.c
	gcc libciel.c -std=gnu99 -Wall -c -o libciel.o -D_FILE_OFFSET_BITS=64 -fPIC -I../../../ext/jansson-install/include

libciel.so: libciel.o
	gcc libciel.o -o libciel.so -shared -Wl,-rpath,'$$ORIGIN/../../../ext/jansson-install/lib' -L../../../ext/jansson-install/lib -ljansson

clean:
	-rm *.o *.so loader streamfs