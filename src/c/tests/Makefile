
.PHONY: first-class

all: libtest1.so libtest2a.so libtest2b.so first-class

libtest1.so: test1.c
	gcc -std=gnu99 -I../include -fpic -shared -Wl,-soname,libtest1.so -o libtest1.so test1.c

libtest2a.so: test2a.c
	gcc -std=gnu99 -I../include -fpic -shared -Wl,-soname,libtest2a.so -o libtest2a.so test2a.c

libtest2b.so: test2b.c
	gcc -std=gnu99 -I../include -fpic -shared -Wl,-soname,libtest2b.so -o libtest2b.so test2b.c

first-class:
	make -C first-class

clean:
	-rm *.so
	make -C first-class clean