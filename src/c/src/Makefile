default: loader streamfs

loader: loader.c
	gcc -std=gnu99 -o loader -I../include -ldl loader.c

streamfs: streamfs.c
	gcc -Wall -Werror -D_FILE_OFFSET_BITS=64 -o streamfs streamfs.c -lfuse -lpthread
