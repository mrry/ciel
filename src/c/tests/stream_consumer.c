
#define _GNU_SOURCE

#include <jansson.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>

#include "libciel.h"

int main(int argc, char** argv) {

  if(argc < 9) {
    fprintf(stderr, "stream_producer needs at least 6 arguments\n");
    exit(1);
  }
  
  if(strcmp(argv[1], "--write-fifo") != 0) {
    fprintf(stderr, "stream_producer first arg must be --write-fifo\n");
    exit(1);
  }
  if(strcmp(argv[3], "--read-fifo") != 0) {
    fprintf(stderr, "stream producer third arg must be --read-fifo\n");
    exit(1);
  }

  printf("C stream consumer: start\n");

  ciel_init(argv[2], argv[4]);

  printf("FIFOs open\n");

  char* ref_id = argv[5];
  int may_stream = !strcmp(argv[6], "True");
  int sole_consumer = !strcmp(argv[7], "True");
  int must_block = !strcmp(argv[8], "True");

  json_t* task_private = ciel_get_task();
  // Don't care
  json_decref(task_private);

  ciel_block_on_refs(1, ref_id);

  struct ciel_input* input = ciel_open_ref_async(ref_id, 1024*1024*64, may_stream, sole_consumer, must_block);

  char read_buffer[4096];
  
  long bytes_read = 0;

  while(1) {
    int this_read = ciel_read_ref(input, read_buffer, 4096);
    if(this_read == -1) {
      fprintf(stderr, "Error reading input!");
      exit(1);
    }
    else if(this_read == 0) {
      break;
    }
    bytes_read += this_read;
  }

  ciel_close_ref(input);
  
  char* response_string;
  asprintf(&response_string, "Consumer read %ld bytes\n", bytes_read);
  ciel_define_output_with_plain_string(0, response_string);
  free(response_string);

  ciel_exit();

  return 0;

}
