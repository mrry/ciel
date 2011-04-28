
#define _GNU_SOURCE

#include <jansson.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>

#include "libciel.h"

int main(int argc, char** argv) {

  if(argc < 5) {
    fprintf(stderr, "stream_producer needs at least 4 arguments\n");
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

  printf("C stream producer: start\n");

  ciel_init(argv[2], argv[4]);

  printf("FIFOs open\n");

  json_t* task_private = ciel_get_task();

  int n_chunks;
  int may_stream;
  int may_pipe;
  
  json_error_t error_bucket;

  if(json_unpack_ex(task_private, &error_bucket, 0, "{s[ibb]}", "proc_pargs", &n_chunks, &may_stream, &may_pipe)) {
    ciel_json_error(0, &error_bucket);
    exit(1);
  }

  json_decref(task_private);

  char* filename = ciel_open_output(1, may_stream, may_pipe, 0);
  
  FILE* fout = fopen(filename, "w");

  char write_buffer[4096];
  for(int i = 0; i < 4096; i++)
    write_buffer[i] = (32 + (i % 32));
  
  for(int i = 0; i < n_chunks; i++) {
    for(int j = 0; j < 16384; j++) {
      ciel_write_all(fout, write_buffer, 4096);
    }
  }

  fflush(fout);
  fclose(fout);
  
  long bytes_written = (long)(4096*16384)* (long)(n_chunks);
  json_t* out_ref = ciel_close_output(1, bytes_written);
  json_decref(out_ref);
  
  char* response_string;
  asprintf(&response_string, "Producer wrote %ld bytes\n", bytes_written);
  ciel_define_output_with_plain_string(0, response_string);
  free(response_string);

  ciel_exit();

  return 0;

}
