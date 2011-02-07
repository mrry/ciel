/*
 * Copyright (c) 2010 Chris Smowton <chris.smowton@cl.cam.ac.uk>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include "sw_task.h"

int main(int argc, char** argv) {

  if(argc > 1 && !strcmp(argv[1], "--version")) {
    printf("Ciel C bindings v0.1\n");
    return 0;
  }

  if(argc < 3) {

    fprintf(stderr, "Usage: loader entry_point_name /fully/qualified/libname.so [other libs]\n");
    exit(1);

  }
  
  for(int i = 2; i < argc; i++) {

    if(!dlopen(argv[i], RTLD_LAZY | RTLD_GLOBAL)) {
      fprintf(stderr, "Failed to load %s: %s\n", argv[i], dlerror());
      exit(1);
    }

  }

  void* self_handle = dlopen(0, RTLD_LAZY);
  if(!self_handle) {

    fprintf(stderr, "Failed to get a self-handle from dlopen: %s\n", dlerror());
    exit(1);

  }

  skywriting_task_t to_invoke = dlsym(self_handle, argv[1]);
  if(!to_invoke) {

    fprintf(stderr, "Failed to find a symbol %s: %s\n", argv[1], dlerror());
    exit(1);

  }

  /* Step 2: Get input/output files and arguments! */

  char* entire_stdin = 0;
  int entire_stdin_length = 0;
  int this_read;
  char buffer[1024];

  while(this_read = fread(buffer, sizeof(char), sizeof(buffer), stdin)) {

    entire_stdin = realloc(entire_stdin, entire_stdin_length + this_read);
    if(!entire_stdin) {
      fprintf(stderr, "Failed to malloc %d bytes for stdin\n", entire_stdin_length + this_read);
      exit(1);
    }

    memcpy(&(entire_stdin[entire_stdin_length]), buffer, this_read);
    entire_stdin_length += this_read;

  }

  if(!entire_stdin_length) {
    fprintf(stderr, "Stdin did not deliver a single byte\n");
    exit(1);
  }
  entire_stdin[entire_stdin_length] = '\0'; // Should be the case anyway

  if(!feof(stdin)) {
    fprintf(stderr, "fread(stdin) resulted in a non-EOF error\n");
    exit(1);
  }

  int nInputs, nOutputs, nTargetArgs;
  int parsed = sscanf(entire_stdin, "%d,%d,%d", &nInputs, &nOutputs, &nTargetArgs);
  if(parsed != 3) {
    
    fprintf(stderr, "Failed to parse three integers from first stdin argument\n");
    exit(1);

  }

  entire_stdin_length -= (strlen(entire_stdin) + 1);
  entire_stdin += (strlen(entire_stdin) + 1);

  int* inputFds = malloc(sizeof(int) * nInputs);
  int* outputFds = malloc(sizeof(int) * nOutputs);
  char** targetArgs = malloc(sizeof(char*) * nTargetArgs);

  if(!(inputFds && outputFds && targetArgs)) {

    fprintf(stderr, "Failed to malloc space for %d inputs, %d outputs and %d arguments\n", nInputs, nOutputs, nTargetArgs);
    exit(1);

  }

  for(int i = 0; i < nInputs; i++) {
    
    if(!entire_stdin_length) {
      fprintf(stderr, "Ran out of stdin data reading input files (expected %d)\n", nInputs);
      exit(1);
    }

    inputFds[i] = open(entire_stdin, O_RDONLY);
    if(inputFds[i] == -1) {
      fprintf(stderr, "Error opening %s: %s\n", entire_stdin, strerror(errno));
      exit(1);
    }

    entire_stdin_length -= (strlen(entire_stdin) + 1);
    entire_stdin += (strlen(entire_stdin) + 1);

  }

  for(int i = 0; i < nOutputs; i++) {
    
    if(!entire_stdin_length) {
      fprintf(stderr, "Ran out of stdin data reading output files (expected %d)\n", nOutputs);
      exit(1);
    }

    outputFds[i] = open(entire_stdin, O_WRONLY);
    if(outputFds[i] == -1) {
      fprintf(stderr, "Error opening %s: %s\n", entire_stdin, strerror(errno));
      exit(1);
    }

    entire_stdin_length -= (strlen(entire_stdin) + 1);
    entire_stdin += (strlen(entire_stdin) + 1);

  }

  for(int i = 0; i < nTargetArgs; i++) {
    
    if(!entire_stdin_length) {
      fprintf(stderr, "Ran out of stdin data reading arguments (expected %d)\n", nTargetArgs);
      exit(1);
    }

    targetArgs[i] = entire_stdin;

    entire_stdin_length -= (strlen(entire_stdin) + 1);
    entire_stdin += (strlen(entire_stdin) + 1);

  }

  int target_ret = to_invoke(nInputs, inputFds, nOutputs, outputFds, nTargetArgs, targetArgs);
  if(target_ret) {
    fprintf(stderr, "Target returned non-zero value %d\n", target_ret);
    exit(2);
  }

  return 0;

}
