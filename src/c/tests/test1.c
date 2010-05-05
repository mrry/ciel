
#include "sw_task.h"
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

int test_entry_point(int nInputs, int* inputFds, int nOutputs, int* outputFds, int argc, char** argv) {

  printf("Hello, I have %d inputs, %d outputs and %d arguments\n", nInputs, nOutputs, argc);

  for(int i = 0; i < nInputs; i++) {

    char c;
    int ret = read(inputFds[i], &c, sizeof(char));
    if(ret != 1) {
      printf("Failed to read from input %d (FD %d): ", i, inputFds[i]);
      if(ret == 0) {
	printf("End of file\n");
      }
      else {
	printf("%s\n", strerror(errno));
      }
    }
    else {
      printf("Input %d starts with the character '%c'\n", i, c);
    }
      
  }

  for(int i = 0; i < nOutputs; i++) {

    char c = (char)(((int)'j') + i);
    int ret = write(outputFds[i], &c, sizeof(char));
    if(ret != 1) {
      printf("Failed to write to output %d (FD %d): %s\n", i, outputFds[i], strerror(errno));
    }
    else {
      printf("Wrote character '%c' to output %d\n", c, i);
    }

  }

  for(int i = 0; i < argc; i++) {
    printf("Argument %d is %s\n", i, argv[i]);
  }

  return 0;

}
