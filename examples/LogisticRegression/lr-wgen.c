#include <stdlib.h>
#include <stdio.h>

int main(int argc, char *argv[]) {
  
  if (argc < 3) {
    fprintf(stderr, "Usage: lr-wgen <DIM> <SEED>\n");
    return -1;
  }

  int dim = atoi(argv[1]);
  srand(atoi(argv[2]));

  int i;
  for (i = 0; i < dim; ++i) {
    double curr_elem = (double) rand() / RAND_MAX;
    fwrite(&curr_elem, sizeof(curr_elem), 1, stdout);
    fprintf(stderr, "Elem[%d] = %lf\n", i, curr_elem);
  }

  return EXIT_SUCCESS;

}
