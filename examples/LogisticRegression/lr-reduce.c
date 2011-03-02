#include <stdlib.h>
#include <stdio.h>
#include <math.h>

int main (int argc, char *argv[]) {

  if (argc < 2) {
    fprintf(stderr, "Usage: lr-reduce <DIM>\n");
    return -1;
  }

  int dim = atoi(argv[1]);

  double w_vector[dim];
  double result[dim];

  int i;
  for (i = 0; i < dim; ++i) {
    result[i] = 0.0;
  } 

  int num_read;
  while ( (num_read = fread(w_vector, sizeof(w_vector[0]), dim, stdin)) == dim) {

    if (num_read != dim) {
      fprintf(stderr, "Parsing error reading wfile.\n");
      return -2;
    }
    
    for (i = 0; i < dim; ++i) {
      result[i] += w_vector[i];
    }

  }
  
  fwrite(result, sizeof(result[0]), dim, stdout);

  for (i = 0; i < dim; ++i) {
    fprintf(stderr, "New w[%d] = %lf\n", i, result[i]);
  }

  return EXIT_SUCCESS;

}
