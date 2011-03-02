#include <stdlib.h>
#include <stdio.h>
#include <math.h>

double dot_product (int dim, double x[dim], double y[dim]) {
  double ret = 0.0;
  int i;
  for (i = 0; i < dim; ++i) {
    ret += x[i] * y[i];
  }
  return ret;
}

int main (int argc, char *argv[]) {

  if (argc < 6) {
    fprintf(stderr, "Usage: lr-map <DIM> <DATA FILE> <W FILE> <OUT FILE> <PARSE>\n");
    return -1;
  }

  int dim = atoi(argv[1]);
  FILE *datafile = fopen(argv[2], "r");
  FILE *wfile = fopen(argv[3], "r");
  int should_parse = atoi(argv[5]);

  int curr_label;
  double curr_val[dim];
  double w_vector[dim];
  double result[dim];

  int count = 0;

  if ((fread(w_vector, sizeof(w_vector[0]), dim, wfile)) != dim) {
    fprintf(stderr, "Parsing error reading wfile.\n");
    return -2;
  }

  int i;
  for (i = 0; i < dim; ++i) {
    result[i] = 0.0;
  } 

  while (should_parse ? (fscanf(datafile, "%d", &curr_label) != EOF) : (fread(&curr_label, sizeof(curr_label), 1, datafile) == 1)) {

    // Read a row from the datafile.
    if (should_parse) {
      for (i = 0; i < dim; ++i) {
	if (fscanf(datafile, "%lf", &curr_val[i]) != 1) {
	  fprintf(stderr, "Parsing error.\n");
	  return -2;
	}
      }
    } else {
      if ((fread(curr_val, sizeof(curr_val[0]), dim, datafile)) != dim) {
	fprintf(stderr, "I/O error\n");
	return -2;
      }
    }
      
    // Do the logistic regression calculation.
    double scale = (1.0 / (1.0 + exp(((double) -curr_label) * (dot_product(dim, w_vector, curr_val)))) - 1) * ((double) curr_label);

    for (i = 0; i < dim; ++i) {
      result[i] += scale * curr_val[i];
    }

    //for (i = 0; i < dim; ++i) {
    //  fprintf(stderr, "%lf ", result[i]);
    //}
    //fprintf(stderr, "\n");

    count++;
  }

  fclose(wfile);
  fclose(datafile);

  // Write results (possibly to the same file as wfile).
  FILE *outfile = fopen(argv[4], "w");
  fwrite(result, sizeof(result[0]), dim, outfile);
  fclose(outfile);

  for (i = 0; i < dim; ++i) {
    fprintf(stderr, "New w[%d] = %lf\n", i, result[i]);
  }

  fprintf(stderr, "Parsed %d rows successfully.\n", count);

  return EXIT_SUCCESS;

}
