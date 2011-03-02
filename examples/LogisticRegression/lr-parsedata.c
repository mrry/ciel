#include <stdlib.h>
#include <stdio.h>

int main (int argc, char *argv[]) {

  if (argc < 2) {
    fprintf(stderr, "Usage: lr-parsedata <DIM>\n");
    return -1;
  }

  int dim = atoi(argv[1]);

  int curr_label;
  double curr_val;

  int count = 0;

  while ((fscanf(stdin, "%d", &curr_label) != EOF)) {

    fwrite(&curr_label, sizeof(curr_label), 1, stdout);

    int i;
    for (i = 0; i < dim; ++i) {
      if (fscanf(stdin, "%lf", &curr_val) != 1) {
	fprintf(stderr, "Parsing error.\n");
	return -2;
      }
      fwrite(&curr_val, sizeof(curr_val), 1, stdout);
    }
    count++;
  }

  fprintf(stderr, "Parsed %d rows successfully.\n", count);

  return EXIT_SUCCESS;

}
