#include <stdlib.h>
#include <stdio.h>
#include <math.h>

double ranf() {
  return ((double) rand()) / ((double) RAND_MAX);
}

double box_muller(double m, double s)  /* normal random variate generator */
{                       /* mean m, standard deviation s */
    double x1, x2, w, y1;
    static double y2;
    static int use_last = 0;

    if (use_last)               /* use value from previous call */
    {
        y1 = y2;
        use_last = 0;
    }
    else
    {
        do {
            x1 = 2.0 * ranf() - 1.0;
            x2 = 2.0 * ranf() - 1.0;
            w = x1 * x1 + x2 * x2;
        } while ( w >= 1.0 );

        w = sqrt( (-2.0 * log( w ) ) / w );
        y1 = x1 * w;
        y2 = x2 * w;
        use_last = 1;
    }

    return( m + y1 * s );
}

int main(int argc, char *argv[]) {
  
  if (argc < 5) {
    fprintf(stderr, "Usage: lr-datagen <DIM> <NPOINTS> <SEED> <SEP>\n");
    return -1;
  }

  int dim = atoi(argv[1]);
  int npoints = atoi(argv[2]);
  srand(atoi(argv[3]));
  double sep = (double) atoi(argv[4]);

  int i, j;
  for (j = 0; j < npoints; ++j) {
    int curr_label = (j < (npoints / 2)) ? 1 : -1;
    fprintf(stdout, "%d", curr_label);
    for (i = 0; i < dim; ++i) {
      double curr_elem = box_muller(0.0, 3.0) * (double)curr_label * sep;
      fprintf(stdout, "\t%lf", curr_elem);
    }
    fprintf(stdout, "\n");
  }

  return EXIT_SUCCESS;

}
