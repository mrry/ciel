// (c) Anil Madhavapeddy <anil@recoil.org>
// In the public domain
// Parallel binomial options parser for use with Skywriting

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <err.h>

#include <jansson.h>
#include "libciel.h"

static double u,d,drift,q;
static double s,k,t,v,rf,cp;
static int n, chunk, start;

struct ciel_input* binopt_ciel_in;
struct ciel_output* binopt_ciel_out;

#define INPUT(v) \
  do { \
  int bytes_read = 0; \
  while(bytes_read < sizeof(v)) { \
    int this_read = ciel_read_ref(binopt_ciel_in, ((char*)&v) + bytes_read, sizeof(v) - bytes_read);	\
    if(this_read <= 0) { \
      fprintf(stderr, "Error reading\n"); \
      exit(1); \
    } \
    bytes_read += this_read; \
  } \
  } while(0);
#define OUTPUT(v) \
 do { \
 int bytes_written = 0; \
 while(bytes_written < sizeof(v)) { \
   int this_write = ciel_write_output(binopt_ciel_out, ((char*)&v) + bytes_written, sizeof(v) - bytes_written); \
   if(this_write <= 0) {     \
     fprintf(stderr, "Error writing\n"); \
     exit(1);				 \
   } \
   bytes_written += this_write; \
 }			       \
} while(0);

void
init_vals(void)
{
  double h = t / (double)n;
  double xd = (rf - 0.5 * pow(v,2)) * h;
  double xv = v * sqrt(h);
  u = exp(xd + xv);
  d = exp(xd - xv);
  drift = exp(rf * h);
  q = (drift - d) / (u - d);
}  

void
gen_initial_optvals(void)
{
  double stkval;
  for (int j=n; j>-1; j--) {
    stkval = s * pow(u,(n-j)) * pow(d,j);
    stkval = fmax(0, (cp * (stkval-k)));
    OUTPUT(stkval);
  }
}

void process_rows(int rowstart, int rowto)
{
  OUTPUT(rowto);
  int chunk = rowstart - rowto;
  double *acc = malloc((chunk+1)*sizeof(double));
  if (!acc) err(1, "malloc");
  for (int pos=0; pos<=rowstart; pos++) {
    double v,v2;
    INPUT(v);
    v2=acc[0];
    acc[0]=v;
    int maxcol = chunk < pos ? chunk : pos;
    for (int idx=1; idx<=maxcol; idx++) {
      double nv = ((q * acc[idx-1]) + (1-q) * v2) / drift;
      v2 = acc[idx];
      acc[idx] = nv;
    }
    if (maxcol == chunk)
      OUTPUT(acc[maxcol]);
  }
}

#ifndef min
	#define min( a, b ) ( ((a) < (b)) ? (a) : (b) )
#endif

int 
main(int argc, char **argv)
{

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

  printf("C binopt: start\n");

  ciel_init(argv[2], argv[4]);

  json_t* task_private = ciel_get_task();

  json_error_t error_bucket;
  json_t* kwargs;

  char* s_str;
  char* k_str;
  char* t_str;
  char* v_str;
  char* rf_str;
  char* cp_str;
  char* n_str;
  char* chunk_str;
  char* start_str;

  fprintf(stderr, "doing decode\n");

  if(json_unpack_ex(task_private, &error_bucket, 0, "{s[sssssssss]so}", "proc_pargs", 
		    &s_str, &k_str, &t_str, &v_str, &rf_str, &cp_str, &n_str, 
		    &chunk_str, &start_str, "proc_kwargs", &kwargs)) {
    ciel_json_error("Failed to decode proc_pargs", &error_bucket);
    exit(1);
  }

  fprintf(stderr, "doing conversion\n");

  s = strtod(s_str, NULL);
  k = strtod(k_str, NULL);
  t = strtod(t_str, NULL);
  v = strtod(v_str, NULL);
  rf = strtod(rf_str, NULL);
  cp = strtod(cp_str, NULL);
  n = (int)strtod(n_str, NULL);
  chunk = (int)strtod(chunk_str, NULL);
  start = strcmp(start_str, "false");

  fprintf(stderr, "Got arguments: %g %g %g %g %g %g %d %d %s\n", s, k, t, v, rf, cp, n, chunk, start_str);

  if(!start) {
    
    json_t* in_ref = json_object_get(kwargs, "input_ref");
    binopt_ciel_in = ciel_open_ref_async(in_ref, 1, 1, 0);
    ciel_set_input_unbuffered(binopt_ciel_in);

  }
  else {
    binopt_ciel_in = 0;
  }

  json_decref(task_private);

  binopt_ciel_out = ciel_open_output(0, 1, 0, 0);
  ciel_set_output_unbuffered(binopt_ciel_out);

  init_vals();
  if (start == 1) {
    OUTPUT(n);
    gen_initial_optvals();
  } else {
    int rowstart=0;
    INPUT(rowstart);
    if (rowstart==0) {
      double v;
      INPUT(v);
      char buf[20];
      int chars_written = min(snprintf(buf, 20, "%.9f\n", v), 20);
      ciel_write_output(binopt_ciel_out, buf, chars_written);
    } else {
      int rowto = (rowstart - chunk) > 0 ? (rowstart - chunk) : 0;
      process_rows(rowstart, rowto);
    }
  }


  if(!start) {
    ciel_close_ref(binopt_ciel_in);
  }

  ciel_close_output(binopt_ciel_out);

  ciel_exit();

  return 0;
}
