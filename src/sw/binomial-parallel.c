// (c) Anil Madhavapeddy <anil@recoil.org>
// In the public domain
// Parallel binomial options parser for use with Skywriting

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <err.h>

static double u,d,drift,q;
static double s,k,t,v,rf,cp;
static int n, chunk, start;

// function nabbed from openssh, copyright openbsd @ /usr/src/usr.bin/ssh/atomicio.c
size_t
atomicio(f, fd, _s, n)
  ssize_t (*f) (int, void *, size_t);
  int fd;
  void *_s;
  size_t n;
{
  char *s = _s;
  size_t pos = 0;
  ssize_t res;

  while (n > pos) {
    res = (f) (fd, s + pos, n - pos);
    switch (res) {
    case -1:
      if (errno == EINTR || errno == EAGAIN)
        continue;
      return 0;
    case 0:
      errno = EPIPE;
      return pos;
    default:
      pos += (size_t)res;
    }
  }
  return pos;
}

#define SAFE_IO(fn,fd,v) \
  do { \
     ssize_t r = atomicio(fn,fd, (void *)(&(v)), sizeof((v))); \
     if (r != (sizeof((v)))) err(1,#fn); \
  } while(0)
#define vwrite (ssize_t (*)(int, void *, size_t))write
#define INPUT(v) SAFE_IO(read,STDIN_FILENO,v)
#define OUTPUT(v) SAFE_IO(vwrite,STDOUT_FILENO,v)

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

int 
main(int argc, char **argv)
{
  s = strtod(argv[1], NULL); 
  k = strtod(argv[2], NULL);
  t = strtod(argv[3], NULL);
  v = strtod(argv[4], NULL);
  rf = strtod(argv[5], NULL);
  cp = strtod(argv[6], NULL);
  n = (int)strtod(argv[7], NULL);
  chunk = (int)strtod(argv[8], NULL);
  start = (int)strtod(argv[9], NULL);
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
      printf("%.9f\n", v);
    } else {
      int rowto = (rowstart - chunk) > 0 ? (rowstart - chunk) : 0;
      process_rows(rowstart, rowto);
    }
  }
  return 0;
}
