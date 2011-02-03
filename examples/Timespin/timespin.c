/**
 * Simple script that busy-waits until a timer expires, with duration
 * specified (in seconds) on argv[1].
 *
 * Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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

#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <signal.h>

volatile sig_atomic_t flag = 1;

void catch_timer (int sig) {
  flag = 0;
}

int main (int argc, char *argv[]) {

  int secs = atoi(argv[1]);

  struct itimerval timer;
  timer.it_interval.tv_sec = 0;
  timer.it_interval.tv_usec = 0;
  timer.it_value.tv_sec = secs;
  timer.it_value.tv_usec = 0;

  if ( signal(SIGALRM, catch_timer) ) return -1;

  if ( setitimer(ITIMER_REAL, &timer, NULL) ) return -1;

  while (flag) {
    ;
  }

  printf("true");
  return 0;
}
