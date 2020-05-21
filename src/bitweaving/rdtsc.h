// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_EXAMPLE_RDTSC_H_
#define BITWEAVING_EXAMPLE_RDTSC_H_

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(__i386__) && !defined(__x86_64__) && !defined(__sparc__)
//#warning No supported architecture found -- timers will return junk.
#endif

static __inline__ unsigned long long curtick() {
  unsigned long long tick;
#if defined(__i386__)
  unsigned long lo, hi;
  __asm__ __volatile__ (".byte 0x0f, 0x31" : "=a" (lo), "=d" (hi));
  tick = (unsigned long long) hi << 32 | lo;
#elif defined(__x86_64__)
  unsigned long lo, hi;
  __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
  tick = (unsigned long long) hi << 32 | lo;
#elif defined(__sparc__)
  __asm__ __volatile__ ("rd %%tick, %0" : "=r" (tick));
#endif
  return tick;
}

static __inline__ void startTimer(unsigned long long* t) {
  *t = curtick();
}

static __inline__ void stopTimer(unsigned long long* t) {
  *t = curtick() - *t;
}

#ifdef __cplusplus
}
#endif

#endif // BITWEAVING_EXAMPLE_RDTSC_H_
