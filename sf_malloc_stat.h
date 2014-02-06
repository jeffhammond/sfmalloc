/*****************************************************************************/
/*                                                                           */
/* Copyright (c) 2011, Seoul National University.                            */
/* All rights reserved.                                                      */
/*                                                                           */
/* Redistribution and use in source and binary forms, with or without        */
/* modification, are permitted provided that the following conditions        */
/* are met:                                                                  */
/*   1. Redistributions of source code must retain the above copyright       */
/*      notice, this list of conditions and the following disclaimer.        */
/*   2. Redistributions in binary form must reproduce the above copyright    */
/*      notice, this list of conditions and the following disclaimer in the  */
/*      documentation and/or other materials provided with the distribution. */
/*   3. Neither the name of Seoul National University nor the names of its   */
/*      contributors may be used to endorse or promote products derived      */
/*      from this software without specific prior written permission.        */
/*                                                                           */
/* THIS SOFTWARE IS PROVIDED BY SEOUL NATIONAL UNIVERSITY "AS IS" AND ANY    */
/* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED */
/* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE    */
/* DISCLAIMED. IN NO EVENT SHALL SEOUL NATIONAL UNIVERSITY BE LIABLE FOR ANY */
/* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL        */
/* DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS   */
/* OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)     */
/* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,       */
/* STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN  */
/* ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE           */
/* POSSIBILITY OF SUCH DAMAGE.                                               */
/*                                                                           */
/* Contact information:                                                      */
/*   Center for Manycore Programming                                         */
/*   School of Computer Science and Engineering                              */
/*   Seoul National University, Seoul 151-744, Korea                         */
/*   http://aces.snu.ac.kr                                                   */
/*                                                                           */
/* Contributors:                                                             */
/*   Sangmin Seo, Junghyun Kim, and Jaejin Lee                               */
/*                                                                           */
/*****************************************************************************/

#ifndef __SF_MALLOC_STAT_H__
#define __SF_MALLOC_STAT_H__

#ifdef MALLOC_STATS
static double CPU_CLOCK = 1.0;

//-------------------------------------------------------------------
// Statistics
//-------------------------------------------------------------------
static __inline__ uint64_t get_timestamp() {                                  
  unsigned hi, lo;
  __asm__ __volatile__ (                                                      
      "rdtsc"
      : "=a" (lo), "=d" (hi)                                                  
      ); 
  return ((uint64_t)lo | ((uint64_t)hi << 32));                               
} 

typedef struct {
  uint64_t cnt_mmap;
  uint64_t cnt_munmap;
  uint64_t cnt_madvise;
  uint64_t size_mmap;
  uint64_t size_munmap;
  uint64_t size_madvise;
  uint64_t size_mmap_max;

  uint64_t cnt_malloc;
  uint64_t cnt_free;
  uint64_t cnt_realloc;
  uint64_t cnt_memalign;
  uint64_t time_malloc;
  uint64_t time_free;
  uint64_t time_realloc;
  uint64_t time_memalign;

  uint64_t pcache_malloc_hit;
  uint64_t pcache_malloc_real_hit;
  uint64_t pcache_malloc_miss;
  uint64_t pcache_malloc_evict;
  uint64_t pcache_free_hit;
  uint64_t pcache_free_miss;
  uint64_t pcache_free_evict;

  uint64_t pcolor_get;
  uint64_t pcolor_new;
  uint64_t pcolor_dup;
} thread_stat_t CACHE_LINE_ALIGN;

static __thread thread_stat_t l_stat TLS_MODEL;


#define inc_cnt_mmap()                l_stat.cnt_mmap++
#define inc_cnt_munmap()              l_stat.cnt_munmap++
#define inc_cnt_madvise()             l_stat.cnt_madvise++
#define inc_size_mmap(s)              l_stat.size_mmap += (s)
#define inc_size_munmap(s)            l_stat.size_munmap += (s)
#define inc_size_madvise(s)           l_stat.size_madvise += (s)
#define update_size_mmap_max()        \
  if ((l_stat.size_mmap - l_stat.size_munmap) > l_stat.size_mmap_max) \
    l_stat.size_mmap_max = l_stat.size_mmap - l_stat.size_munmap

#define get_cnt_mmap()                l_stat.cnt_mmap
#define get_cnt_munmap()              l_stat.cnt_munmap
#define get_cnt_madvise()             l_stat.cnt_madvise
#define get_size_mmap()               l_stat.size_mmap
#define get_size_munmap()             l_stat.size_munmap
#define get_size_madvise()            l_stat.size_madvise
#define get_size_mmap_max()           l_stat.size_mmap_max

#define inc_cnt_malloc()              l_stat.cnt_malloc++
#define inc_cnt_free()                l_stat.cnt_free++
#define inc_cnt_realloc()             l_stat.cnt_realloc++
#define inc_cnt_memalign()            l_stat.cnt_memalign++
#define inc_time_malloc(t)            l_stat.time_malloc += (t)
#define inc_time_free(t)              l_stat.time_free += (t)
#define inc_time_realloc(t)           l_stat.time_realloc += (t)
#define inc_time_memalign(t)          l_stat.time_memalign += (t)
#define inc_pcache_malloc_hit()       l_stat.pcache_malloc_hit++
#define inc_pcache_malloc_real_hit()  l_stat.pcache_malloc_real_hit++
#define inc_pcache_malloc_miss()      l_stat.pcache_malloc_miss++
#define inc_pcache_malloc_evict()     l_stat.pcache_malloc_evict++
#define inc_pcache_free_hit()         l_stat.pcache_free_hit++
#define inc_pcache_free_miss()        l_stat.pcache_free_miss++
#define inc_pcache_free_evict()       l_stat.pcache_free_evict++
#define inc_pcolor_get()              l_stat.pcolor_get++
#define inc_pcolor_new()              l_stat.pcolor_new++
#define inc_pcolor_dup()              l_stat.pcolor_dup++

#define get_cnt_malloc()              l_stat.cnt_malloc
#define get_cnt_free()                l_stat.cnt_free
#define get_cnt_realloc()             l_stat.cnt_realloc
#define get_cnt_memalign()            l_stat.cnt_memalign
#define get_time_malloc()             ((double)l_stat.time_malloc/CPU_CLOCK)
#define get_time_free()               ((double)l_stat.time_free/CPU_CLOCK)
#define get_time_realloc()            ((double)l_stat.time_realloc/CPU_CLOCK)
#define get_time_memalign()           ((double)l_stat.time_memalign/CPU_CLOCK)
#define get_pcache_malloc_hit()       l_stat.pcache_malloc_hit
#define get_pcache_malloc_real_hit()  l_stat.pcache_malloc_real_hit
#define get_pcache_malloc_miss()      l_stat.pcache_malloc_miss
#define get_pcache_malloc_evict()     l_stat.pcache_malloc_evict
#define get_pcache_free_hit()         l_stat.pcache_free_hit
#define get_pcache_free_miss()        l_stat.pcache_free_miss
#define get_pcache_free_evict()       l_stat.pcache_free_evict
#define get_pcolor_get()              l_stat.pcolor_get
#define get_pcolor_new()              l_stat.pcolor_new
#define get_pcolor_dup()              l_stat.pcolor_dup

#define malloc_timer_start()    uint64_t _start_time = get_timestamp()
#define malloc_timer_stop()     uint64_t _end_time = get_timestamp(); \
                                inc_time_malloc(_end_time - _start_time)
#define free_timer_start()      uint64_t _start_time = get_timestamp()
#define free_timer_stop()       uint64_t _end_time = get_timestamp(); \
                                inc_time_free(_end_time - _start_time)
#define realloc_timer_start()   uint64_t _start_time = get_timestamp()
#define realloc_timer_stop()    uint64_t _end_time = get_timestamp(); \
                                inc_time_realloc(_end_time - _start_time)
#define memalign_timer_start()  uint64_t _start_time = get_timestamp()
#define memalign_timer_stop()   uint64_t _end_time = get_timestamp(); \
                                inc_time_memalign(_end_time - _start_time)

#define getKB(s)    ((s) / 1024.0)
#define getMB(s)    ((s) / 1024.0 / 1024.0)
#define getGB(s)    ((s) / 1024.0 / 1024.0 / 1024.0)

#else //MALLOC_STATS

#define inc_cnt_mmap()
#define inc_cnt_munmap()
#define inc_cnt_madvise()
#define inc_size_mmap(s)
#define inc_size_munmap(s)
#define inc_size_madvise(s)
#define update_size_mmap_max()

#define get_cnt_mmap()
#define get_cnt_munmap()
#define get_cnt_madvise()
#define get_size_mmap()
#define get_size_munmap()
#define get_size_madvise()
#define get_size_mmap_max()

#define inc_cnt_malloc()
#define inc_cnt_free()
#define inc_cnt_realloc()
#define inc_cnt_memalign()
#define inc_time_malloc(t)
#define inc_time_free(t)
#define inc_time_realloc(t)
#define inc_time_memalign(t)
#define inc_pcache_malloc_hit()
#define inc_pcache_malloc_real_hit()
#define inc_pcache_malloc_miss()
#define inc_pcache_malloc_evict()
#define inc_pcache_free_hit()
#define inc_pcache_free_miss()
#define inc_pcache_free_evict()
#define inc_pcolor_get()
#define inc_pcolor_new()
#define inc_pcolor_dup()

#define get_cnt_malloc()
#define get_cnt_free()
#define get_cnt_realloc()
#define get_cnt_memalign()
#define get_time_malloc()
#define get_time_free()
#define get_time_realloc()
#define get_time_memalign()
#define get_pcache_malloc_hit()
#define get_pcache_malloc_real_hit()
#define get_pcache_malloc_miss()
#define get_pcache_malloc_evict()
#define get_pcache_free_hit()
#define get_pcache_free_miss()
#define get_pcache_free_evict()
#define get_pcolor_get()
#define get_pcolor_new()
#define get_pcolor_dup()

#define malloc_timer_start()
#define malloc_timer_stop()
#define free_timer_start()
#define free_timer_stop()
#define realloc_timer_start()
#define realloc_timer_stop()
#define memalign_timer_start()
#define memalign_timer_stop()

#endif //MALLOC_STATS


#endif //__SF_MALLOC_STAT_H__

