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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <time.h>
#include <sched.h>
#include <errno.h>
#include <pthread.h>

#include "sf_malloc_ctrl.h"
#include "sf_malloc_def.h"
#include "sf_malloc_stat.h"
#include "sf_malloc_atomic.h"

#include <assert.h>


////////////////////////////////////////////////////////////////////////////
// Internal Data
////////////////////////////////////////////////////////////////////////////
static volatile uint32_t g_initialized = 0;
static volatile uint32_t g_id = 1;
static volatile uint32_t g_thread_num = 0;
static pthread_key_t     g_thread_key;


////////////////////////////////////////////////////////////////////////////
// Global Data Structures
////////////////////////////////////////////////////////////////////////////
static sizemap_t         g_sizemap;
static pagemap_t         g_pagemap;

// Hazard Pointer List
static hazard_ptr_t*     g_hazard_ptr_list = NULL;
static volatile uint32_t g_hazard_ptr_free_num = 0;

// Free Superpage List
static sph_t*            g_free_sp_list = NULL;
static volatile uint32_t g_free_sp_len = 0;
#define FREE_SP_LIST_THRESHOLD    (g_thread_num * 2)


////////////////////////////////////////////////////////////////////////////
// Thread-Local Data Structures
////////////////////////////////////////////////////////////////////////////
#ifdef MALLOC_USE_PAGEMAP_CACHE
// Page Map Cache
static __thread size_t          l_pagemap_tag  TLS_MODEL = 0xFFFFFFFFFFFFFFFF;
static __thread pagemap_leaf_t* l_pagemap_leaf TLS_MODEL = NULL;
#endif
// Thread Local Heap (TLH)
static __thread tlh_t l_tlh TLS_MODEL;


////////////////////////////////////////////////////////////////////////////
// Internal Functions
////////////////////////////////////////////////////////////////////////////
/* Initialization */
void sf_malloc_init();
void sf_malloc_thread_init();
void sf_malloc_exit();
void sf_malloc_thread_exit();
void sf_malloc_destructor(void* val);

/* mmap/munmap */
static inline void* do_mmap(size_t size);
static inline void  do_munmap(void* addr, size_t size);
static inline void  do_madvise(void* addr, size_t size);

/* SizeMap */
static void sizemap_init();
static inline uint32_t get_logfloor(uint32_t n);
static inline uint32_t get_classindex(uint32_t s);
static inline uint32_t get_sizeclass(uint32_t size);
static inline uint32_t get_size_for_class(uint32_t cl);
static inline uint32_t get_pages_for_class(uint32_t cl);
static inline uint32_t get_blocks_for_class(uint32_t cl);
static inline uint32_t get_alignment(uint32_t size);

/* PageMap */
static void pagemap_init();
static inline void  pagemap_expand(size_t page_id, size_t n);
static inline void* pagemap_get(size_t page_id);
static inline void* pagemap_get_checked(size_t page_id);
static inline void  pagemap_set(size_t page_id, void* val);
static inline void  pagemap_set_range(size_t start, size_t len, void* val);

/* Superpage and Superpage Header (SPH) */
static sph_t* sph_alloc(tlh_t* tlh);
static void   sph_free(tlh_t* tlh, sph_t* sph);
static void   sph_get_remote_pbs(sph_t* sph);
static void   sph_coalesce_pbs(pbh_t* pbh);
static bool   take_superpage(tlh_t* tlh, sph_t* sph);
static void   finish_superpages(tlh_t* tlh);
static bool   try_to_free_superpage(sph_t* sph);
static inline void   sph_link_init(sph_t* sph);
static inline void   sph_list_prepend(sph_t** list, sph_t* sph);
static inline sph_t* sph_list_pop(sph_t** list);
static inline void   sph_list_remove(sph_t** list, sph_t* sph);

/* Hazard Pointer */
static hazard_ptr_t* hazard_ptr_alloc();
static void          hazard_ptr_free(hazard_ptr_t* hptr);
static bool scan_hazard_pointers(sph_t* sph);

/* Page Block Header (PBH) */
static inline pbh_t* pbh_alloc(sph_t* sph, size_t page_id, size_t len);
static inline void   pbh_free(pbh_t* pbh);
static inline void   pbh_add_blocks(tlh_t* tlh, pbh_t* pbh,
                                    void* start_blk, void* end_blk,
                                    uint32_t N);
static void          pbh_add_unused(tlh_t* tlh, pbh_t* pbh, 
                                    void* unused, uint32_t N);
static inline sph_t* pbh_get_superpage(pbh_t* pbh);
static inline void   pbh_link_init(pbh_t* pbh);
static inline void   pbh_field_init(pbh_t* pbh);
static inline void   pbh_list_prepend(pbh_t** list, pbh_t* pbh);
static inline pbh_t* pbh_list_pop(pbh_t** list);
static inline void   pbh_list_remove(pbh_t** list, pbh_t* pbh);
static inline void   pbh_list_move_to_first(pbh_t** list, pbh_t* pbh);

/* Page Block (PB) */
static pbh_t* pb_alloc(tlh_t* tlh, size_t page_len);
static pbh_t* pb_alloc_from_tlh(tlh_t* tlh, size_t page_len);
static void   pb_free(tlh_t* tlh, pbh_t* pbh);
static void   pb_remote_free(tlh_t* tlh, void* pb, pbh_t* pbh);
static inline void   pb_split(tlh_t* tlh, pbh_t* pbh, size_t len);
static inline pbh_t* pb_coalesce(tlh_t* tlh, pbh_t* pbh);

/* Thread Local Heap (TLH) */
static void tlh_init();
static void tlh_clear(tlh_t* tlh);
static void tlh_return_list(tlh_t* tlh, uint32_t cl);
static void tlh_return_unused(tlh_t* tlh, uint32_t cl);
static void tlh_return_pbhs(tlh_t* tlh, uint32_t cl);

/* Page Block Cache */
static inline int  get_cache_hit_index(v8qi val);
static inline void pb_cache_return(tlh_t* tlh, void* page);

/* Allocation/Deallocation */
static inline void* bump_alloc(size_t size, blk_list_t* b_list);
static inline void* small_malloc(uint32_t cl);
static inline void* large_malloc(size_t page_len);
static inline void* huge_malloc(size_t page_len);
static inline bool  remote_free(tlh_t* tlh, pbh_t* pbh,
                                void* first, void* last, uint32_t N);
static inline void  small_free(void* ptr, pbh_t* pbh);
static inline void  large_free(void* ptr, pbh_t* pbh);
static inline void  huge_free(void* ptr, size_t size);

/* Statistics */
void malloc_stats();
#ifdef MALLOC_STATS
void stats_init();
void print_stats();
#else
#define stats_init();
#define print_stats()
#endif

/* Debugging */
#ifdef MALLOC_DEBUG
static void debug_init();
#ifdef MALLOC_DEBUG_DETAIL
static void print_sizemap();
#else
#define print_sizemap()
#endif
void print_pbh(pbh_t* pbh);
void print_pbh_list(pbh_t* list);
void print_superpage(sph_t* spage);
void print_superpage_list(sph_t* list);
void print_free_pb_list(tlh_t* tlh);
void print_tlh(tlh_t* tlh);
void print_block_list(void* block);
#else
#define debug_init()
#endif //MALLOC_DEBUG



////////////////////////////////////////////////////////////////////////////
// Initialization Functions
////////////////////////////////////////////////////////////////////////////
/* Initialize data structures for sf_malloc library. */
void sf_malloc_init() {
  // Only the main thread can initialize the sf_malloc library.
  if (g_initialized) return;
  g_initialized = 1;

#ifdef MALLOC_DEBUG
  if (PBH_SIZE != CACHE_LINE_SIZE) {
    CRASH("PBH size (%lu) != cache line size (%u)\n",
          PBH_SIZE, CACHE_LINE_SIZE);
  }
#endif

  // Initialize thread local heap.
  tlh_init();

  // Initialize data structures.
  debug_init();
  sizemap_init();
  pagemap_init();
  stats_init();

  // Create a thread key to call the destructor.
  if (pthread_key_create(&g_thread_key, sf_malloc_destructor)) {
    HANDLE_ERROR("pthread_key_create");
  }

#ifdef MALLOC_USE_STATIC_LINKING
  // Register the exit function.
  if (atexit(sf_malloc_exit)) {
    HANDLE_ERROR("atexit");
  }
#endif

  LOG_D("[T%u] sf_malloc_init(): TLH=%p\n", TID(), &l_tlh);
}


/* Initialize thread-private data structures. */
void sf_malloc_thread_init() {
  // Initialize thread local heap.
  tlh_init();

  if (pthread_setspecific(g_thread_key, (void*)&l_tlh)) {
    HANDLE_ERROR("pthread_setspecific");
  }

  LOG_D("[T%u] INIT: TLH=%p\n", TID(), &l_tlh);
}


/* Finalize the sf_malloc library. */
void sf_malloc_exit() {
  if (g_initialized == 0) return;
  g_initialized = 0;

  LOG_D("[T%u] sf_malloc_exit()\n", TID());
  print_stats();
  malloc_stats();
}


/* Finalize thread-private data structures. */
void sf_malloc_thread_exit() {
  tlh_t* tlh = &l_tlh;
  if (tlh->thread_id == DEAD_OWNER) return;

  // Clear thread-local heap.
  tlh_clear(&l_tlh);

  LOG_D("[T%u] EXIT\n", TID());
  print_stats();

  // Reset thread ID
  tlh->thread_id = DEAD_OWNER;

  // Decrease the number of currently running threads.
  atomic_dec_int((volatile int*)&g_thread_num);
}


void sf_malloc_destructor(void* val) {
  sf_malloc_thread_exit();
  pthread_setspecific(g_thread_key, NULL);
}



////////////////////////////////////////////////////////////////////////////
// MMAP/MUNMAP Functions
////////////////////////////////////////////////////////////////////////////
#define MMAP_PROT   (PROT_READ | PROT_WRITE)
#define MMAP_FLAGS  (MAP_PRIVATE | MAP_ANONYMOUS)
static inline void* do_mmap(size_t size) {
  void* mem = mmap(0, size, MMAP_PROT, MMAP_FLAGS, -1, 0);
  if (mem == MAP_FAILED) {
    perror("do_mmap");
    CRASH("size=%lu\n", size);
  }

  inc_cnt_mmap();
  inc_size_mmap(size);
  update_size_mmap_max();

  return mem;
}


static inline void do_munmap(void* addr, size_t size) {
  if (munmap(addr, size) == -1) {
    perror("do_munmap");
    CRASH("addr=%p size=%lu\n", addr, size);
  }

  inc_cnt_munmap();
  inc_size_munmap(size);
}


static inline void do_madvise(void* addr, size_t size) {
  if (madvise(addr, size, MADV_DONTNEED) == -1) {
    perror("do_madvise");
    CRASH("addr=%p size=%lu\n", addr, size);
  }

  inc_cnt_madvise();
  inc_size_madvise(size);
}



////////////////////////////////////////////////////////////////////////////
// SizeMap Functions
////////////////////////////////////////////////////////////////////////////
/* Initialize the mapping arrays */
static void sizemap_init() {
  const uint8_t class_array[CLASS_ARRAY_SIZE] = {
    0,  0,  1,  2,  2,  3,  3,  4,  4,  5, 
    5,  6,  6,  7,  7,  8,  8,  9,  9, 10, 
    10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 
    15, 16, 16, 17, 17, 17, 17, 18, 18, 18, 
    18, 19, 19, 19, 19, 20, 20, 20, 20, 21, 
    21, 21, 21, 21, 21, 21, 21, 22, 22, 22, 
    22, 22, 22, 22, 22, 23, 23, 23, 23, 23, 
    23, 23, 23, 24, 24, 24, 24, 24, 24, 24, 
    24, 25, 25, 25, 25, 25, 25, 25, 25, 26, 
    26, 26, 26, 26, 26, 26, 26, 27, 27, 27, 
    27, 27, 27, 27, 27, 28, 28, 28, 28, 28, 
    28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 
    28, 28, 28, 28, 28, 28, 28, 28, 28, 29, 
    30, 31, 32, 33, 34, 34, 34, 35, 35, 36, 
    36, 37, 37, 37, 37, 38, 38, 39, 39, 39, 
    39, 39, 39, 40, 40, 41, 41, 42, 42, 42, 
    42, 43, 43, 43, 43, 43, 43, 43, 43, 44, 
    44, 44, 44, 45, 45, 46, 46, 46, 46, 46, 
    46, 46, 46, 46, 46, 47, 47, 47, 47, 48, 
    48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 
    48, 49, 49, 50, 50, 50, 50, 50, 50, 50, 
    50, 50, 50, 50, 50, 50, 50, 51, 51, 51, 
    51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 
    51, 52, 52, 52, 52, 52, 52, 52, 52, 52, 
    52, 52, 52, 52, 52, 52, 52, 52, 52, 53, 
    53, 53, 53, 53, 53, 53, 53, 53, 53, 54, 
    54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 
    54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 
    54, 55, 55, 55, 55, 55, 55, 56, 56, 56, 
    56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 
    56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 
    56, 56, 56, 57, 57, 58, 58, 58, 58, 58, 
    58, 58, 58, 58, 58, 58, 58, 58, 58, 58, 
    58, 58, 58, 58, 58, 58, 58, 58, 58, 58, 
    58, 58, 58, 58, 58, 59, 59, 59, 59, 59, 
    59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 
    59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 
    59, 59, 59, 59, 59, 59, 59
  };

  const uint32_t class_to_size[NUM_CLASSES] = {
        8,    16,    32,    48,    64,    80,    96,   112,   128,   144, 
      160,   176,   192,   208,   224,   240,   256,   288,   320,   352, 
      384,   448,   512,   576,   640,   704,   768,   832,  1024,  1152, 
     1280,  1408,  1536,  1664,  2048,  2304,  2560,  3072,  3328,  4096, 
     4352,  4608,  5120,  6144,  6656,  6912,  8192,  8704, 10240, 10496, 
    12288, 14080, 16384, 17664, 20480, 21248, 24576, 24832, 28672, 32768
  };

  const uint16_t class_to_pages[NUM_CLASSES] = {
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1, 
    1,  1,  1,  1,  1,  2,  1,  2,  1,  2, 
    1,  3,  2,  3,  1,  3,  2,  3,  5,  1, 
    6,  5,  4,  3,  5,  7,  2,  7,  5,  8, 
    3,  7,  4,  9,  5, 11,  6, 13,  7,  8
  };

  const uint16_t num_blocks_per_pbh[NUM_CLASSES] = {
    512, 256, 128,  85,  64,  51,  42,  36,  32,  28, 
     25,  23,  21,  19,  18,  17,  16,  14,  12,  11, 
     10,   9,   8,   7,   6,  11,   5,   9,   4,   7, 
      3,   8,   5,   7,   2,   5,   3,   4,   6,   1, 
      5,   4,   3,   2,   3,   4,   1,   3,   2,   3, 
      1,   2,   1,   2,   1,   2,   1,   2,   1,   1
  };

  memcpy(g_sizemap.class_array, class_array,
         sizeof(uint8_t) * CLASS_ARRAY_SIZE);
  for (int i = 0; i < NUM_CLASSES; ++i) {
    g_sizemap.info[i].class_to_size = class_to_size[i];
    g_sizemap.info[i].class_to_pages = class_to_pages[i];
    g_sizemap.info[i].num_blocks_per_pbh = num_blocks_per_pbh[i];
  }

#ifdef MALLOC_DEBUG_DETAIL
  print_sizemap();
#endif
}

static inline uint32_t get_logfloor(uint32_t n) {
  uint32_t log = 0;
  for (int32_t i = 4; i >= 0; --i) {
    uint32_t shift = 1 << i;
    uint32_t x = n >> shift;
    if (x != 0) {
      n = x;
      log += shift;
    }
  }
  assert(n == 1);
  return log;
}

/* Compute index of the class_array[] entry for a given size */
static inline uint32_t get_classindex(uint32_t s) {
  const bool big = (s > MAX_SMALL_SIZE);
  const uint32_t add_amount   = big ? (127 + (120 << 7)) : 7;
  const uint32_t shift_amount = big ? 7 : 3;
  return (s + add_amount) >> shift_amount;
}

static inline uint32_t get_sizeclass(uint32_t size) {
  return g_sizemap.class_array[get_classindex(size)];
}

/* Get the byte-size for a specified class */
static inline uint32_t get_size_for_class(uint32_t cl) {
  return g_sizemap.info[cl].class_to_size;
}

static inline uint32_t get_pages_for_class(uint32_t cl) {
  return g_sizemap.info[cl].class_to_pages;
}

static inline uint32_t get_blocks_for_class(uint32_t cl) {
  return g_sizemap.info[cl].num_blocks_per_pbh;
}

static uint32_t get_alignment(uint32_t size) {
  int alignment = ALIGNMENT;
  if (size > MAX_SIZE) {
    alignment = PAGE_SIZE;
  } else if (size >= 2048) {
    // Cap alignment at 256 for large sizes.
    alignment = 256;
  } else if (size >= 128) {
    // Space wasted due to alignment is at most 1/8, i.e., 12.5%.
    alignment = (1 << get_logfloor(size)) / 8;
  } else if (size >= 16) {
    // We need an alignment of at least 16 bytes to satisfy
    // requirements for some SSE types.
    alignment = 16;
  }

  assert(size < 16 || alignment >= 16);
  assert((alignment & (alignment - 1)) == 0);

  return alignment;
}


////////////////////////////////////////////////////////////////////////////
// PageMap Functions
////////////////////////////////////////////////////////////////////////////
static void pagemap_init() {
}


static void pagemap_expand(size_t page_id, size_t n) {
  for (size_t key = page_id; key < page_id + n; ) {
    const size_t i1 = key >> (PMAP_LEAF_BIT + PMAP_INTERIOR_BIT);
    const size_t i2 = (key >> PMAP_LEAF_BIT) & (PMAP_INTERIOR_LEN - 1);
    assert(i1 < PMAP_INTERIOR_LEN && i2 < PMAP_INTERIOR_LEN);

    // Make 2nd level node if necessary
    if (g_pagemap.node[i1] == NULL) {
      size_t node_size = sizeof(pagemap_node_t);
      pagemap_node_t* new_node = (pagemap_node_t*)do_mmap(node_size);
      if (!CAS_ptr(&g_pagemap.node[i1], NULL, new_node)) {
        do_munmap(new_node, node_size);
      }
    }

    // Make leaf node if necessary
    pagemap_node_t* interior = g_pagemap.node[i1];
    if (interior->leaf[i2] == NULL) {
      size_t leaf_size = sizeof(pagemap_leaf_t);
      pagemap_leaf_t* new_leaf = (pagemap_leaf_t*)do_mmap(leaf_size);
      if (!CAS_ptr(&interior->leaf[i2], NULL, new_leaf)) {
        do_munmap(new_leaf, leaf_size);
      }
    }

    // Advance key past whatever is covered by this leaf node
    key = ((key >> PMAP_LEAF_BIT) + 1) << PMAP_LEAF_BIT;
  }
}


static inline void* pagemap_get(size_t page_id) {
#ifdef MALLOC_USE_PAGEMAP_CACHE
  const size_t tag = page_id >> PMAP_LEAF_BIT;
  const size_t i3  = page_id & (PMAP_LEAF_LEN - 1);
  if (UNLIKELY(l_pagemap_tag != tag)) {
    // new caching
    const size_t i1 = tag >> PMAP_INTERIOR_BIT;
    const size_t i2 = tag & (PMAP_INTERIOR_LEN - 1);
    l_pagemap_tag  = tag;
    l_pagemap_leaf = g_pagemap.node[i1]->leaf[i2];
  }
  return l_pagemap_leaf->val[i3];
#else
  const size_t i1 = page_id >> (PMAP_LEAF_BIT + PMAP_INTERIOR_BIT);
  const size_t i2 = (page_id >> PMAP_LEAF_BIT) & (PMAP_INTERIOR_LEN - 1);
  const size_t i3 = page_id & (PMAP_LEAF_LEN - 1);
  assert((page_id >> PMAP_BITS) == 0);
  assert(g_pagemap.node[i1] != NULL);
  assert(g_pagemap.node[i1]->leaf[i2] != NULL);
  return g_pagemap.node[i1]->leaf[i2]->val[i3];
#endif
}


static inline void* pagemap_get_checked(size_t page_id) {
  const size_t i1 = page_id >> (PMAP_LEAF_BIT + PMAP_INTERIOR_BIT);
  const size_t i2 = (page_id >> PMAP_LEAF_BIT) & (PMAP_INTERIOR_LEN - 1);
  const size_t i3 = page_id & (PMAP_LEAF_LEN - 1);

  if (((page_id >> PMAP_BITS) > 0) ||
      (g_pagemap.node[i1] == NULL) ||
      (g_pagemap.node[i1]->leaf[i2] == NULL)) return NULL;

  return g_pagemap.node[i1]->leaf[i2]->val[i3];
}


static inline void pagemap_set(size_t page_id, void* val) {
#ifdef MALLOC_USE_PAGEMAP_CACHE
  const size_t tag = page_id >> PMAP_LEAF_BIT;
  const size_t i3  = page_id & (PMAP_LEAF_LEN - 1);
  if (UNLIKELY(l_pagemap_tag != tag)) {
    const size_t i1 = tag >> PMAP_INTERIOR_BIT;
    const size_t i2 = tag & (PMAP_INTERIOR_LEN - 1);
    l_pagemap_tag  = tag;
    l_pagemap_leaf = g_pagemap.node[i1]->leaf[i2];
  }
  l_pagemap_leaf->val[i3] = val;
#else
  assert(page_id >> PMAP_BITS == 0);
  const size_t i1 = page_id >> (PMAP_LEAF_BIT + PMAP_INTERIOR_BIT);
  const size_t i2 = (page_id >> PMAP_LEAF_BIT) & (PMAP_INTERIOR_LEN - 1);
  const size_t i3 = page_id & (PMAP_LEAF_LEN - 1);
  g_pagemap.node[i1]->leaf[i2]->val[i3] = val;
#endif
}


static inline void pagemap_set_range(size_t start, size_t len, void* val) {
  for (size_t page_id = start; page_id < (start + len); page_id++) {
    pagemap_set(page_id, val);
  }
}


////////////////////////////////////////////////////////////////////////////
// Superpage Header Functions
////////////////////////////////////////////////////////////////////////////
static sph_t* sph_alloc(tlh_t* tlh) {
  sph_t* sph = g_free_sp_list;
  if (sph != NULL) {
    // Pop the whole list.
    if (CAS_ptr(&g_free_sp_list, sph, NULL)) {
      // Get the first one and push the remained list.
      sph_t* next_sph = sph->next;
      if (next_sph != NULL) {
        if (!CAS_ptr(&g_free_sp_list, NULL, next_sph)) {
          // FIXME: Find the last superpage.
          sph_t* last_sph = next_sph;
          while (last_sph->next != NULL) {
            last_sph = last_sph->next;
          }

          // Push to the global free superpage list.
          sph_t* cur_sph;
          do {
            cur_sph = g_free_sp_list;
            last_sph->next = cur_sph;
          } while (!CAS_ptr(&g_free_sp_list, cur_sph, next_sph));
        }
      }

      // Decrease the length of global free superpage list.
      assert(g_free_sp_len > 0);
      atomic_dec_int((volatile int*)&g_free_sp_len);
    } else {
      // CAS failed.
      sph = NULL;
    }
  }

  if (sph == NULL) {
    void* mem = do_mmap(SUPERPAGE_SIZE + SPH_SIZE);
    sph = (sph_t*)mem;
    sph->start_page = (size_t)(mem + SPH_SIZE) >> PAGE_SHIFT;

    // Expand pagemap.
    pagemap_expand(sph->start_page, SUPERPAGE_LEN);
  }

  // Set the owner of superpage.
  sph->omark.owner_id = tlh->thread_id;

  // Prepend the new superpage to the superpage list.
  sph_list_prepend(&tlh->sp_list, sph);

  return sph;
}


static void sph_free(tlh_t* tlh, sph_t* sph) {
  // Remove the superpage form the Superpage List.
  sph_list_remove(&tlh->sp_list, sph);

  // Update pagemap.
  pagemap_set_range(sph->start_page, SUPERPAGE_LEN, NULL);

  // Check the hazard_mark.
  bool hazardous = false;
  if (sph->hazard_mark) {
    if (scan_hazard_pointers(sph)) {
      hazardous = true;
    } else {
      sph->hazard_mark = false;
    }
  }

  if (hazardous || g_free_sp_len < FREE_SP_LIST_THRESHOLD) {
    atomic_inc_uint(&g_free_sp_len);

    // Push to the global Free Superpage List.
    sph_t* cur_sph;
    do {
      cur_sph = g_free_sp_list;
      sph->next = cur_sph;
    } while (!CAS_ptr(&g_free_sp_list, cur_sph, sph));
  } else {
    // Return the memory to the OS.
    do_munmap(sph, SUPERPAGE_SIZE + SPH_SIZE);
  }
}


static void sph_get_remote_pbs(sph_t* sph) {
  void* remote_pb;
  do {
    remote_pb = sph->remote_pb_list;
  } while (!CAS_ptr(&sph->remote_pb_list, remote_pb, NULL));

  do {
    size_t page_id = (size_t)remote_pb >> PAGE_SHIFT;
    pbh_t* pbh = (pbh_t*)pagemap_get(page_id);
    pbh->status = PBH_ON_FREE_LIST;
    assert(pbh->sizeclass == NUM_CLASSES);
    sph_coalesce_pbs(pbh);

    remote_pb = GET_NEXT(remote_pb);
  } while (remote_pb);
}


static void sph_coalesce_pbs(pbh_t* pbh) {
  pbh_t* prev_pbh = (pbh_t*)pagemap_get_checked(pbh->start_page - 1);
  assert(((uintptr_t)prev_pbh & HUGE_MALLOC_MARK) == 0);

  void*  next_val = pagemap_get_checked(pbh->start_page + pbh->length);
  pbh_t* next_pbh = ((uintptr_t)next_val & HUGE_MALLOC_MARK)
                    ? NULL : (pbh_t*)next_val;

  if (prev_pbh && (prev_pbh->status == PBH_ON_FREE_LIST)) {
    prev_pbh->length += pbh->length;

    // If the coalesced length is the same as the length of superpage,
    // we don't need to update more because superpage will be freed.
    if (prev_pbh->length == SUPERPAGE_LEN) {
      // Only prev_pbh is free.
      return;
    } else if (next_pbh && (next_pbh->status == PBH_ON_FREE_LIST)) {
      // Both prev_pbh and next_pbh are free. Coalesce together.
      uint32_t next_len = next_pbh->length;

      prev_pbh->length += next_len;
      if (prev_pbh->length == SUPERPAGE_LEN) return;

      // Update the pagemap and deallocate next_pbh.
      pagemap_set_range(next_pbh->start_page, next_len, prev_pbh);
      pbh_free(next_pbh);
    }

    // Update the pagemap and deallocate pbh.
    pagemap_set_range(pbh->start_page, pbh->length, prev_pbh);
    pbh_free(pbh);
  } else if (next_pbh && (next_pbh->status == PBH_ON_FREE_LIST)) {
    // Only next_pbh is free.
    uint32_t next_len = next_pbh->length;

    pbh->length += next_len;
    if (pbh->length == SUPERPAGE_LEN) return;

    // Update the pagemap and deallocate next_pbh.
    pagemap_set_range(next_pbh->start_page, next_len, pbh);
    pbh_free(next_pbh);
  }
}


static bool take_superpage(tlh_t* tlh, sph_t* sph) {
  // Try to change the ownership of superpage.
  if (!CAS32(&sph->omark.owner_id, DEAD_OWNER, tlh->thread_id)) 
    return false;

  if (sph->remote_pb_list != NULL) {
    sph_get_remote_pbs(sph);
  }

  // Adopt all pbhs in the superpage.
  pbh_t* pbh = GET_FIRST_PBH(sph);
  uint32_t total_len = 0;
  while (total_len < SUPERPAGE_LEN) {
    uint32_t len = pbh->length;
    assert(len > 0);
    assert(pbh->index == (total_len + 1));

    if (pbh->status == PBH_ON_FREE_LIST) {
      pbh_list_prepend(&tlh->free_pb_list[len-1], pbh);
    } else if (pbh->sizeclass < NUM_CLASSES) {
      uint32_t count = pbh->cnt_free + pbh->cnt_unused + pbh->remote_list.cnt;
      if (count == get_blocks_for_class(pbh->sizeclass)) {
        // PBH became totally free.
        pbh_field_init(pbh);
        pbh_list_prepend(&tlh->free_pb_list[len-1], pbh);
      } else {
        blk_list_t* b_list = &tlh->blk_list[pbh->sizeclass];
        pbh_list_prepend(&b_list->pbh_list, pbh);
      }
    }

    // next pbh
    total_len += len;
    pbh = pbh + len;
  }
  assert(total_len == SUPERPAGE_LEN);

  // Prepend the adopted superpage to the superpage list.
  sph_list_prepend(&tlh->sp_list, sph);

  return true;
}


static void finish_superpages(tlh_t* tlh) {
  sph_t** sp_list = &tlh->sp_list;

  ownermark_t live_mark, dead_mark;
  live_mark.owner_id    = (*sp_list)->omark.owner_id;
  live_mark.finish_mark = NONE;
  dead_mark.owner_id    = DEAD_OWNER;
  dead_mark.finish_mark = NONE;

  do {
    sph_t* sph = sph_list_pop(sp_list);
    assert(sph->omark.owner_id == live_mark.owner_id);

    while (true) {
      sph->omark.finish_mark = NONE;

      // Try to clean up the superpage.
      if (try_to_free_superpage(sph)) {
        break;
      }

      // If the superpage was not freed, make it dead.
      if (CAS64((uint64_t*)&sph->omark, live_mark.with, dead_mark.with)) {
        LOG_D("[T%u] DEAD SUPERPAGE\n", tlh->thread_id);
        break;
      }
    } //end while
  } while (*sp_list != NULL);
}


static bool try_to_free_superpage(sph_t* sph) {
  if (sph->remote_pb_list != NULL) {
    sph_get_remote_pbs(sph);
  }

  pbh_t* pbh = GET_FIRST_PBH(sph);
  pbh_t* prev_pbh = NULL;

  uint32_t cnt_inuse = 0;
  uint32_t total_len = 0;
  while (total_len < SUPERPAGE_LEN) {
    uint32_t len = pbh->length;
    assert(len > 0);

    total_len += len;

    if (pbh->status != PBH_ON_FREE_LIST && pbh->sizeclass < NUM_CLASSES) {
      uint32_t count = pbh->cnt_free + pbh->cnt_unused + pbh->remote_list.cnt;
      if (count == get_blocks_for_class(pbh->sizeclass)) {
        // PBH became totally free.
        pbh_field_init(pbh);

        // Try coalescing.
        pbh_t* next_pbh = (total_len < SUPERPAGE_LEN) ? (pbh + len) : NULL;
        if (prev_pbh && prev_pbh->status == PBH_ON_FREE_LIST) {
          pagemap_set_range(pbh->start_page, len, prev_pbh);
          pbh_free(pbh);

          prev_pbh->length += len;
          if (next_pbh && next_pbh->status == PBH_ON_FREE_LIST) {
            uint32_t next_len = next_pbh->length;
            prev_pbh->length += next_len;
            pagemap_set_range(next_pbh->start_page, next_len, prev_pbh);
            pbh_free(next_pbh);

            total_len += next_len;
            pbh = next_pbh + next_len;
          } else {
            pbh = pbh + len;
          }
          
          continue;
        } else if (next_pbh && next_pbh->status == PBH_ON_FREE_LIST) {
          uint32_t next_len = next_pbh->length;
          pbh->length += next_len;
          pagemap_set_range(next_pbh->start_page, next_len, pbh);
          pbh_free(next_pbh);

          total_len += next_len;
          prev_pbh = pbh;
          pbh = next_pbh + next_len;

          continue;
        }
      } else {
        cnt_inuse++;
      }
    }

    // next pbh
    prev_pbh = pbh;
    pbh = pbh + len;
  }
  assert(total_len == SUPERPAGE_LEN);

  if (cnt_inuse == 0) {
    // Superpage became totall free.
    LOG_D("[T%u] EMPTY: %p\n", TID(), sph);
    sph->hazard_mark = true;

    // Link the superpage to g_free_sp_list
    atomic_inc_uint(&g_free_sp_len);
    sph_t* global_list;
    do {
      global_list = g_free_sp_list;
      sph->next = global_list;
    } while (!CAS_ptr(&g_free_sp_list, global_list, sph));

    // Update pagemap.
    pagemap_set_range(sph->start_page, SUPERPAGE_LEN, NULL);

    return true;
  }

  return false;
}


static inline void sph_link_init(sph_t* sph) {
  sph->next = sph;
  sph->prev = sph;
}


static inline void sph_list_prepend(sph_t** list, sph_t* sph) {
  if (*list != NULL) {
    sph_t* top = *list;
    sph->next = top;
    sph->prev = top->prev;
    top->prev->next = sph;
    top->prev = sph;
  } else {
    sph_link_init(sph);
  }
  *list = sph;
}


static inline sph_t* sph_list_pop(sph_t** list) {
  assert(*list != NULL);

  sph_t* sph = *list;
  *list = (sph != sph->next) ? sph->next : NULL;

  // Remove the superpage from the list.
  sph->prev->next = sph->next;
  sph->next->prev = sph->prev;
  sph_link_init(sph);

  return sph;
}


static inline void sph_list_remove(sph_t** list, sph_t* sph) {
  if (sph == sph->next) {
    assert(*list == sph);
    *list = NULL;
  } else {
    if (*list == sph) *list = sph->next;
    sph->prev->next = sph->next;
    sph->next->prev = sph->prev;
    sph_link_init(sph);
  }
}



////////////////////////////////////////////////////////////////////////////
// Hazard Pointer List Functions
////////////////////////////////////////////////////////////////////////////
static hazard_ptr_t* hazard_ptr_alloc() {
  // Allocate from the current list.
  if (g_hazard_ptr_free_num > 0) {
    for (hazard_ptr_t* hp = g_hazard_ptr_list; hp != NULL; hp = hp->next) {
      if (hp->active) continue;
      if (atomic_xchg_uint(&hp->active, 1)) continue;
      atomic_dec_int((int32_t*)&g_hazard_ptr_free_num);
      return hp;
    }
  }

  // Allocate a new page and split it.
  hazard_ptr_t* first_hptr = (hazard_ptr_t*)do_mmap(PAGE_SIZE);
  first_hptr->active = 1;

  uint32_t rem_len = (PAGE_SIZE / sizeof(hazard_ptr_t)) - 1;

  hazard_ptr_t* last_hptr = first_hptr;
  for (uint32_t i = 0; i < rem_len; i++) {
    hazard_ptr_t* next_hptr = last_hptr + 1;
    last_hptr->next = next_hptr;
    last_hptr = next_hptr;
  }

  hazard_ptr_t* top;
  do {
    top = g_hazard_ptr_list;
    last_hptr->next = top;
  } while (!CAS_ptr(&g_hazard_ptr_list, top, first_hptr));
  atomic_add_uint(&g_hazard_ptr_free_num, rem_len);

  return first_hptr;
}


static void hazard_ptr_free(hazard_ptr_t* hptr) {
  hptr->active = 0;
  atomic_inc_uint(&g_hazard_ptr_free_num);
}


static bool scan_hazard_pointers(sph_t* sph) {
  for (hazard_ptr_t* hp = g_hazard_ptr_list; hp != NULL; hp = hp->next) {
    if (hp->node == sph) return true;
  }
  return false;
}



////////////////////////////////////////////////////////////////////////////
// PBH Functions
////////////////////////////////////////////////////////////////////////////
/* Allocate a new pbh from the superpage. */
static inline pbh_t* pbh_alloc(sph_t* sph, size_t page_id, size_t len) {
  uint32_t pbh_idx = page_id - sph->start_page + 1;
  assert(pbh_idx > 0 && pbh_idx <= SUPERPAGE_LEN);

  pbh_t* new_pbh = (pbh_t*)sph + pbh_idx;
  memset((void*)new_pbh, 0, sizeof(pbh_t));
  new_pbh->start_page = page_id;
  new_pbh->length     = len;
  new_pbh->index      = pbh_idx;

  return new_pbh;
}


/* Deallocate the pbh. */
static inline void pbh_free(pbh_t* pbh) {
  // Do nothing...
}


static inline void pbh_add_blocks(tlh_t* tlh, pbh_t* pbh,
                                 void* start_blk, void* end_blk, uint32_t N) {
  sph_t* sph = pbh_get_superpage(pbh);
  if (UNLIKELY(sph->omark.owner_id != tlh->thread_id)) {
    // Try to free blocks to the owner.
    if (remote_free(tlh, pbh, start_blk, end_blk, N))
      return;
  }

  // Return to the local pbh.
  uint32_t cl = pbh->sizeclass;
  blk_list_t* b_list = &tlh->blk_list[cl];

  uint32_t cnt_ref = get_blocks_for_class(cl) - 
                     (pbh->cnt_free + pbh->cnt_unused + pbh->remote_list.cnt);
  if (cnt_ref == N) {
    // PBH becomes totally free.
    pbh_list_remove(&b_list->pbh_list, pbh);
    pb_free(tlh, pbh);
  } else {
    // Move this pbh to the first of pbh list.
    if (b_list->pbh_list != pbh) {
      pbh_list_move_to_first(&b_list->pbh_list, pbh);
    }

    // Prepend to the the free list of pbh.
    SET_NEXT(end_blk, pbh->free_list);
    pbh->free_list = start_blk;
    pbh->cnt_free += N;
  }
}


static void pbh_add_unused(tlh_t* tlh, pbh_t* pbh, void* unused, uint32_t N) {
  uint32_t cl = pbh->sizeclass;
  blk_list_t* b_list = &tlh->blk_list[cl];

  uint32_t cnt_ref = get_blocks_for_class(cl) - 
                     (pbh->cnt_free + pbh->cnt_unused + pbh->remote_list.cnt);
  if (cnt_ref == N) {
    // PBH becomes totally free.
    pbh_list_remove(&b_list->pbh_list, pbh);
    pb_free(tlh, pbh);
  } else {
    // Move this pbh to the first of pbh list.
    if (b_list->pbh_list != pbh) {
      pbh_list_move_to_first(&b_list->pbh_list, pbh);
    }

    // Add the unallocateed chunk to pbh.
    if (pbh->cnt_unused == 0) {
      pbh->unallocated = unused;
      pbh->cnt_unused  = N;
      return;
    } 

    // PBH already has a unallocated chunk. This may be due to block coloring.
    // We split the smaller chunk between two unallocated chunks.
    void* start_blk;
    void* end_blk;
    uint32_t block_num, blk_size;
    if (pbh->cnt_unused < N) {
      start_blk = pbh->unallocated;
      block_num = pbh->cnt_unused;

      pbh->unallocated  = unused;
      pbh->cnt_unused = N;
    } else {
      start_blk = unused;
      block_num = N;
    }

    // Split and prepend split blocks to the free list of pbh.
    end_blk  = start_blk;
    blk_size = get_size_for_class(cl);
    for (uint32_t i = 1; i < block_num; i++) {
      void* next_blk = end_blk + blk_size;
      SET_NEXT(end_blk, next_blk);
      end_blk = next_blk;
    }
    SET_NEXT(end_blk, pbh->free_list);
    pbh->free_list = start_blk;
    pbh->cnt_free += block_num;
  }
}


static inline sph_t* pbh_get_superpage(pbh_t* pbh) {
  assert(pbh->index > 0 && pbh->index <= SUPERPAGE_LEN);
  return (sph_t*)(pbh - pbh->index);
}


static inline void pbh_link_init(pbh_t* pbh) {
  pbh->next = pbh;
  pbh->prev = pbh;
}


static inline void pbh_field_init(pbh_t* pbh) {
  pbh->status      = PBH_ON_FREE_LIST;
  pbh->cnt_free    = 0;
  pbh->cnt_unused  = 0;
  pbh->free_list   = NULL;
  pbh->unallocated = NULL;
  pbh->remote_list.together = 0;
}


static inline void pbh_list_prepend(pbh_t** list, pbh_t* pbh) {
  if (*list != NULL) {
    pbh_t* top = *list;
    pbh->next = top;
    pbh->prev = top->prev;
    top->prev->next = pbh;
    top->prev = pbh;
  } else {
    pbh_link_init(pbh);
  }
  *list = pbh;
}


static inline void pbh_list_append(pbh_t** list, pbh_t* pbh) {
  if (*list != NULL) {
    pbh_t* top = *list;
    pbh->next = top;
    pbh->prev = top->prev;
    top->prev->next = pbh;
    top->prev = pbh;
  } else {
    pbh_link_init(pbh);
    *list = pbh;
  }
}


static inline pbh_t* pbh_list_pop(pbh_t** list) {
  assert(*list != NULL);

  pbh_t* pbh = *list;
  *list = (pbh != pbh->next) ? pbh->next : NULL;

  // Remove the pbh from the list.
  pbh->prev->next = pbh->next;
  pbh->next->prev = pbh->prev;
  pbh_link_init(pbh);

  return pbh;
}


static inline void pbh_list_remove(pbh_t** list, pbh_t* pbh) {
  if (pbh == pbh->next) {
    assert(*list == pbh);
    *list = NULL;
  } else {
    if (*list == pbh) *list = pbh->next;
    pbh->prev->next = pbh->next;
    pbh->next->prev = pbh->prev;
    pbh_link_init(pbh);
  }
}


static inline void pbh_list_move_to_first(pbh_t** list, pbh_t* pbh) {
  assert(pbh != pbh->next);

  // First remove the pbh.
  pbh->prev->next = pbh->next;
  pbh->next->prev = pbh->prev;

  // Prepend it.
  pbh_t* top = *list;
  pbh->next = top;
  pbh->prev = top->prev;
  top->prev->next = pbh;
  top->prev = pbh;

  // Make it the first.
  *list = pbh;
}



////////////////////////////////////////////////////////////////////////////
// Page Block Functions
////////////////////////////////////////////////////////////////////////////
/* Page block is allocated from Free Page Block List, global Free Superpage
   List, or the OS. */
static pbh_t* pb_alloc(tlh_t* tlh, size_t page_len) {
  assert(page_len > 0 && page_len <= NUM_PAGE_CLASSES);

  // Allocate a page block from TLH.
  pbh_t* pbh = pb_alloc_from_tlh(tlh, page_len);
  if (pbh) return pbh;

  // Check the Remote PB List of the first superpage in Superpage List
  sph_t* first_sph = tlh->sp_list;
  if (first_sph) {
    if (first_sph->remote_pb_list) {
      sph_get_remote_pbs(first_sph);
      tlh->sp_list = first_sph->next;

      pbh = pb_alloc_from_tlh(tlh, page_len);
      if (pbh) return pbh;
    }
  }

  // Request memory from the global Free Superpage List or the OS.
  sph_t* sph = sph_alloc(tlh);
  size_t new_page_id = sph->start_page;
  pbh = pbh_alloc(sph, new_page_id, page_len);
  pbh->status = PBH_IN_USE;
  pagemap_set_range(new_page_id, page_len, pbh);

  // remained pages
  assert(page_len < SUPERPAGE_LEN);
  size_t rem_start = new_page_id + page_len;
  size_t rem_len   = SUPERPAGE_LEN - page_len;
  pbh_t* rem_pbh  = pbh_alloc(sph, rem_start, rem_len);
  rem_pbh->status = PBH_ON_FREE_LIST;
  pbh_list_prepend(&tlh->free_pb_list[rem_len-1], rem_pbh);
  pagemap_set_range(rem_start, rem_len, rem_pbh);

  return pbh;
}


static pbh_t* pb_alloc_from_tlh(tlh_t* tlh, size_t page_len) {
  // Page class index is one less than page_len.
  int32_t pcl = page_len - 1;

  for (int32_t c = pcl; c < NUM_PAGE_CLASSES; c++) {
    if (tlh->free_pb_list[c] != NULL) {
      // Pop the first pbh.
      pbh_t* pbh = pbh_list_pop(&tlh->free_pb_list[c]);
      assert((pbh->length - 1) == c);

      // Make this pbh in-use, and if necessary, split it.
      pbh->status = PBH_IN_USE;
      if (c > pcl) pb_split(tlh, pbh, page_len);

      return pbh;
    }
  }

  return NULL;
}


static void pb_free(tlh_t* tlh, pbh_t* pbh) {
  assert(pbh->length <= SUPERPAGE_LEN);

  if (pbh->length < SUPERPAGE_LEN) {
    pbh = pb_coalesce(tlh, pbh);
  }

  // If the freed pbh makes superpage totally empty, release superpage.
  if (pbh->length == SUPERPAGE_LEN) {
    // Release the superpage.
    sph_free(tlh, pbh_get_superpage(pbh));
  } else {
    // Update the pbh status.
    pbh_field_init(pbh);

    // Insert it into the Free Page Block List.
    pbh_list_prepend(&tlh->free_pb_list[pbh->length-1], pbh);
  }
}


static void pb_remote_free(tlh_t* tlh, void* pb, pbh_t* pbh) {
  sph_t* sph = pbh_get_superpage(pbh);
  tlh->hazard_ptr->node = sph;

  while (1) {
    if (UNLIKELY(sph->omark.owner_id == DEAD_OWNER)) {
      if (take_superpage(tlh, sph)) {
        tlh->hazard_ptr->node = NULL;
        pb_free(tlh, pbh);
        return;
      }
    }

    void* top = sph->remote_pb_list;
    SET_NEXT(pb, top);
    if (CAS_ptr(&sph->remote_pb_list, top, pb)) {
      sph->omark.finish_mark = DO_NOT_FINISH;
      break;
    }
  }

  if (UNLIKELY(sph->omark.owner_id == DEAD_OWNER)) {
    take_superpage(tlh, sph);
  }
  tlh->hazard_ptr->node = NULL;
}


/*
   Split the pbh into two pbhs.
   The first pbh has len length and it will be used.
   The second pbh has remaning length and it is inserted into the appropriate
   free page block list.
 */
static inline void pb_split(tlh_t* tlh, pbh_t* pbh, size_t len) {
  assert(pbh->length > len);
  size_t rem_len = pbh->length - len;

  // Update the original pbh.
  pbh->length = len;

  // Make a pbh for a remaining run of pages and update free list.
  size_t rem_start = pbh->start_page + len;
  pbh_t* rem_pbh   = pbh_alloc(pbh_get_superpage(pbh), rem_start, rem_len);
  rem_pbh->status  = PBH_ON_FREE_LIST;
  pbh_list_prepend(&tlh->free_pb_list[rem_len-1], rem_pbh);

  // Update the pagemap.
  pagemap_set_range(rem_start, rem_len, rem_pbh);
}


static inline pbh_t* pb_coalesce(tlh_t* tlh, pbh_t* pbh) {
  pbh_t* prev_pbh = (pbh_t*)pagemap_get_checked(pbh->start_page - 1);
  assert(((uintptr_t)prev_pbh & HUGE_MALLOC_MARK) == 0);

  void*  next_val = pagemap_get_checked(pbh->start_page + pbh->length);
  pbh_t* next_pbh = ((uintptr_t)next_val & HUGE_MALLOC_MARK)
                    ? NULL : (pbh_t*)next_val;

  if (prev_pbh && (prev_pbh->status == PBH_ON_FREE_LIST)) {
    // Remove prev_pbh form the page list.
    uint32_t prev_len = prev_pbh->length;
    pbh_list_remove(&tlh->free_pb_list[prev_len-1], prev_pbh);

    prev_pbh->length += pbh->length;

    // If the coalesced length is same as the length of superpage,
    // we don't need to update more because superpage will be freed.
    if (prev_pbh->length == SUPERPAGE_LEN) {
      // Only prev_pbh is free.
      return prev_pbh;
    } else if (next_pbh && (next_pbh->status == PBH_ON_FREE_LIST)) {
      // Both prev_pbh and next_pbh are free. Coalesce together.
      uint32_t next_len = next_pbh->length;
      pbh_list_remove(&tlh->free_pb_list[next_len-1], next_pbh);

      prev_pbh->length += next_len;
      if (prev_pbh->length == SUPERPAGE_LEN) return prev_pbh;

      // Update the pagemap and deallocate next_pbh.
      pagemap_set_range(next_pbh->start_page, next_len, prev_pbh);
      pbh_free(next_pbh);
    }

    // Update the pagemap and deallocate pbh.
    pagemap_set_range(pbh->start_page, pbh->length, prev_pbh);
    pbh_free(pbh);

    return prev_pbh;
  } else if (next_pbh && (next_pbh->status == PBH_ON_FREE_LIST)) {
    // Only next_pbh is free.
    uint32_t next_len = next_pbh->length;
    pbh_list_remove(&tlh->free_pb_list[next_len-1], next_pbh);

    pbh->length += next_len;
    if (pbh->length == SUPERPAGE_LEN) return pbh;

    // Update the pagemap and deallocate next_pbh.
    pagemap_set_range(next_pbh->start_page, next_len, pbh);
    pbh_free(next_pbh);
  }

  return pbh;
}



////////////////////////////////////////////////////////////////////////////
// Thread Local Heap Functions
////////////////////////////////////////////////////////////////////////////
static void tlh_init() {
  // Set the thread ID
  uint32_t tid = atomic_inc_uint(&g_id);
  if (tid == MAX_NUM_THREADS) {
    HANDLE_ERROR("Too many threads are created...\n");
  }

  // Increase the number of currently running threads.
  atomic_inc_uint(&g_thread_num);

  tlh_t* tlh = &l_tlh;
  tlh->thread_id = tid;

  // Allocate a hazard pointer.
  tlh->hazard_ptr = hazard_ptr_alloc();
}


static void tlh_clear(tlh_t* tlh) {
#ifdef MALLOC_USE_PAGE_BLOCK_CACHE
  pb_cache_t* pb_cache = &tlh->pb_cache;
  for (int w = 0; w < NUM_PB_CACHE_WAY; w++) {
    pb_cache_block_t* block = &pb_cache->block[w];
    if (block->data) {
      pb_cache_return(tlh, block->data);
      block->data = NULL;
      block->length = 0;
    }
  }
#endif

  for (uint32_t cl = 0; cl < NUM_CLASSES; cl++) {
    blk_list_t* b_list = &tlh->blk_list[cl];

    if (b_list->free_blk_list != NULL) {
      assert(b_list->cnt_free > 0);
      tlh_return_list(tlh, cl);
    }
    
    if (b_list->ptr_to_unused != NULL) {
      assert(b_list->cnt_unused > 0);
      tlh_return_unused(tlh, cl);
    }

    if (b_list->pbh_list != NULL) {
      tlh_return_pbhs(tlh, cl);
    }
  }

  // If there remains superpages, make them orphaned.
  if (tlh->sp_list != NULL) {
    finish_superpages(tlh);
  }

  // Deallocate the hazard pointer.
  hazard_ptr_free(tlh->hazard_ptr);
  tlh->hazard_ptr = NULL;
}


static void tlh_return_list(tlh_t* tlh, uint32_t cl) {
  blk_list_t* b_list = &tlh->blk_list[cl];

  void* list = b_list->free_blk_list;
  assert(list != NULL);

  void* prev_blk = list;
  void* curr_blk = GET_NEXT(list);
  void* blk_list = list;
  size_t prev_page_id = (size_t)list >> PAGE_SHIFT;
  pbh_t* blk_pbh = (pbh_t*)pagemap_get(prev_page_id);
  uint32_t cont_num = 1;

  // If contiguous blocks are in the same pbh, we will return them together.
  while (curr_blk != NULL) {
    size_t curr_page_id = (size_t)curr_blk >> PAGE_SHIFT;

    // Check curr_blk is in the same pbh with the previous block.
    if (curr_page_id == prev_page_id) {
      // Same --> continue
      cont_num++;
    } else {
      pbh_t* pbh = (pbh_t*)pagemap_get(curr_page_id);
      if (pbh == blk_pbh) {
        // Same --> continue
        prev_page_id = curr_page_id;
        cont_num++;
      } else {
        // Different --> Return to the pbh.
        pbh_add_blocks(tlh, blk_pbh, blk_list, prev_blk, cont_num);

        // Renew the start block.
        blk_list = curr_blk;
        blk_pbh = pbh;
        prev_page_id = curr_page_id;
        cont_num = 1;
      }
    }

    // Move to the next block.
    prev_blk = curr_blk;
    curr_blk = GET_NEXT(curr_blk);
  }

  // Return remained blocks.
  pbh_add_blocks(tlh, blk_pbh, blk_list, prev_blk, cont_num);

  b_list->free_blk_list = NULL;
  b_list->cnt_free  = 0;
}


static void tlh_return_unused(tlh_t* tlh, uint32_t cl) {
  blk_list_t* b_list = &tlh->blk_list[cl];
  
  void* unallocated = b_list->ptr_to_unused;
  size_t page_id = (size_t)unallocated >> PAGE_SHIFT;
  pbh_t* pbh = (pbh_t*)pagemap_get(page_id);

  pbh_add_unused(tlh, pbh, unallocated, b_list->cnt_unused);

  b_list->ptr_to_unused = NULL;
  b_list->cnt_unused = 0;
}


static void tlh_return_pbhs(tlh_t* tlh, uint32_t cl) {
  blk_list_t* b_list = &tlh->blk_list[cl];

  uint32_t blks_per_pbh = get_blocks_for_class(cl);
  do {
    pbh_t* pbh = pbh_list_pop(&b_list->pbh_list);

    // Try again if we can free the pbh.
    uint32_t count = pbh->cnt_free + pbh->cnt_unused + pbh->remote_list.cnt;
    if (count == blks_per_pbh) {
      pb_free(tlh, pbh);
    }

    // If pbh has some unfreed blocks, just keep it in the superpage.
    // This is safe because superpage will not be freed.
    // Another thread will adopt the superpage itself.
  } while (b_list->pbh_list != NULL);
}



////////////////////////////////////////////////////////////////////////////
// Page Block Cache Functions
////////////////////////////////////////////////////////////////////////////
static inline int bit_pos(int v) {
  int pos;
  __asm__ __volatile__ (
      "bsf %1, %0"
      : "=r" (pos)
      : "r" (v)
      );
  return pos;
}


static inline int get_cache_hit_index(v8qi val) {
  int mask = __builtin_ia32_pmovmskb(val);
  return bit_pos(mask);
}


static inline void pb_cache_return(tlh_t* tlh, void* pb) {
  do {
    size_t page_id = (size_t)pb >> PAGE_SHIFT;
    pbh_t* pbh = pagemap_get(page_id);
    void* next_pb = GET_NEXT(pb);

    sph_t* sph = pbh_get_superpage(pbh);
    if (sph->omark.owner_id == tlh->thread_id) {
      pb_free(tlh, pbh);
    } else {
      pb_remote_free(tlh, pb, pbh);
    }

    pb = next_pb;
  } while (pb != NULL);
}



////////////////////////////////////////////////////////////////////////////
// Allocation/Deallocation Functions
////////////////////////////////////////////////////////////////////////////
static inline void* bump_alloc(size_t size, blk_list_t* b_list) {
  void* ret = b_list->ptr_to_unused;

  // If size is smaller than the half of cache line size, 
  // split all blocks in a cache line.
  if (size <= (CACHE_LINE_SIZE / 2)) {
    uint32_t blks_per_line = CACHE_LINE_SIZE / size;

    // Update the Free Block List.
    b_list->free_blk_list = ret + size;
    b_list->cnt_free = blks_per_line - 1;

    void* free_blk = b_list->free_blk_list;
    for (int i = 2; i < blks_per_line; i++) {
      void* next_blk = free_blk + size;
      SET_NEXT(free_blk, next_blk);
      free_blk = next_blk;
    }
    SET_NEXT(free_blk, NULL);

    // Update the unallocated pointer.
    b_list->cnt_unused -= blks_per_line;
    b_list->ptr_to_unused = (b_list->cnt_unused > 0) ? (free_blk+size) : NULL;
  } else {
    // Update the unallocated pointer.
    b_list->cnt_unused--;
    b_list->ptr_to_unused = (b_list->cnt_unused > 0) ? (ret + size) : NULL;
  }

  return ret;
}


/*
   Allocate a memory for small sizes.
   - size: byte size corresponding to the class cl
   - cl: size-class
 */
static inline void* small_malloc(uint32_t cl) {
  tlh_t* tlh = &l_tlh;
  blk_list_t* b_list = &tlh->blk_list[cl];

  ////////////////////////////////////////////////////////////////////////
  // Case 1: When we have thread-local free list.
  ////////////////////////////////////////////////////////////////////////
  if (LIKELY(b_list->free_blk_list != NULL)) {
    assert(b_list->cnt_free > 0);

    // Pop the first free block.
    void* ret = b_list->free_blk_list;
    b_list->free_blk_list = GET_NEXT(ret);
    b_list->cnt_free--;

    return ret;
  }

  ////////////////////////////////////////////////////////////////////////
  // Case 2: When we have the unallocated chunk
  ////////////////////////////////////////////////////////////////////////
  size_t size = get_size_for_class(cl);
  if (b_list->ptr_to_unused != NULL) {
    assert(b_list->cnt_unused > 0);
    // Use pointer-bumping allocation.
    return bump_alloc(size, b_list);
  }

  ////////////////////////////////////////////////////////////////////////
  // Case 3: Allocate from the pbh list.
  ////////////////////////////////////////////////////////////////////////
  if (b_list->pbh_list != NULL) {
    pbh_t* pbh = b_list->pbh_list;

    if (pbh->cnt_free > 0) {
      // PBH has the free list.
      assert(pbh->free_list != NULL);
      void* ret = pbh->free_list;
      
      b_list->free_blk_list = GET_NEXT(pbh->free_list);
      b_list->ptr_to_unused = pbh->unallocated;
      b_list->cnt_free   = pbh->cnt_free - 1;
      b_list->cnt_unused = pbh->cnt_unused;

      pbh->cnt_free    = 0;
      pbh->cnt_unused  = 0;
      pbh->free_list   = NULL;
      pbh->unallocated = NULL;

      if (pbh->remote_list.cnt == 0) {
        // Move this pbh to the last of pbh_list.
        b_list->pbh_list = pbh->next;
      }

      return ret;
    } else if (pbh->cnt_unused > 0) {
      // PBH has only the unallocated chunk.
      assert(pbh->unallocated != NULL);

      b_list->ptr_to_unused = pbh->unallocated;
      b_list->cnt_unused = pbh->cnt_unused;

      pbh->unallocated = NULL;
      pbh->cnt_unused  = 0;

      if (pbh->remote_list.cnt == 0) {
        // Move this pbh to the last of pbh_list.
        b_list->pbh_list = pbh->next;
      }

      return bump_alloc(size, b_list);
    } else if (pbh->remote_list.cnt > 0) {
      // If there exists a remote list, get it.
      remote_list_t top;
      do {
        top = pbh->remote_list;
      } while (!CAS64((uint64_t*)&pbh->remote_list, top.together, 0));

      void* page_addr = (void*)(pbh->start_page << PAGE_SHIFT);
      void* ret = page_addr + size * top.head;

      b_list->free_blk_list = GET_NEXT(ret);
      b_list->cnt_free = top.cnt - 1;

      // Move this pbh to the last of pbh_list.
      b_list->pbh_list = pbh->next;

      return ret;
    }
  }

  ////////////////////////////////////////////////////////////////////////
  // Case 4: Otherwise, allocate a new pbh.
  ////////////////////////////////////////////////////////////////////////
  uint32_t page_num = get_pages_for_class(cl);
  pbh_t* pbh = pb_alloc(tlh, page_num);
  pbh_list_append(&b_list->pbh_list, pbh);

  pbh->sizeclass = cl;
  pbh->cnt_free  = 0;
  pbh->free_list = NULL;
  if (size & (CACHE_LINE_SIZE - 1)) {
    pbh->status = PBH_AGAINST_FALSE_SHARING;
  }
  pbh->remote_list.together = 0;

  uint32_t blks_per_pbh = get_blocks_for_class(cl);
  void* start_addr = (void*)(pbh->start_page << PAGE_SHIFT);
  
  pbh->unallocated = NULL;
  pbh->cnt_unused  = 0;

  b_list->ptr_to_unused = start_addr;
  b_list->cnt_unused = blks_per_pbh;

  return bump_alloc(size, b_list);
}


#ifdef MALLOC_USE_PAGE_BLOCK_CACHE
static inline void pcache_check_sanity(pb_cache_t* pb_cache) {
  for (int i = 0; i < NUM_PB_CACHE_WAY; i++) {
    unsigned pb_len = (uint8_t)pb_cache->tag.e[i];
    void*    pb     = pb_cache->block[i].data;
    size_t list_len = pb_cache->block[i].length;
    size_t cnt = 0;
    while (pb != NULL) {
      size_t page_id = (size_t)pb >> PAGE_SHIFT;
      pbh_t* pbh = (pbh_t*)pagemap_get(page_id);
      if (pbh == NULL) {
        CRASH("ERROR: i=%u pb_len=%u pbh is NULL\n", i, pb_len);
      } else if (pbh->length != pb_len) {
        CRASH("ERROR: i=%u pb_len=%u pbh->length=%u\n",i,pb_len, pbh->length);
      }

      pb = GET_NEXT(pb);
      cnt++;
    }
    if (cnt != list_len) {
      CRASH("ERROR: i=%u pb_len=%u list_len=%lu cnt=%lu\n",
            i, pb_len, list_len, cnt);
    }
  }
}


static inline void print_char8(char8_t v) {
  printf("{ ");
  for (int i = 7; i >= 0; i--) {
    printf("%u", (uint8_t)v.e[i]);
    if (i > 0) printf(", ");
  }
  printf(" }");
}
#endif


/* malloc for MAX_SIZE < size <= NUM_PAGE_CLASSES pages. */
static inline void* large_malloc(size_t page_len) {
  tlh_t* tlh = &l_tlh;
#ifdef MALLOC_USE_PAGE_BLOCK_CACHE
  pb_cache_t* pb_cache = &tlh->pb_cache;
  
  char in = (char)page_len;
  char8_t v_in = {.v = (v8qi){in, in, in, in, in, in, in, in}};

  // Compare with cache
  int pos;
  char8_t v_cmp;
  v_cmp.v = __builtin_ia32_pcmpeqb(pb_cache->tag.v, v_in.v);
  if (*(uint64_t*)&v_cmp) {
    inc_pcache_malloc_hit();

    // Hit
    pos = get_cache_hit_index(v_cmp.v);

    // Update LRU state.
    pb_cache->state = (pb_cache->state & g_way_table[pos].mask) |
                      g_way_table[pos].set_bit;

    // Check the cache block.
    pb_cache_block_t* block = &pb_cache->block[pos];
    if (block->data) {
      inc_pcache_malloc_real_hit();

      void* ret = block->data;
      block->data = GET_NEXT(ret);
      block->length--;

      return ret;
    }
  }
  else {
    inc_pcache_malloc_miss();

    // Miss
    pos = g_lru_table[pb_cache->state];

    // Update LRU state.
    pb_cache->state = (pb_cache->state & g_way_table[pos].mask) |
                      g_way_table[pos].set_bit;

    // Evict the victim.
    pb_cache_block_t* block = &pb_cache->block[pos];
    if (block->data) {
      inc_pcache_malloc_evict();

      pb_cache_return(tlh, block->data);

      block->data = NULL;
      block->length = 0;
    }

    // Save the new page class.
    pb_cache->tag.e[pos] = in;
  }

  pbh_t* pbh = pb_alloc(tlh, page_len);
  pbh->sizeclass = NUM_CLASSES;
  return (void*)(pbh->start_page << PAGE_SHIFT);
#else
  pbh_t* pbh = pb_alloc(tlh, page_len);
  pbh->sizeclass = NUM_CLASSES;
  return (void*)(pbh->start_page << PAGE_SHIFT);
#endif
}


static inline void* huge_malloc(size_t page_len) {
  // Use mmap directly.
  size_t size = page_len << PAGE_SHIFT;

  void* ret = do_mmap(size);

  size_t page_id = (size_t)ret >> PAGE_SHIFT;
  void* val = (void*)(size | HUGE_MALLOC_MARK);

  pagemap_expand(page_id, 1);
  pagemap_set(page_id, val);

  return ret;
}


static inline bool remote_free(tlh_t* tlh, pbh_t* pbh,
                               void* first, void* last, uint32_t N) {
  sph_t* sph = pbh_get_superpage(pbh);
  uint32_t cl = pbh->sizeclass;

  void* start_addr = (void*)(pbh->start_page << PAGE_SHIFT);
  uint32_t size = get_size_for_class(cl);
  uint16_t blk_idx = (uintptr_t)(first - start_addr) / size;

  remote_list_t new_top;
  new_top.head = blk_idx;

  tlh->hazard_ptr->node = sph;

  while (true) {
    if (UNLIKELY(sph->omark.owner_id == DEAD_OWNER)) {
      if (take_superpage(tlh, sph)) {
        tlh->hazard_ptr->node = NULL;
        return false;
      }
    }

    remote_list_t top = pbh->remote_list;
    if (top.cnt == 0) {
      SET_NEXT(last, NULL);
    } else {
      void* head_addr = start_addr + (size * top.head);
      SET_NEXT(last, head_addr);
    }
    new_top.cnt = top.cnt + N;

    if (CAS64((uint64_t*)&pbh->remote_list, top.together, new_top.together)) {
      sph->omark.finish_mark = DO_NOT_FINISH;
      break;
    }
  }

  if (UNLIKELY(sph->omark.owner_id == DEAD_OWNER)) {
    take_superpage(tlh, sph);
  }

  tlh->hazard_ptr->node = NULL;

  return true;
}


/* Deallocate a memory for small sizes. */
static inline void small_free(void* ptr, pbh_t* pbh) {
  tlh_t* tlh = &l_tlh;

  if (pbh->status == PBH_AGAINST_FALSE_SHARING) {
    sph_t* sph = pbh_get_superpage(pbh);
    if (UNLIKELY(sph->omark.owner_id != tlh->thread_id)) {
      // Try to free the block to the owner.
      if (remote_free(tlh, pbh, ptr, ptr, 1))
        return;
    }
  }

  // Local free
  uint32_t cl = pbh->sizeclass;
  blk_list_t* b_list = &tlh->blk_list[cl];

  uint32_t threshold = get_blocks_for_class(cl);
  if (UNLIKELY(b_list->cnt_free >= threshold)) {
    tlh_return_list(tlh, cl);
  }

  // Prepend the free block to the free block list for its size-class.
  SET_NEXT(ptr, b_list->free_blk_list);
  b_list->free_blk_list = ptr;
  b_list->cnt_free++;
}


static inline void large_free(void* ptr, pbh_t* pbh) {
  tlh_t* tlh = &l_tlh;
#ifdef MALLOC_USE_PAGE_BLOCK_CACHE
  pb_cache_t* pb_cache = &tlh->pb_cache;

  char in = (char)pbh->length;
  char8_t v_in = {.v = (v8qi){in, in, in, in, in, in, in, in}};

  // Compare with cache
  int pos;
  char8_t v_cmp;
  v_cmp.v = __builtin_ia32_pcmpeqb(pb_cache->tag.v, v_in.v);
  if (*(uint64_t*)&v_cmp) {
    inc_pcache_free_hit();

    // Hit
    pos = get_cache_hit_index(v_cmp.v);

    // Link to the page cache.
    pb_cache_block_t* block = &pb_cache->block[pos];
    if (block->length < 2) {
      // Page block cache keeps up to two PBs.
      SET_NEXT(ptr, block->data);
      block->data = ptr;
      block->length++;
    } else {
      sph_t* sph = pbh_get_superpage(pbh);
      if (sph->omark.owner_id == tlh->thread_id) {
        pb_free(tlh, pbh);
      } else {
        pb_remote_free(tlh, ptr, pbh);
      }
    }
  } else {
    inc_pcache_free_miss();

    // Miss
    pos = g_lru_table[pb_cache->state];

    // Evict the victim.
    pb_cache_block_t* block = &pb_cache->block[pos];
    if (block->data) {
      inc_pcache_free_evict();

      pb_cache_return(tlh, block->data);
    }

    SET_NEXT(ptr, NULL);
    block->data = ptr;
    block->length = 1;

    // Save the new page class.
    pb_cache->tag.e[pos] = in;
  }

  // Update LRU state.
  pb_cache->state = (pb_cache->state & g_way_table[pos].mask) |
                    g_way_table[pos].set_bit;
#else
  sph_t* sph = pbh_get_superpage(pbh);
  if (sph->omark.owner_id == tlh->thread_id) {
    pb_free(tlh, pbh);
  } else {
    pb_remote_free(tlh, ptr, pbh);
  }
#endif
}


static inline void huge_free(void* ptr, size_t size) {
  do_munmap(ptr, size);
  pagemap_set((size_t)ptr >> PAGE_SHIFT, NULL);
}



////////////////////////////////////////////////////////////////////////////
// Library Functions
////////////////////////////////////////////////////////////////////////////
/*
   malloc() allocates size bytes and returns a pointer to the allocated 
   memory.  The memory is not cleared. 

   PARAMETER
   - size: bytes to allocate

   RETURN VALUE
   - Return a pointer to the allocated memory, which is suitably aligned for
     any kind of variable.
   - On error, return NULL.
   - NULL may also be returned by a successful call to malloc() with a size
     of zero.
 */
void *malloc(size_t size) {
  inc_cnt_malloc();
  malloc_timer_start();

#ifdef MALLOC_NEED_INIT
  if (UNLIKELY(!g_initialized)) sf_malloc_init();
  if (UNLIKELY(l_tlh.thread_id == 0)) sf_malloc_thread_init();
#else
  // malloc() should be called after library initialization through 
  // sf_malloc_init() call.
  assert(g_initialized != 0);
#endif

  void* ret;
  if (size <= MAX_SIZE) {
    uint32_t cl = get_sizeclass(size);
    ret = small_malloc(cl);
  } else {
    size_t page_len = GET_PAGE_LEN(size);
    if (page_len <= NUM_PAGE_CLASSES) {
      ret = large_malloc(page_len);
    } else {
      ret = huge_malloc(page_len);
    }
  }

  malloc_timer_stop();

  return ret;
}


/*
   free() frees the memory space pointed to by ptr, which must have been 
   returned by a previous call to malloc(), calloc() or realloc(). 
   Otherwise, or if free(ptr) has already been called before, undefined 
   behavior occurs.

   PARAMETER
   - ptr: pointer to free

   RETURN VALUE
   - Return no value.
 */
void free(void *ptr) {
  inc_cnt_free();
  free_timer_start();

#ifdef MALLOC_NEED_INIT
  if (UNLIKELY(l_tlh.thread_id == 0)) sf_malloc_thread_init();
#endif

  if (UNLIKELY(ptr == NULL)) return;

  size_t page_id = (size_t)ptr >> PAGE_SHIFT;
  void* val = pagemap_get(page_id);
  assert(val != NULL);

  if (UNLIKELY((uintptr_t)val & HUGE_MALLOC_MARK)) {
    size_t size = (size_t)val & ~HUGE_MALLOC_MARK;
    huge_free(ptr, size);
  } else {
    pbh_t* pbh = (pbh_t*)val;
    if (pbh->sizeclass < NUM_CLASSES) {
      small_free(ptr, pbh);
    } else {
      large_free(ptr, pbh);
    }
  }

  free_timer_stop();
}


/*
   calloc() allocates memory for an array of nmemb elements of size bytes 
   each and returns a pointer to the allocated memory.
   The memory is set to zero.

   PARAMETER
   - nmemb: # of elements
   - size: size of each element

   RETURN VALUE
   - Return a pointer to the allocated memory, which is suitably aligned for
     any kind of variable.
   - On error, return NULL.
   - NULL may also be returned by a successful call to calloc() with nmemb 
     or size equal to zero.
 */
void *calloc(size_t nmemb, size_t size) {
  // If nmemb or size is 0, then calloc() returns either NULL, or a unique
  // pointer value that can later be successfully passed to free().
  if (nmemb == 0 || size == 0) {
    return NULL;
  }

  size_t total_size = nmemb * size;
  void *ret = malloc(total_size);
  if (ret) memset(ret, 0, total_size);

  return ret;
}


/* 
   realloc() changes the size of the memory block pointed to by ptr to size
   bytes.  The contents will be unchanged to the minimum of the old and new 
   sizes; newly allocated memory will be uninitialized.

   PARAMETER
   - ptr: pointer to existing memory block
   - size: new size

   RETURN VALUE
   - Return a pointer to the newly allocated memory, which is suitably 
     aligned for any kind of variable and may be different from ptr, 
     or NULL if the request fails.
   - If size was equal to 0, either NULL or a pointer suitable to be passed 
     to free() is returned.
   - If realloc() fails the original block is left untouched; 
     it is not freed or moved. 
 */
void *realloc(void *ptr, size_t size) {
  // If ptr is NULL, then the call is equivalent to malloc(size), for all 
  // values of size.
  if (ptr == NULL) {
    return malloc(size);
  }

  // If size is equal to zero, and ptr is not NULL, then the call is 
  // equivalent to free(ptr).  Unless ptr is NULL, it must have been returned
  // by an earlier call to malloc(), calloc() or realloc(). 
  // If the area pointed to was moved, a free(ptr) is done.
  if (size == 0) {
    free(ptr);
    return NULL;
  }

  inc_cnt_realloc();
  realloc_timer_start();

  size_t old_size;
  size_t page_id = (size_t)ptr >> PAGE_SHIFT;
  void* val = pagemap_get(page_id);
  if (UNLIKELY((uintptr_t)val & HUGE_MALLOC_MARK)) {
    old_size = (size_t)val & ~HUGE_MALLOC_MARK;
  } else {
    pbh_t* pbh = (pbh_t*)val;
    if (pbh->sizeclass < NUM_CLASSES) {
      old_size = get_size_for_class(pbh->sizeclass);
    } else {
      old_size = pbh->length * PAGE_SIZE;
    }
  }

  // If size is larger than old_size or size is smaller than the half of
  // old_size, allocate a new block.
  void* ret;
  if ((size > old_size) || (size < (old_size / 2))) {
    ret = malloc(size);
    memcpy(ret, ptr, ((old_size < size) ? old_size : size));
    free(ptr);
  } else {
    // Otherwise, return the original pointer.
    ret = ptr;
  }

  realloc_timer_stop();

  return ret;
}


/*
   The function posix_memalign() allocates size bytes and places the address
   of the allocated memory in *memptr.  The address of the allocated memory
   will be a multiple of alignment, which must be a power of two and a 
   multiple of sizeof(void *).  If size is 0, then posix_memalign() returns
   either NULL, or a unique pointer value that can later be successfully
   passed to free().

   PARAMETER
   - memptr: pointer for saving the pointer to the allocated memory
   - alignment: alignment number
   - size: bytes to allocate

   RETURN VALUE
   - Return zero on success, or one of the error values listed in ERRORS
   
   ERRORS
   - EINVAL: The alignment argument was not a power of two, or was not a 
             multiple of sizeof(void *).
   - ENOMEM: There was insufficient memory to fulfill the allocation request. 
 */
int posix_memalign(void **memptr, size_t alignment, size_t size) {
  inc_cnt_memalign();
  memalign_timer_start();

  if (size == 0) {
    *memptr = NULL;
    memalign_timer_stop();
    return 0;
  }

  // Check if alignment is a power of 2.
  if ((alignment & (alignment - 1)) != 0) {
    *memptr = NULL;
    memalign_timer_stop();
    return EINVAL;
  }

  // Fall back to malloc if we would already align properly.
  if (alignment <= get_alignment(size)) {
    *memptr = malloc(size);
    assert(((uintptr_t)(*memptr) % alignment) == 0);
    memalign_timer_stop();
    return 0;
  }

  // Bigger alignment.
  if (size <= MAX_SIZE && alignment < PAGE_SIZE) {
    uint32_t cl = get_sizeclass(size);
    while (cl < NUM_CLASSES &&
           ((get_size_for_class(cl) & (alignment - 1)) != 0)) {
      cl++;
    }
    if (cl < NUM_CLASSES) {
      size = get_size_for_class(cl);
      *memptr = malloc(size);
      memalign_timer_stop();
      return 0;
    }
  }

  // Use pagelist allocator.
  if (alignment <= PAGE_SIZE) {
    // size may not huge, but we need page allocation.
    size_t page_num = GET_PAGE_LEN(size);
    if (page_num <= NUM_PAGE_CLASSES) {
      *memptr = large_malloc(page_num);
    } else {
      *memptr = huge_malloc(page_num);
    }
    memalign_timer_stop();
    return 0;
  }

  // Allocate extra pages and carve off an aligned portion.
  size_t alloc_pages = GET_PAGE_LEN(size + alignment);
  void *new_blk = huge_malloc(alloc_pages);
  assert(new_blk != NULL);

  void* ret_blk = new_blk;
  while (((uintptr_t)ret_blk & (alignment - 1)) != 0) {
    ret_blk += PAGE_SIZE;
  }
  assert(((size_t)(ret_blk - new_blk) >> PAGE_SHIFT) < alloc_pages);

  if (ret_blk != new_blk) {
    // We need split.
    void* val = pagemap_get((size_t)new_blk >> PAGE_SHIFT);
    size_t skip_size = (size_t)((uintptr_t)ret_blk - (uintptr_t)new_blk);
    huge_free(new_blk, skip_size);

    size_t size = ((size_t)val & ~HUGE_MALLOC_MARK) - skip_size;
    size_t page_id = (size_t)ret_blk >> PAGE_SHIFT;
    val = (void*)(size | HUGE_MALLOC_MARK);

    pagemap_expand(page_id, 1);
    pagemap_set(page_id, val);
  }

  *memptr = ret_blk;
  memalign_timer_stop();
  return 0;
}


/*
   valloc() allocates size bytes and returns a pointer to the allocated
   memory.  The memory address will be a multiple of the page size.

   PARAMETER
   - size: bytes to allocate

   RETURN VALUE
   - Return the pointer to the allocated memory
   - Return NULL if the request fails
 */
void *valloc(size_t size) {
  void *free_blk;
  int ret = posix_memalign(&free_blk, sysconf(_SC_PAGESIZE), size);
  assert(ret == 0);

  return free_blk;
}


/*
   memalign() allocates size bytes and returns a pointer to the allocated
   memory.  The memory address will be a multiple of boundary, which must 
   be a power of two.

   PARAMETER
   - boundary: alignment number
   - size: bytes to allocate

   RETURN VALUE
   - Return the pointer to the allocated memory
   - Return NULL if the request fails
 */
void *memalign(size_t boundary, size_t size) {
  void *free_blk;
  int ret = posix_memalign(&free_blk, boundary, size);
  assert(ret == 0);

  return free_blk;
}


////////////////////////////////////////////////////////////////////////////
// Statistics Functions
////////////////////////////////////////////////////////////////////////////
void malloc_stats() {
}

#ifdef MALLOC_STATS
void stats_init() {
  FILE *cpuinfo_stream;
  char buffer[100];

  cpuinfo_stream = fopen("/proc/cpuinfo", "r");
  if (cpuinfo_stream == NULL) {
    HANDLE_ERROR("fopen() in stats_init()");
  }

  while (fgets(buffer, 100, cpuinfo_stream)) {
    if (strncmp(buffer, "cpu MHz", 7) == 0) {
      char *pch = strtok(buffer, " :");
      for (int i = 0; i < 2; i++) {
        pch = strtok(NULL, " :");
      }
      CPU_CLOCK = atof(pch) * 1e6;
//      printf("cpu frequency: %.1f\n", CPU_CLOCK);
      break;
    }
  }

  fclose(cpuinfo_stream);
}

void print_stats() {
  fprintf(stdout, "======= THREAD(%u) STATISTICS =======\n"
      "malloc  : cnt(%lu) time(%.9f)\n"
      "free    : cnt(%lu) time(%.9f)\n"
      "realloc : cnt(%lu) time(%.9f)\n"
      "memalign: cnt(%lu) time(%.9f)\n"
      "pcache  : malloc(hit:%lu real_hit:%lu miss:%lu evict:%lu)\n"
      "          free(hit:%lu miss:%lu evict:%lu)\n"
      "mmap    : cnt(%lu) size(%lu B, %.1f KB, %.1f MB) max(%.1f MB)\n"
      "munmap  : cnt(%lu) size(%lu B, %.1f KB, %.1f MB)\n"
      "madvise : cnt(%lu) size(%lu B, %.1f KB, %.1f MB)\n\n",
      l_tlh.thread_id,
      get_cnt_malloc(), get_time_malloc(),
      get_cnt_free(), get_time_free(),
      get_cnt_realloc(), get_time_realloc(),
      get_cnt_memalign(), get_time_memalign(),
      get_pcache_malloc_hit(), get_pcache_malloc_real_hit(),
      get_pcache_malloc_miss(), get_pcache_malloc_evict(),
      get_pcache_free_hit(), get_pcache_free_miss(), get_pcache_free_evict(),

      get_cnt_mmap(), get_size_mmap(),
      getKB(get_size_mmap()), getMB(get_size_mmap()),
      getMB(get_size_mmap_max()),

      get_cnt_munmap(), get_size_munmap(),
      getKB(get_size_munmap()), getMB(get_size_munmap()),

      get_cnt_madvise(), get_size_madvise(),
      getKB(get_size_madvise()), getMB(get_size_madvise())
      );
}
#endif


////////////////////////////////////////////////////////////////////////////
// To support miscellaneous functions
////////////////////////////////////////////////////////////////////////////
#define ALIAS(x)  __attribute__ ((alias (x)))
void cfree(void *ptr)                               ALIAS("free");

#ifdef __GLIBC__
void *__libc_malloc(size_t size)                    ALIAS("malloc");
void  __libc_free(void *ptr)                        ALIAS("free");
void *__libc_realloc(void *ptr, size_t size)        ALIAS("realloc");
void *__libc_calloc(size_t n, size_t size)          ALIAS("calloc");
void  __libc_cfree(void *ptr)                       ALIAS("free");
void *__libc_memalign(size_t align, size_t s)       ALIAS("memalign");
void *__libc_valloc(size_t size)                    ALIAS("valloc");
int __posix_memalign(void **r, size_t a, size_t s)  ALIAS("posix_memalign");
#endif


////////////////////////////////////////////////////////////////////////////
// Debug Functions
////////////////////////////////////////////////////////////////////////////
#ifdef MALLOC_DEBUG
// Output stream for debugging
static FILE *g_DOUT = NULL;

static void debug_init() {
  g_DOUT = stdout;
}

static inline void print_class_array() {
  fprintf(g_DOUT, "========== SizeMap.class_array ==========\n");
  for (int i = 0; i < CLASS_ARRAY_SIZE; ++i) {
    fprintf(g_DOUT, "%3d: %u\n", i, g_sizemap.class_array[i]);
  }
  fprintf(g_DOUT, "\n");
}

static inline void print_class_to_size() {
  fprintf(g_DOUT, "========== SizeMap.class_to_size ==========\n");
  for (int i = 0; i < NUM_CLASSES; ++i) {
    fprintf(g_DOUT, "%2d: %u (%u)\n",
        i, g_sizemap.info[i].class_to_size,
        g_sizemap.info[i].class_to_size % CACHE_LINE_SIZE);
  }
  fprintf(g_DOUT, "\n");
}

static inline void print_class_to_pages() {
  fprintf(g_DOUT, "========== SizeMap.class_to_pages ==========\n");
  for (int i = 0; i < NUM_CLASSES; ++i) {
    fprintf(g_DOUT, "%2d: %u\n", i, g_sizemap.info[i].class_to_pages);
  }
  fprintf(g_DOUT, "\n");
}

static inline void print_num_blocks_per_pbh() {
  fprintf(g_DOUT, "========== SizeMap.num_blocks_per_pbh ==========\n");
  for (int i = 0; i < NUM_CLASSES; ++i) {
    fprintf(g_DOUT, "%2d: %u\n", i, g_sizemap.info[i].num_blocks_per_pbh);
  }
  fprintf(g_DOUT, "\n");
}

#ifdef MALLOC_DEBUG_DETAIL
static void print_sizemap() {
  print_class_array();
  print_class_to_size();
  print_class_to_pages();
  print_num_blocks_per_pbh();
}
#endif

static uint32_t get_pbh_list_length(const pbh_t* list) {
  if (list == NULL) return 0;

  uint32_t len = 1;
  for (pbh_t* s = list->next; s != list; s = s->next) {
    len++;
  }
  return len;
}

static const char* get_pbh_status_str(uint32_t status) {
  switch (status) {
    case PBH_ON_FREE_LIST: return "PBH_ON_FREE_LIST";
    case PBH_IN_USE:       return "PBH_IN_USE";
    case PBH_AGAINST_FALSE_SHARING:
                           return "PBH_AGAINST_FALSE_SHARING";
    default: return "UNKNOWN";
  }
}

void print_pbh(pbh_t* pbh) {
  fprintf(g_DOUT,
      "---------------------------------------\n"
      "current pbh: %p [T%u]\n"
      "---------------------------------------\n"
      "next        : %p\n"
      "prev        : %p\n"
      "start_page  : 0x%lx\n"
      "length      : %u\n"
      "sizeclass   : %u\n"
      "status      : %s\n"
      "cnt_free    : %u\n"
      "cnt_unused  : %u\n"
      "page_color  : %u\n"
      "block_color : %u\n"
      "free_list   : %p\n"
      "unallocated : %p\n"
      "remote_list.head : %u\n"
      "remote_list.cnt  : %u\n"
      "---------------------------------------\n",
      pbh, l_tlh.thread_id,
      pbh->next, pbh->prev,
      pbh->start_page, pbh->length, pbh->sizeclass,
      get_pbh_status_str(pbh->status),
      pbh->cnt_free, pbh->cnt_unused,
      pbh->page_color, pbh->block_color,
      pbh->free_list, pbh->unallocated,
      pbh->remote_list.head, pbh->remote_list.cnt
  );
}

void print_pbh_list(pbh_t* list) {
  fprintf(g_DOUT, "========== PBH List ==========\n"); 
  if (list == NULL) {
    fprintf(g_DOUT, "No list\n");
    return;
  }

  uint64_t sum_cnt_free = 0;
  uint64_t sum_cnt_unused = 0;
  uint64_t sum_cnt_remote = 0;

  pbh_t* pbh = list;
  while (1) {
    print_pbh(pbh);

    sum_cnt_free += pbh->cnt_free;
    sum_cnt_unused += pbh->cnt_unused;
    sum_cnt_remote += pbh->remote_list.cnt;

    fprintf(g_DOUT, "PBH FREE LIST: \n");
    print_block_list(pbh->free_list);

    pbh = pbh->next;
    if (pbh != list) fprintf(g_DOUT, "--->\n");
    else break;
    if (pbh == pbh->next) {
      fprintf(g_DOUT, "!!!!!!!! WHAT? !!!!!!!!\n");
      break;
    }
  };

  fprintf(g_DOUT,
      "---------------------------------------\n"
      "SUMMARY\n"
      "---------------------------------------\n"
      "length of list: %u\n"
      "sum_cnt_free  : %lu\n"
      "sum_cnt_unused: %lu\n"
      "sum_cnt_remote: %lu\n"
      "---------------------------------------\n\n",
      get_pbh_list_length(list),
      sum_cnt_free, sum_cnt_unused, sum_cnt_remote
  );
  fflush(g_DOUT);
}

void print_superpage(sph_t* spage) {
  fprintf(g_DOUT,
      "---------------------------------------\n"
      "current SP : %p\n"
      "---------------------------------------\n"
      "next       : %p\n"
      "prev       : %p\n"
      "start_page : 0x%lx\n"
      "omark.onwer_id    : %u\n"
      "omark.finish_mark : %u\n"
      "---------------------------------------\n",
      spage,
      spage->next, spage->prev, spage->start_page,
      spage->omark.owner_id, spage->omark.finish_mark
  );

  pbh_t* pbh = (pbh_t*)spage + 1;
  uint32_t total_len = 0;
  while (total_len < SUPERPAGE_LEN) {
    uint32_t len = pbh->length;
    print_pbh(pbh);

    // next pbh
    total_len += len;
    pbh = pbh + len;
  }
  assert(total_len == SUPERPAGE_LEN);
}

void print_superpage_list(sph_t* list) {
  fprintf(g_DOUT, "========== SuperPage List ==========\n"); 
  if (list == NULL) {
    fprintf(g_DOUT, "No list\n");
    return;
  }

  uint32_t superpage_len = 1;

  sph_t* spage = list;
  while (1) {
    print_superpage(spage);

    // next superpage
    spage = spage->next;
    if (spage != list) {
      fprintf(g_DOUT, "--->\n");
      superpage_len++;
    } else break;
  };

  fprintf(g_DOUT,
      "---------------------------------------\n"
      "SUMMARY\n"
      "---------------------------------------\n"
      "length of list: %u\n"
      "---------------------------------------\n\n",
      superpage_len
  );
  fflush(g_DOUT);
}

void print_free_pb_list(tlh_t* tlh) {
  fprintf(g_DOUT, "========== Free Page Block Lists ==========\n"); 

  size_t pagelist_cnt = 0;
  for (uint32_t i = 0; i < NUM_PAGE_CLASSES; i++) {
    size_t length = get_pbh_list_length(tlh->free_pb_list[i]);
    if (length == 0) continue;
    fprintf(g_DOUT,
        "---------------------------------------\n"
        "FPBL %u| Length: %lu\n"
        "---------------------------------------\n",
        i, length
    );
    print_pbh_list(tlh->free_pb_list[i]);
    pagelist_cnt += length;
  }

  fprintf(g_DOUT,
      "---------------------------------------\n"
      "FPBL SUMMARY\n"
      "---------------------------------------\n"
      "Total #: %lu\n"
      "---------------------------------------\n\n",
      pagelist_cnt
  );
  fflush(g_DOUT);
}

static uint32_t get_block_list_length(void* block) {
  uint32_t length = 0;
  while (block != NULL) {
    length++;
    block = GET_NEXT(block);
  }
  return length;
}

void print_block_list(void* block) {
  uint32_t length = 0;
  while (block != NULL) {
    length++;
    fprintf(g_DOUT, "%p --> ", block);
    if (length % 5 == 0) fprintf(g_DOUT, "\n");
    block = GET_NEXT(block);
  }
  fprintf(g_DOUT, "%p\n", block);
}

#ifdef MALLOC_USE_PAGE_BLOCK_CACHE
void print_pb_cache(pb_cache_t* pb_cache) {
  fprintf(g_DOUT, "========== Page Block Cache ==========\n"); 
  for (int w = 0; w < NUM_PB_CACHE_WAY; w++) {
    fprintf(g_DOUT, "%d: ", w);
    pb_cache_block_t* way = &pb_cache->block[w];
    uint32_t length = 0;
    void* block = way->data;
    while (block != NULL) {
      length++;
      size_t page_id = (size_t)block >> PAGE_SHIFT;
      pbh_t* pbh = (pbh_t*)pagemap_get(page_id);

      fprintf(g_DOUT, "%p(%u) --> ", block, pbh->length);
      if (length % 5 == 0) fprintf(g_DOUT, "\n   ");
      block = GET_NEXT(block);
    }
    fprintf(g_DOUT, "%p\n", block);
  }
  fprintf(g_DOUT, "\n");
}
#else
#define print_pb_cache(c)
#endif

void print_tlh(tlh_t* tlh) {
  fprintf(g_DOUT, "========== Thread Local Heap (T%u) ==========\n",
      tlh->thread_id); 

  fprintf(g_DOUT, "========== Block Lists ==========\n"); 
  for (uint32_t i = 0; i < NUM_CLASSES; i++) {
    blk_list_t* b_list = &tlh->blk_list[i];
    if ((b_list->pbh_list == NULL) &&
        (b_list->free_blk_list == NULL) &&
        (b_list->ptr_to_unused == NULL) &&
        (b_list->cnt_free == 0) &&
        (b_list->cnt_unused == 0)) continue;

    fprintf(g_DOUT,
        "---------------------------------------\n"
        "BLOCK LIST %u\n"
        "---------------------------------------\n"
        "free_blk_list : %p (len:%u)\n"
        "unallocated   : %p\n"
        "cnt_free      : %u\n"
        "cnt_unused    : %u\n"
        "pbh_list      : %p (len:%u)\n"
        "---------------------------------------\n",
        i,
        b_list->free_blk_list, get_block_list_length(b_list->free_blk_list),
        b_list->ptr_to_unused,
        b_list->cnt_free,
        b_list->cnt_unused,
        b_list->pbh_list, get_pbh_list_length(b_list->pbh_list)
        );

    print_pbh_list(b_list->pbh_list);

    fprintf(g_DOUT, "---------------------------------------\n");
    fprintf(g_DOUT, "FREE LIST:\n");
    print_block_list(b_list->free_blk_list);
    fprintf(g_DOUT, "---------------------------------------\n");
  }
  fprintf(g_DOUT, "\n");

  print_free_pb_list(tlh);

  print_pb_cache(&tlh->pb_cache);
  
  print_superpage_list(tlh->sp_list);

  fprintf(g_DOUT, "\n");
}
#endif //MALLOC_DEBUG

