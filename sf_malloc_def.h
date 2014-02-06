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

#ifndef __SF_MALLOC_DEF_H__
#define __SF_MALLOC_DEF_H__

#include <stddef.h>
#include <stdint.h>
#include <limits.h>
#include <sys/mman.h>


////////////////////////////////////////////////////////////////////////////
// Architecture-Dependent Constants
////////////////////////////////////////////////////////////////////////////
// Currently, SFMalloc targets only the 64-bit environment.
#define MACHINE_BIT         64
#define PAGE_SHIFT          12
#define CACHE_LINE_SIZE     64
#define MAX_NUM_THREADS     UINT_MAX


////////////////////////////////////////////////////////////////////////////
// Constant Definitions
////////////////////////////////////////////////////////////////////////////
#define PAGE_SIZE           (1 << PAGE_SHIFT)
#define MAX_SIZE            (8u * PAGE_SIZE)
#define ALIGNMENT           8
#define NUM_CLASSES         60
#define MAX_SMALL_SIZE      1024
#define CLASS_ARRAY_SIZE    ((((1<<PAGE_SHIFT)*8u + 127 + (120<<7)) >> 7) + 1)
#define NUM_PB_CACHE_WAY    8

/* NUM_PAGE_CLASSES should be less than 256. */
#define NUM_PAGE_CLASSES    62
//#define NUM_PAGE_CLASSES    126
//#define NUM_PAGE_CLASSES    254

#if (NUM_PAGE_CLASSES <= 62)
#define SPH_SIZE  PAGE_SIZE
#elif (NUM_PAGE_CLASSES <= 126)
#define SPH_SIZE  (PAGE_SIZE * 2)
#elif (NUM_PAGE_CLASSES <= 254)
#define SPH_SIZE  (PAGE_SIZE * 4)
#endif

#define SUPERPAGE_LEN       (NUM_PAGE_CLASSES + 1)
#define SUPERPAGE_SIZE      (SUPERPAGE_LEN * PAGE_SIZE)
#define DEAD_OWNER          0

#define HUGE_MALLOC_MARK    0x1

#define CACHE_LINE_ALIGN    __attribute__ ((aligned (CACHE_LINE_SIZE)))
#define TLS_MODEL           __attribute__ ((tls_model ("initial-exec")))
//#define TLS_MODEL


////////////////////////////////////////////////////////////////////////////
// Type Definitions
////////////////////////////////////////////////////////////////////////////

//-------------------------------------------------------------------
// SizeMap: mapping from size to size_class and vice versa
//-------------------------------------------------------------------
// NOTE: Values for size classes are from google's tcmalloc.
//       However, index is slightly different.
typedef struct {
  uint8_t class_array[CLASS_ARRAY_SIZE];

  struct {
    // Mapping from size class to max size storable in that class
    uint32_t class_to_size;

    // Mapping from size class to number of pages to allocate at a time
    uint16_t class_to_pages;

    // Mapping from size class to number of blocks in pbh
    uint16_t num_blocks_per_pbh;
  } info[NUM_CLASSES];
} sizemap_t CACHE_LINE_ALIGN;


//-------------------------------------------------------------------
// PageMap: mapping from page number (id) to pbh
//-------------------------------------------------------------------
#define PMAP_BITS             (MACHINE_BIT - PAGE_SHIFT)
#define PMAP_INTERIOR_BIT     (PMAP_BITS / 3)
//#define PMAP_INTERIOR_BIT     ((PMAP_BITS + 2) / 3)
#define PMAP_INTERIOR_LEN     (1 << PMAP_INTERIOR_BIT)
#define PMAP_LEAF_BIT         (PMAP_BITS - 2 * PMAP_INTERIOR_BIT)
#define PMAP_LEAF_LEN         (1 << PMAP_LEAF_BIT)

typedef struct {
  void* val[PMAP_LEAF_LEN];
} pagemap_leaf_t;

typedef struct {
  pagemap_leaf_t* leaf[PMAP_INTERIOR_LEN];
} pagemap_node_t;

typedef struct {
  pagemap_node_t* node[PMAP_INTERIOR_LEN];
} pagemap_t CACHE_LINE_ALIGN;


//-------------------------------------------------------------------
// Type for Superpage and Superpage Header (SPH)
//-------------------------------------------------------------------
// Superpage is a unit of mmap/munmap.
// Superpage and SPH are used interchangeably in the code.
typedef union {
  struct {
    uint32_t owner_id;
    uint32_t finish_mark;
  };
  uint64_t with;
} ownermark_t __attribute__ ((aligned(8)));

#define NONE            0
#define DO_NOT_FINISH   1

typedef struct sph {
  struct sph* next;            // next pointer in linked list
  struct sph* prev;            // prev pointer in linked list
  size_t      start_page;      // starting page number
  volatile ownermark_t omark;  // owner_id + finish_mark
  void*       remote_pb_list;  // remote list for large blocks 
  uint32_t    hazard_mark;
} sph_t;


//-------------------------------------------------------------------
// Type for Hazard Pointer
//-------------------------------------------------------------------
typedef struct hazard_ptr hazard_ptr_t;
struct hazard_ptr {
  hazard_ptr_t*     next;
  sph_t*            node;
  volatile uint32_t active;
  char pad[CACHE_LINE_SIZE-(sizeof(void*)*2 + sizeof(uint32_t))];
};


//-------------------------------------------------------------------
// Type for Page Block Header (PBH)
//-------------------------------------------------------------------
#define PBH_SIZE    sizeof(pbh_t)
typedef union {
  struct {
    uint32_t head;  // block index
    uint32_t cnt;   // number of remotely freed blocks
  };
  uint64_t together;
} remote_list_t __attribute__ ((aligned(8)));

typedef struct pbh pbh_t  CACHE_LINE_ALIGN;
struct pbh {
  pbh_t*   next;          // next pointer in linked list
  pbh_t*   prev;          // prev pointer in linked list
  size_t   start_page;    // starting page number

  uint8_t  length;        // number of pages in pbh
  uint8_t  index;         // position(index) in superpage
  uint8_t  sizeclass;     // size-calss for small blocks
  uint8_t  status;        // status of the pbh
  uint32_t cnt_free;      // number of free blocks in free list
  uint32_t cnt_unused;    // number of unused free blocks    
  uint16_t page_color;    // for page coloring
  uint16_t block_color;   // for block coloring

  void*    free_list;     // pointer to the first free block
  void*    unallocated;   // pointer to the first unused free block

  volatile remote_list_t  remote_list;   // for remote free
};

enum {
  PBH_ON_FREE_LIST,
  PBH_IN_USE,
  PBH_AGAINST_FALSE_SHARING
};


//-------------------------------------------------------------------
// Block List
//-------------------------------------------------------------------
// We pack this structure as 32B to reduce cache miss.
typedef struct {
  void*    free_blk_list;   // free block list for a specific size-class
  void*    ptr_to_unused;   // pointer to the unallocated chunk
  uint32_t cnt_free;        // length of free_list
  uint32_t cnt_unused;      // number of unallocated blocks
  pbh_t*   pbh_list;        // list of PBs that are used in this class
} blk_list_t;


//-------------------------------------------------------------------
// Page Block Cache
//-------------------------------------------------------------------
// This is an 8-way associative cache using pseudo-lru replacement algorithm.
typedef char v8qi __attribute__ ((vector_size (8)));

typedef union {
  v8qi v;
  char e[8];
} char8_t;

#ifdef MALLOC_USE_PAGE_BLOCK_CACHE
typedef struct {
  void*  data;
  size_t length;
} pb_cache_block_t;

typedef struct {
  pb_cache_block_t block[NUM_PB_CACHE_WAY];
  char8_t          tag;
  uint8_t          state;   // LRU state
} pb_cache_t;

#define NUM_LRU_TABLE_ENTRY   128
const uint8_t g_lru_table[NUM_LRU_TABLE_ENTRY] CACHE_LINE_ALIGN = {
  0, 4, 2, 4, 0, 6, 2, 6, 1, 4, 2, 4, 1, 6, 2, 6, 
  0, 4, 3, 4, 0, 6, 3, 6, 1, 4, 3, 4, 1, 6, 3, 6, 
  0, 5, 2, 5, 0, 6, 2, 6, 1, 5, 2, 5, 1, 6, 2, 6, 
  0, 5, 3, 5, 0, 6, 3, 6, 1, 5, 3, 5, 1, 6, 3, 6, 
  0, 4, 2, 4, 0, 7, 2, 7, 1, 4, 2, 4, 1, 7, 2, 7, 
  0, 4, 3, 4, 0, 7, 3, 7, 1, 4, 3, 4, 1, 7, 3, 7, 
  0, 5, 2, 5, 0, 7, 2, 7, 1, 5, 2, 5, 1, 7, 2, 7, 
  0, 5, 3, 5, 0, 7, 3, 7, 1, 5, 3, 5, 1, 7, 3, 7
};

typedef struct {
  uint8_t mask;
  uint8_t set_bit;
} way_table_t;

const way_table_t g_way_table[NUM_PB_CACHE_WAY] = {
  {0x74, 0xB},
  {0x74, 0x3},
  {0x6C, 0x11},
  {0x6C, 0x1},
  {0x5A, 0x24},
  {0x5A, 0x4},
  {0x3A, 0x40},
  {0x3A, 0x0}
};
#endif


//-------------------------------------------------------------------
// Thread Local Heap (TLH)
//-------------------------------------------------------------------
// The size of tlh_t should be less than or equal to 2KB.
typedef struct {
  blk_list_t    blk_list[NUM_CLASSES];          // Block Lists
  pbh_t*        free_pb_list[NUM_PAGE_CLASSES]; // Free Page Block Lists
  sph_t*        sp_list;        // Superpage List
  hazard_ptr_t* hazard_ptr;     // PTR to Hazard Pointer
#ifdef MALLOC_USE_PAGE_BLOCK_CACHE
  pb_cache_t    pb_cache;       // Page Block Cache
#endif
  uint32_t      thread_id;

#ifdef MALLOC_USE_PAGE_COLORING
  char8_t       pagecolor_cache;
  uint8_t       pagecolor_state;
#endif
} tlh_t CACHE_LINE_ALIGN;



////////////////////////////////////////////////////////////////////////////
// Macro Functions
////////////////////////////////////////////////////////////////////////////
/* s should the pointer to the superpage header. */
#define GET_FIRST_PBH(s)  ((pbh_t*)(s) + 1)

#define GET_NEXT(p)       (void*)(*(uintptr_t*)(p))
#define SET_NEXT(p,n)     *(uintptr_t*)(p) = (uintptr_t)(n)
#define GET_PAGE_LEN(s)   \
  (((s) >> PAGE_SHIFT) + (((s) & (PAGE_SIZE - 1)) != 0 ? 1 : 0))

#define MIN(a,b)  (((a) < (b)) ? (a) : (b))
#define MAX(a,b)  (((a) > (b)) ? (a) : (b))

#define LIKELY(x)   __builtin_expect(x, 1)
#define UNLIKELY(x) __builtin_expect(x, 0)
#define TID()       l_tlh.thread_id


#define CRASH(fmt,...)    \
  fprintf(stderr, "%s:%d: "fmt, __FILE__, __LINE__, __VA_ARGS__); \
  exit(EXIT_FAILURE)

#define HANDLE_ERROR(msg) \
  do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define HANDLE_ERROR_EN(en,msg) \
  do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0) 


#ifdef MALLOC_DEBUG
#define LOG(fmt,...)    fprintf(stdout,fmt,__VA_ARGS__); fflush(stdout)
#else  //MALLOC_DEBUG
#define LOG(fmt,...)
#endif //MALLOC_DEBUG

#ifdef MALLOC_DEBUG_DETAIL
#define LOG_D(fmt,...)    fprintf(stdout,fmt,__VA_ARGS__); fflush(stdout)
#else  //MALLOC_DEBUG_DETAIL
#define LOG_D(fmt,...)
#endif //MALLOC_DEBUG_DETAIL


#endif //__SF_MALLOC_DEF_H__

