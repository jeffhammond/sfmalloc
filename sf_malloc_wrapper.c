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
#include <pthread.h>
#include <assert.h>

#define __USE_GNU
#include <dlfcn.h>

#include "sf_malloc_internal.h"


typedef int (*pthread_create_fpt)(pthread_t *, const pthread_attr_t *, void *(*)(void*), void *);
typedef void (*pthread_exit_fpt)(void *);
typedef void *(*start_fpt)(void *);

typedef struct {
  start_fpt start_fun;
  void *arg;
} wrapper_arg_t; 


/* function pointers */
static pthread_create_fpt thread_create = NULL;


/* Take a handle of "pthread_create" */
static void dlsym_pthread_create() {
  char *error;
  dlerror();  // Clear any existing error

  thread_create = (pthread_create_fpt)dlsym(RTLD_NEXT, "pthread_create");
  if ((error = dlerror()) != NULL) {
    fprintf(stderr, "%s\n", error);
    exit(EXIT_FAILURE);
  }
}


/* Wrapper function for start_routine of pthread_create() */
static void *wrapper(void *warg) {
  // Initialize malloc library for this thread
  sf_malloc_thread_init();

  // Get the routine and argument for the pthread_create
  start_fpt start_routine = ((wrapper_arg_t *)warg)->start_fun;
  void *arg = ((wrapper_arg_t *)warg)->arg;

  free(warg);

  void *result = start_routine(arg);

  return result;
}


/* Wrapper pthread_create() to call the wrapper function of start_routine */
int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                   void *(*start_routine)(void *), void *arg) {
  if (thread_create == NULL) {
    dlsym_pthread_create();
  }

  wrapper_arg_t *warg = (wrapper_arg_t *)malloc(sizeof(wrapper_arg_t));
  if (warg == NULL) {
    fprintf(stderr, "malloc for wrapper_arg_t failed\n");
    exit(EXIT_FAILURE);
  }

  // Call the real pthread_create()
  warg->start_fun = start_routine;
  warg->arg = arg;
  int result = thread_create(thread, attr, wrapper, warg);

  return result;
}

