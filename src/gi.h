/* gi.h

   Copyright (C) 2004, 2005, 2006, 2008, 2009, 2011, 2012 Thien-Thi Nguyen

   This file is part of Guile-PG.

   Guile-PG is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   Guile-PG is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Guile-PG.  If not, see <http://www.gnu.org/licenses/>.  */

/*
 * always
 */

#ifndef _GI_H_
#define _GI_H_

#include <libguile.h>
#include "snuggle/level.h"
#include "snuggle/humdrum.h"

#define GI_LEVEL_NOT_YET_1_8  ! GI_LEVEL (1, 8)

#if GI_LEVEL_NOT_YET_1_8
#include <guile/gh.h>
#define CHARACTER         gh_char2scm
#define NEWCELL_X(svar)   SCM_NEWCELL (svar)
#else  /* !GI_LEVEL_NOT_YET_1_8 */
#define CHARACTER         SCM_MAKE_CHAR
#define NEWCELL_X(svar)   svar = scm_cell (0, 0)
#endif /* !GI_LEVEL_NOT_YET_1_8 */

#if GI_LEVEL_NOT_YET_1_8

#define DEFSMOB(tagvar,name,m,f,p)                              \
  tagvar = scm_make_smob_type_mfpe (name, 0, m, f, p, NULL)

#define GCMALLOC(sz,what)    scm_must_malloc (sz, what)
#define GCFREE(ptr,what)     scm_must_free (ptr)
#define GCRV(ptr)            sizeof (*ptr)

#else  /* !GI_LEVEL_NOT_YET_1_8 */

#define DEFSMOB(tagvar,name,m,f,p)  do          \
    {                                           \
      tagvar = scm_make_smob_type (name, 0);    \
      scm_set_smob_mark (tagvar, m);            \
      scm_set_smob_free (tagvar, f);            \
      scm_set_smob_print (tagvar, p);           \
    }                                           \
  while (0)

#define GCMALLOC(sz,what)    scm_gc_malloc (sz, what)
#define GCFREE(ptr,what)     scm_gc_free (ptr, sizeof (*ptr), what)
#define GCRV(ptr)            0

#endif  /* !GI_LEVEL_NOT_YET_1_8 */

/*
 * backward (sometimes foresight was incomplete)
 */

#ifdef HAVE_GUILE_MODSUP_H
#include <guile/modsup.h>
#else  /* !defined HAVE_GUILE_MODSUP_H */

#define GH_DEFPROC(fname, primname, req, opt, var, arglist, docstring) \
  SCM_SNARF_HERE (static SCM fname arglist;)                           \
  SCM_DEFINE (fname, primname, req, opt, var, arglist, docstring)

#define GH_MODULE_LINK_FUNC(module_name, fname_frag, module_init_func)  \
void                                                                    \
scm_init_ ## fname_frag ## _module (void);                              \
void                                                                    \
scm_init_ ## fname_frag ## _module (void)                               \
{                                                                       \
  /* Make sure strings(1) finds module name at bol.  */                 \
  static const char modname[] = "\n" module_name;                       \
  scm_register_module_xxx (1 + modname, module_init_func);              \
}

#endif  /* !defined HAVE_GUILE_MODSUP_H */

/*
 * abstractions
 */

#if GI_LEVEL_NOT_YET_1_8
#define NOINTS()   SCM_DEFER_INTS
#define INTSOK()   SCM_ALLOW_INTS
#else
#define NOINTS()
#define INTSOK()
#endif

#define GIVENP(x)          (! SCM_UNBNDP (x))
#define NOT_FALSEP(x)      (SCM_NFALSEP (x))
#define MEMQ(k,l)          (NOT_FALSEP (scm_memq ((k), (l))))

#define DEFAULT_FALSE(maybe,yes)  ((maybe) ? (yes) : SCM_BOOL_F)
#define RETURN_TRUE()                         return SCM_BOOL_T
#define RETURN_FALSE()                        return SCM_BOOL_F
#define RETURN_UNSPECIFIED()                  return SCM_UNSPECIFIED

#define ASSERT(what,expr,msg)  SCM_ASSERT ((expr), what, msg, FUNC_NAME)
#define ASSERT_STRING(n,arg)  ASSERT (arg, STRINGP (arg), SCM_ARG ## n)
#define ASSERT_INTEGER(n,arg)  ASSERT (arg, INTEGERP (arg), SCM_ARG ## n)

#if GI_LEVEL_NOT_YET_1_8
#define VALIDATE_NNINT_COPY(n,svar,cvar)     \
  SCM_VALIDATE_INUM_MIN_COPY (n, svar, 0, cvar)
#define VALIDATE_NNINT_RANGE_COPY(n,svar,hi,cvar)   \
  SCM_VALIDATE_INUM_RANGE_COPY (n, svar, 0, hi, cvar)
#define VALIDATE_KEYWORD(n,svar)                \
  SCM_VALIDATE_KEYWORD (n, svar)
#else
#define VALIDATE_NNINT_MORE_COPY(n,svar,cvar,more)      \
    {                                                   \
      ASSERT_INTEGER (n, svar);                         \
      cvar = C_INT (svar);                              \
      SCM_ASSERT_RANGE (n, svar, !PROB (cvar) && more); \
    }                                                   \
  while (0)
#define VALIDATE_NNINT_COPY(n,svar,cvar)        \
  VALIDATE_NNINT_MORE_COPY (n, svar, cvar, 1)
#define VALIDATE_NNINT_RANGE_COPY(n,svar,hi,cvar)       \
  VALIDATE_NNINT_MORE_COPY (n, svar, cvar, hi > cvar)
#define VALIDATE_KEYWORD(n,svar)                        \
  ASSERT (svar, scm_is_keyword (svar), SCM_ARG ## n)
#endif

/* For some versions of Guile, (make-string (ash 1 24)) => "".

   That is, ‘make-string’ doesn't fail, but lengths past (1- (ash 1 24))
   overflow an internal limit and silently return an incorrect value.
   We hardcode this limit here for now.  */
#define MAX_NEWSTRING_LENGTH ((1 << 24) - 1)

#define SMOBDATA(obj)  ((void *) SCM_SMOB_DATA (obj))

#define PCHAIN(...)  (LISTIFY (__VA_ARGS__, SCM_UNDEFINED))

#define ERROR(blurb, ...)  SCM_MISC_ERROR (blurb, PCHAIN (__VA_ARGS__))
#define MEMORY_ERROR()     SCM_MEMORY_ERROR
#define SYSTEM_ERROR()     SCM_SYSERROR

/* Write a C byte array (pointer + len) to a Scheme port.  */
#define WBPORT(scmport,cp,clen)  scm_lfwrite (cp, clen, scmport)

#define PRIMPROC             GH_DEFPROC
#define PERMANENT            scm_permanent_object
#define MOD_INIT_LINK_THUNK  GH_MODULE_LINK_FUNC

#endif /* _GI_H_ */

/* gi.h ends here */
