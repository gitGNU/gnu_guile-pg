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
#include "snuggle/defsmob.h"
#include "snuggle/modsup.h"

#if !GI_LEVEL_1_8
#define CHARACTER         gh_char2scm
#define NEWCELL_X(svar)   SCM_NEWCELL (svar)
#else  /* GI_LEVEL_1_8 */
#define CHARACTER         SCM_MAKE_CHAR
#define NEWCELL_X(svar)   svar = scm_cell (0, 0)
#endif /* GI_LEVEL_1_8 */

/*
 * abstractions
 */

#if !GI_LEVEL_1_8
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

#if !GI_LEVEL_1_8
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

#endif /* _GI_H_ */

/* gi.h ends here */
