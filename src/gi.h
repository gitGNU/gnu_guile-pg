/* gi.h

   Copyright (C) 2004, 2005, 2006, 2008, 2009, 2011 Thien-Thi Nguyen

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
   along with Guile-PG; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA  */

/*
 * always
 */

#ifndef _GI_H_
#define _GI_H_

#include <libguile.h>
#include <guile/gh.h>

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

#define GH_STONED(obj) \
  scm_permanent_object (obj)

#endif  /* !defined HAVE_GUILE_MODSUP_H */

/*
 * forward (sometimes kids make messes for others to clean up)
 */

#ifdef HAVE_SCM_GC_PROTECT_OBJECT
#define scm_protect_object(x)  (scm_gc_protect_object (x))
#endif

/*
 * abstractions
 */

#define NOINTS()   SCM_DEFER_INTS
#define INTSOK()   SCM_ALLOW_INTS

#define GIVENP(x)          (! SCM_UNBNDP (x))
#define NOT_FALSEP(x)      (SCM_NFALSEP (x))

#define DEFAULT_FALSE(maybe,yes)  ((maybe) ? (yes) : SCM_BOOL_F)
#define RETURN_FALSE()                        return SCM_BOOL_F
#define RETURN_UNSPECIFIED()                  return SCM_UNSPECIFIED

#define ASSERT(what,expr,msg)  SCM_ASSERT ((expr), what, msg, FUNC_NAME)
#define ASSERT_STRING(n,arg)  ASSERT (arg, SCM_STRINGP (arg), SCM_ARG ## n)

/* Coerce a string that is to be used in contexts where the extracted C
   string is expected to be zero-terminated and is read-only.  We check
   this condition precisely instead of simply coercing all substrings,
   to avoid waste for those substrings that may in fact already satisfy
   the condition.  Callers should extract w/ ROZT.  */
#define ROZT_X(x)                                       \
  if (SCM_ROCHARS (x) [SCM_ROLENGTH (x)])               \
    x = gh_str2scm (SCM_ROCHARS (x), SCM_ROLENGTH (x))

#define ROZT(x)  (SCM_ROCHARS (x))

/* For some versions of Guile, (make-string (ash 1 24)) => "".

   That is, `make-string' doesn't fail, but lengths past (1- (ash 1 24))
   overflow an internal limit and silently return an incorrect value.
   We hardcode this limit here for now.  */
#define MAX_NEWSTRING_LENGTH ((1 << 24) - 1)

#define SMOBDATA(obj)  ((void *) SCM_SMOB_DATA (obj))

#define PCHAIN(...)  (gh_list (__VA_ARGS__, SCM_UNDEFINED))

#define ERROR(blurb, ...)  SCM_MISC_ERROR (blurb, PCHAIN (__VA_ARGS__))
#define MEMORY_ERROR()     SCM_MEMORY_ERROR
#define SYSTEM_ERROR()     SCM_SYSERROR

/* Write a C byte array (pointer + len) to a Scheme port.  */
#define WBPORT(scmport,cp,clen)  scm_lfwrite (cp, clen, scmport)

#define PRIMPROC             GH_DEFPROC
#define PERMANENT            GH_STONED
#define MOD_INIT_LINK_THUNK  GH_MODULE_LINK_FUNC

#endif /* _GI_H_ */

/* gi.h ends here */
