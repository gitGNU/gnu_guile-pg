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

#if defined SCM_MAJOR_VERSION && defined SCM_MINOR_VERSION
#define GI_LEVEL  ((SCM_MAJOR_VERSION << 8) + SCM_MINOR_VERSION)
#else
#define GI_LEVEL  0x0104                /* Guile 1.4.x */
#endif
#define GI_LEVEL_NOT_YET_1_8  (GI_LEVEL < 0x0108)

#include <guile/gh.h>
#define BOOLEANP          gh_boolean_p
#define EXACTP            gh_exact_p
#define NULLP             gh_null_p
#define PAIRP             gh_pair_p
#define STRINGP           gh_string_p
#define SYMBOLP           gh_symbol_p
#define PROCEDUREP        gh_procedure_p
#define BOOLEAN           gh_bool2scm
#define NUM_INT           gh_int2scm
#define NUM_ULONG         gh_ulong2scm
#define SYMBOL            gh_symbol2scm
#define STRING            gh_str02scm
#define BSTRING           gh_str2scm
#define CHARACTER         gh_char2scm
#define C_INT             gh_scm2int
#define VECTOR_LEN        gh_vector_length
#define EQ                gh_eq_p
#define CONS              gh_cons
#define CAR               gh_car
#define CDR               gh_cdr
#define SETCAR            gh_set_car_x
#define SETCDR            gh_set_cdr_x
#define EVAL_STRING       gh_eval_str
#define APPLY             gh_apply
#define LISTIFY           gh_list

#define DEFSMOB(tagvar,name,m,f,p)                              \
  tagvar = scm_make_smob_type_mfpe (name, 0, m, f, p, NULL)

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
#define RETURN_FALSE()                        return SCM_BOOL_F
#define RETURN_UNSPECIFIED()                  return SCM_UNSPECIFIED

#define ASSERT(what,expr,msg)  SCM_ASSERT ((expr), what, msg, FUNC_NAME)
#define ASSERT_STRING(n,arg)  ASSERT (arg, SCM_STRINGP (arg), SCM_ARG ## n)

/* These are provisionary.  We need a better way for Guile 2.x.  */
#ifndef SCM_ROCHARS
#define SCM_ROCHARS(x)   SCM_CHARS (x)
#endif
#ifndef SCM_ROLENGTH
#define SCM_ROLENGTH(x)  SCM_LENGTH (x)
#endif
#ifndef SCM_ROUCHARS
#define SCM_ROUCHARS(x)  ((unsigned char *) SCM_ROCHARS (x))
#endif

/* Coerce a string that is to be used in contexts where the extracted C
   string is expected to be zero-terminated and is read-only.  We check
   this condition precisely instead of simply coercing all substrings,
   to avoid waste for those substrings that may in fact already satisfy
   the condition.  Callers should extract w/ ROZT.  */
#define ROZT_X(x)                                       \
  if (SCM_ROCHARS (x) [SCM_ROLENGTH (x)])               \
    x = BSTRING (SCM_ROCHARS (x), SCM_ROLENGTH (x))

#define ROZT(x)  (SCM_ROCHARS (x))

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
