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

#if GI_LEVEL_NOT_YET_1_8
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
#define VREF(v,i)         (SCM_VELTS (v)[i])
#define EQ                gh_eq_p
#define CONS              gh_cons
#define CAR               gh_car
#define CDR               gh_cdr
#define SETCAR            gh_set_car_x
#define SETCDR            gh_set_cdr_x
#define EVAL_STRING       gh_eval_str
#define APPLY             gh_apply
#define LISTIFY           gh_list
#define NEWCELL_X(svar)   SCM_NEWCELL (svar)
#else  /* !GI_LEVEL_NOT_YET_1_8 */
#define BOOLEANP          scm_is_bool
#define EXACTP(x)         scm_is_true (scm_exact_p (x))
#define NULLP             scm_is_null
#define PAIRP             scm_is_pair
#define STRINGP           scm_is_string
#define SYMBOLP           scm_is_symbol
#define PROCEDUREP(x)     scm_is_true (scm_procedure_p (x))
#define BOOLEAN           scm_from_bool
#define NUM_INT           scm_from_int
#define NUM_ULONG         scm_from_ulong
#define SYMBOL            scm_from_locale_symbol
#define STRING            scm_from_locale_string
#define BSTRING           scm_from_locale_stringn
#define CHARACTER         SCM_MAKE_CHAR
#define C_INT             scm_to_int
#define VECTOR_LEN        scm_c_vector_length
#define VREF              SCM_SIMPLE_VECTOR_REF
#define EQ                scm_is_eq
#define CONS              scm_cons
#define CAR               scm_car
#define CDR               scm_cdr
#define SETCAR            scm_set_car_x
#define SETCDR            scm_set_cdr_x
#define EVAL_STRING       scm_c_eval_string
#define APPLY             scm_apply_0
#define LISTIFY           scm_list_n
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
#define RETURN_FALSE()                        return SCM_BOOL_F
#define RETURN_UNSPECIFIED()                  return SCM_UNSPECIFIED

#define ASSERT(what,expr,msg)  SCM_ASSERT ((expr), what, msg, FUNC_NAME)
#define ASSERT_STRING(n,arg)  ASSERT (arg, STRINGP (arg), SCM_ARG ## n)
#define ASSERT_EXACT(n,arg)  ASSERT (arg, EXACTP (arg), SCM_ARG ## n)

#if GI_LEVEL_NOT_YET_1_8
#define ASSERT_EXACT_NON_NEGATIVE_COPY(n,svar,cvar)     \
  SCM_VALIDATE_INUM_MIN_COPY (n, svar, 0, cvar)
#define VALIDATE_EXACT_0_UP_TO_N_COPY(n,svar,hi,cvar)   \
  SCM_VALIDATE_INUM_RANGE_COPY (n, svar, 0, hi, cvar)
#define VALIDATE_KEYWORD(n,svar)                \
  SCM_VALIDATE_KEYWORD (n, svar)
#else
#define ASSERT_EXACT_NON_NEGATIVE_COPY(n,svar,cvar)  do \
    {                                                   \
      ASSERT_EXACT (n, svar);                           \
      cvar = C_INT (svar);                              \
      SCM_ASSERT_RANGE (n, svar, !PROB (cvar));         \
    }                                                   \
  while (0)
#define VALIDATE_EXACT_0_UP_TO_N_COPY(n,svar,hi,cvar)  do       \
    {                                                           \
      ASSERT_EXACT (n, svar);                                   \
      cvar = C_INT (svar);                                      \
      SCM_ASSERT_RANGE (n, svar, !PROB (cvar) && hi > cvar);    \
    }                                                           \
  while (0)
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
