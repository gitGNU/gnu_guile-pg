/* libpostgres.h

   Copyright (C) 1999,2000 Ian Grant
   Copyright (C) 2003,2004,2005 Thien-Thi Nguyen

   This file is part of Guile-PG.

   Guile-PG is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   Guile-PG is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Guile-PG; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA  */

/* We fudge this. I wonder why only libguile sources can access this?  */
#define SCM_SYSCALL(line) line

#define IFNULL(x,y) ((x) == NULL ? (y) : (x))

typedef struct
{
  SCM          notice;        /* port to send notices to */
  SCM          client;
  int          count;         /* which dbconn is this? */
  PGconn      *dbconn;        /* Postgres data structure */
  FILE        *fptrace;       /* The current trace stream */
} xc_t;

extern int   xc_p (SCM obj);
extern xc_t *xc_unbox (SCM obj);
extern int   xc_display (SCM exp, SCM port, scm_print_state *pstate);

extern void init_libpostgres_lo (void);

/* SCM_DEFINE alone doesn't declare static, so we prefix that decl.  */
#define PG_DEFINE(FNAME, PRIMNAME, REQ, OPT, VAR, ARGLIST, DOCSTRING) \
  SCM_SNARF_HERE(static SCM FNAME ARGLIST;) \
  SCM_DEFINE (FNAME, PRIMNAME, REQ, OPT, VAR, ARGLIST, DOCSTRING)

/* string munging */

/* Coerce a string that is to be used in contexts where the extracted C
   string is expected to be zero-terminated and is read-only.  We check
   this condition precisely instead of simply coercing all substrings,
   to avoid waste for those substrings that may in fact already satisfy
   the condition.  Callers should extract w/ ROZT.  */
#define ROZT_X(x)                                               \
  if (SCM_ROCHARS (x) [SCM_ROLENGTH (x)] != '\0')               \
    x = scm_makfromstr (SCM_ROCHARS (x), SCM_ROLENGTH (x), 0)

#define ROZT(x)  (SCM_ROCHARS (x))

/* other abstractions */

#define NOT_FALSEP(x)      (SCM_NFALSEP (x))
#define EXACTLY_FALSEP(x)  (SCM_FALSEP (x))
#define EXACTLY_TRUEP(x)   (gh_eq_p ((x), SCM_BOOL_T))

#define VALIDATE_CONNECTION_UNBOX_DBCONN(n,arg,cvar)            \
  do {                                                          \
    SCM_ASSERT (xc_p (arg), arg, SCM_ARG ## n, FUNC_NAME);      \
    cvar = xc_unbox (arg)->dbconn;                              \
  } while (0)

/* libpostgres.h ends here */
