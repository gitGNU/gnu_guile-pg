/*  Guile-pg - A Guile interface to PostgreSQL
    Copyright (C) 1999-2000, 2003 The Free Software Foundation

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software Foundation,
    Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA  */

/* Author: Ian Grant <Ian.Grant@cl.cam.ac.uk> */

/* We fudge this. I wonder why only libguile sources can access this? */
#define SCM_SYSCALL(line) line

#define IFNULL(x,y) ((x) == NULL ? (y) : (x))

typedef struct _scm_extended_dbconn {
  SCM          notice;        /* port to send notices to */
  SCM          client;
  int          count;         /* which dbconn is this? */
  PGconn      *dbconn;        /* Postgres data structure */
  FILE        *fptrace;       /* The current trace stream */
} scm_extended_dbconn;

typedef struct _scm_extended_result {
    SCM          conn;          /* Connection */
    int          count;         /* which result is this? */
    PGresult    *result;        /* Postgres result structure */
} scm_extended_result;

extern int                  guile_pg_sec_p (SCM obj);
extern scm_extended_dbconn *guile_pg_sec_unbox (SCM obj);

extern void init_libpostgres_lo (void);

/* SCM_DEFINE alone doesn't declare static, so we prefix that decl.  */
#define PG_DEFINE(FNAME, PRIMNAME, REQ, OPT, VAR, ARGLIST, DOCSTRING) \
  SCM_SNARF_HERE(static SCM FNAME ARGLIST;) \
  SCM_DEFINE (FNAME, PRIMNAME, REQ, OPT, VAR, ARGLIST, DOCSTRING)

/* string munging */

/* Return a string that is to be used in contexts where the extracted C
   string is expected to be zero-terminated and is read-only.  We check
   this condition precisely instead of simply coercing all substrings,
   to avoid waste for those substrings that may in fact already satisfy
   the condition.  Callers should extract w/ ROZT.  */
#define ROZT_X(x)                                               \
  if (SCM_ROCHARS (x) [SCM_ROLENGTH (x)] != '\0')               \
    x = scm_makfromstr (SCM_ROCHARS (x), SCM_ROLENGTH (x), 0)

#define ROZT(x)  (SCM_ROCHARS (x))

/* libpostgres.h ends here */
