/* libpostgres.c

   Copyright (C) 1999 Ian Grant
   Copyright (C) 2002,2003,2004,2005 Thien-Thi Nguyen

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

#include <config.h>

#include <stdio.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#ifndef HAVE_TMPFILE
#include "tmpfile.h"
#define tmpfile guile_pg_tmpfile
#endif

#include <libguile.h>
#include <guile/gh.h>

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

#ifdef INVALIDOID_HEADER                /* PostgreSQL folks <- slack */
#include INVALIDOID_HEADER
#endif

#include "libpostgres.h"

#define BUF_LEN 256

#ifdef GC_DEBUG
#define GC_PRINT(x) x
#else
#define GC_PRINT(x) (void)0
#endif

#ifdef INIT_DEBUG
#define INIT_PRINT(x) x
#else
#define INIT_PRINT(x) (void)0
#endif


/*
 * support
 */

static SCM strip_newlines (char *str);
void scm_init_database_postgres_module (void);

typedef struct _smob_tag {
  long  type_tag; /* type tag */
  int   count;
} smob_tag;

static smob_tag pg_conn_tag, pg_result_tag;

typedef struct _pgrs { ExecStatusType n; char *s; SCM sym; } pgrs_t;

#define _PGRES(s) { s, #s, SCM_BOOL_F } /* `sym' field set below */
static pgrs_t pgrs[] = {
  _PGRES (PGRES_TUPLES_OK),
  _PGRES (PGRES_EMPTY_QUERY),
  _PGRES (PGRES_COMMAND_OK),
  _PGRES (PGRES_COPY_OUT),
  _PGRES (PGRES_COPY_IN),
  _PGRES (PGRES_BAD_RESPONSE),
  _PGRES (PGRES_NONFATAL_ERROR),
  _PGRES (PGRES_FATAL_ERROR)
};
#undef _PGRES

static int pgrs_count = sizeof (pgrs) / sizeof (pgrs_t);


/*
 * boxing, unboxing, gc functions
 */
int
xc_p (SCM obj)
{
  return SCM_SMOB_PREDICATE (pg_conn_tag.type_tag, obj);
}

xc_t *
xc_unbox (SCM obj)
{
  return ((xc_t *) gh_cdr (obj));
}

#define ASSERT_CONNECTION(n,arg) \
  SCM_ASSERT (xc_p (arg), arg, SCM_ARG ## n, FUNC_NAME)

#define CONN_NOTICE(conn)   (xc_unbox (conn)->notice)
#define CONN_CLIENT(conn)   (xc_unbox (conn)->client)
#define CONN_FPTRACE(conn)  (xc_unbox (conn)->fptrace)

static SCM
xc_box (xc_t *xc)
{
  SCM z;

  SCM_ENTER_A_SECTION;
  SCM_NEWCELL (z);
  SCM_SETCDR (z, (SCM) xc);
  SCM_SETCAR (z, pg_conn_tag.type_tag);
  SCM_EXIT_A_SECTION;

  return z;
}

int
xc_display (SCM exp, SCM port, scm_print_state *pstate)
{
  xc_t *xc = xc_unbox (exp);
  char *dbstr = PQdb (xc->dbconn);
  char *hoststr = PQhost (xc->dbconn);
  char *portstr = PQport (xc->dbconn);
  char *optionsstr = PQoptions (xc->dbconn);

  /* A port without a host is misleading.  */
  if (! hoststr)
    portstr = NULL;

  scm_puts ("#<PG-CONN:", port);
  scm_intprint (xc->count, 10, port); scm_putc (':', port);
  scm_puts (IFNULL (dbstr, "?"), port); scm_putc (':', port);
  scm_puts (IFNULL (hoststr, ""), port); scm_putc (':', port);
  scm_puts (IFNULL (portstr, ""), port); scm_putc (':', port);
  scm_puts (IFNULL (optionsstr, ""), port);
  scm_puts (">", port);

  return 1;
}

static SCM
xc_mark (SCM obj)
{
  xc_t *xc = xc_unbox (obj);

  GC_PRINT (fprintf (stderr, "marking PG-CONN %d\n", xc->count));
  scm_gc_mark (xc->notice);
  return xc->client;
}

static scm_sizet
xc_free (SCM obj)
{
  xc_t *xc = xc_unbox (obj);
  scm_sizet size = sizeof (xc_t);

  GC_PRINT (fprintf (stderr, "sweeping PG-CONN %d\n", xc->count));

  /* close connection to postgres */
  if (xc->dbconn)
    (void) PQfinish (xc->dbconn);

  /* free this object itself */
  free (xc);

  /* set extension data to NULL */
  SCM_SETCDR (obj, (SCM)NULL);

  return size;
}

typedef struct  /* extended result */
{
  SCM          conn;          /* Connection */
  int          count;         /* Which result is this? */
  PGresult    *result;        /* Postgres result structure */
} xr_t;

static int
xr_p (SCM obj)
{
  return SCM_SMOB_PREDICATE (pg_result_tag.type_tag, obj);
}

static xr_t *
xr_unbox (SCM obj)
{
  return ((xr_t*) gh_cdr (obj));
}

#define RESULT(x)  (xr_unbox (x)->result)

#define VALIDATE_RESULT_UNBOX2(pos,arg,cvar1,cvar2)             \
  do {                                                          \
    SCM_ASSERT (xr_p (arg), arg, SCM_ARG ## pos, FUNC_NAME);    \
    cvar1 = xr_unbox (arg);                                     \
    cvar2 = cvar1->result;                                      \
  } while (0)

static SCM
xr_box (xr_t *xr)
{
  SCM z;

  SCM_ENTER_A_SECTION;
  SCM_NEWCELL (z);
  SCM_SETCDR (z, (SCM) xr);
  SCM_SETCAR (z, pg_result_tag.type_tag);
  SCM_EXIT_A_SECTION;

  return z;
}

static int
xr_display (SCM exp, SCM port, scm_print_state *pstate)
{
  xr_t *xr = xr_unbox (exp);
  ExecStatusType status;
  int ntuples = 0;
  int nfields = 0;
  int pgrs_index;

  SCM_DEFER_INTS;
  status = PQresultStatus (xr->result);
  if (status == PGRES_TUPLES_OK)
    {
      ntuples = PQntuples (xr->result);
      nfields = PQnfields (xr->result);
    }
  SCM_ALLOW_INTS;

  for (pgrs_index = 0; pgrs_index < pgrs_count; pgrs_index++)
    if (status == pgrs[pgrs_index].n)
      break;

  scm_puts ("#<PG-RESULT:", port);
  scm_intprint (xr->count, 10, port); scm_putc (':', port);
  scm_puts (pgrs[pgrs_index].s + 6, port); scm_putc (':', port);
  scm_intprint (ntuples, 10, port); scm_putc (':', port);
  scm_intprint (nfields, 10, port);
  scm_putc ('>', port);

  return 1;
}

static SCM
xr_mark (SCM obj)
{
  xr_t *xr = xr_unbox (obj);

  GC_PRINT (fprintf (stderr, "marking PG-RESULT %d\n", xr->count));
  return xr->conn;
}

static scm_sizet
xr_free (SCM obj)
{
  xr_t *xr = xr_unbox (obj);
  scm_sizet size = sizeof (xr_t);

  GC_PRINT (fprintf (stderr, "sweeping PG-RESULT %d\n", xr->count));

  /* clear the result */
  if (xr->result)
    (void) PQclear (xr->result);

  /* free this object itself */
  free (xr);

  /* set extension data to NULL */
  SCM_SETCDR (obj, (SCM)NULL);

  return size;
}

static SCM
make_xr (PGresult *result, SCM conn)
{
  xr_t *xr;
  SCM z;

  /* This looks weird, but they're SCM_DEFERred in xr_box.  */
  SCM_ALLOW_INTS;
  z = xr_box ((xr_t *)
              scm_must_malloc (sizeof (xr_t), "PG-RESULT"));
  xr = xr_unbox (z);
  SCM_DEFER_INTS;

  xr->result = result;
  xr->count = ++pg_result_tag.count;
  xr->conn = conn;
  return z;
}


/*
 * string munging
 */

static SCM
strip_newlines (char *str)
{
  char *lc = str + strlen (str) - 1;    /* last char */

  while (str <= lc && *lc == '\n')
    lc--;

  return gh_str2scm (str, lc + 1 - str);
}


/*
 * other abstractions
 */

#define VALIDATE_FIELD_NUMBER_COPY(pos,num,res,cvar) \
  SCM_VALIDATE_INUM_RANGE_COPY (pos, num, 0, PQnfields (res), cvar)


/*
 * everything (except lo support and some async stuff)
 */

static SCM goodies;

PG_DEFINE (pg_guile_pg_loaded, "pg-guile-pg-loaded", 0, 0, 0,
           (void),
           "Return a list of symbols describing the Guile-PG\n"
           "installation.  These are basically derived from C preprocessor\n"
           "macros determined at build time by the configure script.\n"
           "Presence of this procedure is also a good indicator that\n"
           "the compiled module @code{(database postgres)} is\n"
           "available.  You can test this like so:\n\n"
           "@lisp\n"
           "(false-if-exception (pg-guile-pg-loaded))\n"
           "@end lisp")
{
  return goodies;
}

PG_DEFINE (pg_protocol_version, "pg-protocol-version", 1, 0, 0,
           (SCM conn),
           "Return the client protocol version for @var{conn}.\n"
           "This (integer) will be 2 prior to PostgreSQL 7.4.\n"
           "If @var{conn} is not a connection object, return #f.")
{
#define FUNC_NAME s_pg_protocol_version
  PGconn *dbconn;
  int v = 2;

  if (! xc_p (conn))
    return SCM_BOOL_F;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

#ifdef HAVE_PQPROTOCOLVERSION
  v = PQprotocolVersion (dbconn);
#endif

  return (0 == v ? SCM_BOOL_F : gh_int2scm (v));
#undef FUNC_NAME
}

#define SIMPLE_KEYWORD(name) \
  SCM_KEYWORD (kwd_ ## name, # name)

SIMPLE_KEYWORD (envvar);
SIMPLE_KEYWORD (compiled);
SIMPLE_KEYWORD (val);
SIMPLE_KEYWORD (label);
SIMPLE_KEYWORD (dispchar);
SIMPLE_KEYWORD (dispsize);

#define KWD(name)  (kwd_ ## name)

PG_DEFINE (pg_conndefaults, "pg-conndefaults", 0, 0, 0,
           (void),
           "Return an alist associating options with their connection\n"
           "defaults.  The option name is a keyword.\n"
           "Each associated value is in turn a sub-alist, with\n"
           "the following keys:\n\n"
           "@itemize\n"
           "@item #:envvar\n\n"
           "@item #:compiled\n"
           "@item #:val\n"
           "@item #:label\n"
           "@item #:dispchar (character: @code{#\\*} or @code{#\\D}; or #f)\n"
           "@item #:dispsize (integer)\n"
           "@end itemize\n\n"
           "Values are strings or #f, unless noted otherwise.\n"
           "A @code{dispchar} of @code{#\\*} means the option should\n"
           "be treated like a password: user dialogs should hide\n"
           "the value; while @code{#\\D} means the option is for\n"
           "debugging purposes: probably a good idea to entirely avoid\n"
           "presenting this option in the first place.")
{
  PQconninfoOption *opt, *head;
  SCM tem, pdl, rv = SCM_EOL;

#define MAYBEFALSE(field,exp)                                   \
  ((!opt->field || '\0' == opt->field[0]) ? SCM_BOOL_F : (exp))
#define PUSH() pdl = gh_cons (tem, pdl)
  for (head = opt = PQconndefaults (); opt && opt->keyword; opt++)
    {
      pdl = SCM_EOL;

      tem = gh_int2scm (opt->dispsize);
      tem = gh_cons (KWD (dispsize), tem);
      PUSH ();

      tem = MAYBEFALSE (dispchar, gh_char2scm (opt->dispchar[0]));
      tem = gh_cons (KWD (dispchar), tem);
      PUSH ();

      tem = MAYBEFALSE (label, gh_str02scm (opt->label));
      tem = gh_cons (KWD (label), tem);
      PUSH ();

      tem = MAYBEFALSE (val, gh_str02scm (opt->val));
      tem = gh_cons (KWD (val), tem);
      PUSH ();

      tem = MAYBEFALSE (compiled, gh_str02scm (opt->compiled));
      tem = gh_cons (KWD (compiled), tem);
      PUSH ();

      tem = MAYBEFALSE (envvar, gh_str02scm (opt->envvar));
      tem = gh_cons (KWD (envvar), tem);
      PUSH ();

      tem = scm_c_make_keyword (opt->keyword);
      PUSH ();

      rv = gh_cons (pdl, rv);
    }
#undef PUSH
#undef MAYBEFALSE

  if (head)
    (void) PQconninfoFree (head);

  return rv;
}

static void
notice_processor (void *xc, const char *message)
{
  SCM out = ((xc_t *) xc)->notice;
  SCM msg;

  if (EXACTLY_FALSEP (out))
    return;

  msg = gh_str02scm (message);

  if (EXACTLY_TRUEP (out))
    out = scm_current_error_port ();

  if (SCM_OUTPORTP (out))
    {
      scm_display (msg, out);
      return;
    }

  if (NOT_FALSEP (scm_procedure_p (out)))
    scm_apply (out, msg, scm_listofnull);
}

PG_DEFINE (pg_connectdb, "pg-connectdb", 1, 0, 0,
           (SCM constr),
           "Open and return a connection to the database specified\n"
           "and configured by @var{constr}, a possibly empty string\n"
           "consisting of space-separated @code{name=value} pairs.\n"
           "The @var{name} can be any of:\n"
           "\n"
           "@table @code\n"
           "@item host\n"
           "The host-name or dotted-decimal IP address of the host\n"
           "on which the postmaster is running.  If no @code{host=}\n"
           "sub-string is given then consult in order: the\n"
           "environment variable @code{PGHOST}, otherwise the name\n"
           "of the local host.\n"
           "\n"
           "@item port\n"
           "The TCP or Unix socket on which the backend is\n"
           "listening.  If this is not specified then consult in\n"
           "order: the environment variable @code{PGPORT},\n"
           "otherwise the default port (5432).\n"
           "\n"
           "@item options\n"
           "A string containing the options to the backend server.\n"
           "The options given here are in addition to the options\n"
           "given by the environment variable @code{PGOPTIONS}.\n"
           "The options string should be a set of command line\n"
           "switches as would be passed to the backend.  See the\n"
           "postgres(1) man page for more details.\n"
           "\n"
           "@item tty\n"
           "A string defining the file or device on which error\n"
           "messages from the backend are to be displayed.  If this\n"
           "is empty (@code{\"\"}), then consult the environment\n"
           "variable @code{PGTTY}.  If the specified tty is a file\n"
           "then the file will be readable only by the user the\n"
           "postmaster runs as (usually @code{postgres}).\n"
           "Similarly, if the specified tty is a device then it\n"
           "must have permissions allowing the postmaster user to\n"
           "write to it.\n"
           "\n"
           "@item dbname\n"
           "The name of the database.  If no @code{dbname=}\n"
           "sub-string is given then consult in order: the\n"
           "environment variable @code{PGDATABASE}, the environment\n"
           "variable @code{USER}, otherwise the @code{user} value.\n"
           "\n"
           "@item user\n"
           "The login name of the user to authenticate.  If none is\n"
           "given then consult in order: the environment variable\n"
           "@code{PGUSER}, otherwise the login name of the user\n"
           "owning the process.\n"
           "\n"
           "@item password\n"
           "The password.  Whether or not this is used depends upon\n"
           "the contents of the @file{pg_hba.conf} file.  See the\n"
           "pg_hba.conf(5) man page for details.\n"
           "\n"
           "@item authtype\n"
           "This must be set to @code{password} if password\n"
           "authentication is in use, otherwise it must not be\n"
           "specified.  @end table\n"
           "\n"
           "If @var{value} contains spaces it must be enclosed in\n"
           "single quotes, and any single quotes appearing in\n"
           "@var{value} must be escaped using backslashes.\n"
           "Backslashes appearing in @var{value} must similarly be\n"
           "escaped.  Note that if the @var{constr} is a Guile\n"
           "string literal then all the backslashes will themselves\n"
           "need to be escaped a second time.")
{
#define FUNC_NAME s_pg_connectdb
  xc_t *xc;
  SCM z;
  PGconn *dbconn;
  ConnStatusType connstat;
  SCM pgerrormsg = SCM_BOOL_F;

  SCM_ASSERT (SCM_NIMP (constr) && SCM_ROSTRINGP (constr), constr,
              SCM_ARG1, FUNC_NAME);
  ROZT_X (constr);

  SCM_DEFER_INTS;
  dbconn = PQconnectdb (ROZT (constr));

  if ((connstat = PQstatus (dbconn)) == CONNECTION_BAD)
    {
      /* Get error message before PQfinish, which zonks dbconn storage.  */
      pgerrormsg = strip_newlines (PQerrorMessage (dbconn));
      (void) PQfinish (dbconn);
    }
  SCM_ALLOW_INTS;

  if (connstat == CONNECTION_BAD)
    scm_misc_error (FUNC_NAME, "~A", SCM_LIST1 (pgerrormsg));

  z = xc_box ((xc_t *) scm_must_malloc (sizeof (xc_t), "PG-CONN"));
  xc = xc_unbox (z);

  xc->dbconn = dbconn;
  xc->count = ++pg_conn_tag.count;
  xc->client = SCM_BOOL_F;
  xc->notice = SCM_BOOL_T;
  xc->fptrace = (FILE *) NULL;

  /* Whatever the default was before, we don't care.  */
  (void) PQsetNoticeProcessor (dbconn, &notice_processor, xc);

  return z;
#undef FUNC_NAME
}

PG_DEFINE (pg_connection_p, "pg-connection?", 1, 0, 0,
           (SCM obj),
           "Return #t iff @var{obj} is a connection object\n"
           "returned by @code{pg-connectdb}.")
{
  return xc_p (obj) ? SCM_BOOL_T : SCM_BOOL_F;
}

PG_DEFINE (pg_reset, "pg-reset", 1, 0, 0,
           (SCM conn),
           "Reset the connection @var{conn} with the backend.\n"
           "Equivalent to closing the connection and re-opening it again\n"
           "with the same connect options as given to @code{pg-connectdb}.\n"
           "@var{conn} must be a valid @code{PG_CONN} object returned by\n"
           "@code{pg-connectdb}.")
{
#define FUNC_NAME s_pg_reset
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  (void) PQreset (dbconn);
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
#undef FUNC_NAME
}

PG_DEFINE (pg_get_client_data, "pg-get-client-data", 1, 0, 0,
           (SCM conn),
           "Return the the client data associated with @var{conn}.")
{
#define FUNC_NAME s_pg_get_client_data
  ASSERT_CONNECTION (1, conn);
  return CONN_CLIENT (conn);
#undef FUNC_NAME
}

PG_DEFINE (pg_set_client_data, "pg-set-client-data!", 2, 0, 0,
           (SCM conn, SCM data),
           "Associate @var{data} with @var{conn}.")
{
#define FUNC_NAME s_pg_set_client_data
  ASSERT_CONNECTION (1, conn);
  SCM_DEFER_INTS;
  CONN_CLIENT (conn) = data;
  SCM_ALLOW_INTS;
  return (data);
#undef FUNC_NAME
}

PG_DEFINE (pg_exec, "pg-exec", 2, 0, 0,
           (SCM conn, SCM statement),
           "Execute the SQL string @var{statement} on a given connection\n"
           "@var{conn} returning either a @code{PG_RESULT} object containing\n"
           "a @code{pg-result-status} or @code{#f} if an error occurred,\n"
           "in which case the error message can be obtained using\n"
           "@code{pg-error-message}, passing it the @code{PG_CONN} object\n"
           "on which the statement was attempted.  Note that the error\n"
           "message is available only until the next call to @code{pg-exec}\n"
           "on this connection.")
{
#define FUNC_NAME s_pg_exec
  SCM z;
  PGconn *dbconn;
  PGresult *result;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_ASSERT (SCM_NIMP (statement) && SCM_ROSTRINGP (statement),
              statement, SCM_ARG2, FUNC_NAME);
  ROZT_X (statement);

  SCM_DEFER_INTS;
  result = PQexec (dbconn, ROZT (statement));

  z = (result
       ? make_xr (result, conn)
       : SCM_BOOL_F);
  SCM_ALLOW_INTS;
  return z;
#undef FUNC_NAME
}

PG_DEFINE (pg_result_p, "pg-result?", 1, 0, 0,
           (SCM obj),
           "Return #t iff @var{obj} is a result object\n"
           "returned by @code{pg-exec}.")
{
  return xr_p (obj) ? SCM_BOOL_T : SCM_BOOL_F;
}

PG_DEFINE (pg_result_error_message, "pg-result-error-message", 1, 0, 0,
           (SCM result),
           "Return the error message associated with @var{result},\n"
           "or the empty string if there was no error.\n"
           "If the installation does not support\n"
           "@code{PQRESULTERRORMESSAGE}, return the empty string.")
{
#ifdef HAVE_PQRESULTERRORMESSAGE

#define FUNC_NAME s_pg_result_error_message
  xr_t *xr; PGresult *res;
  char *msg;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  msg = PQresultErrorMessage (res);
  return strip_newlines (msg);
#undef FUNC_NAME

#else /* !HAVE_PQRESULTERRORMESSAGE */

  return gh_str2scm ("", 0);

#endif /* !HAVE_PQRESULTERRORMESSAGE */
}

PG_DEFINE (pg_error_message, "pg-error-message", 1, 0, 0,
           (SCM conn),
           "Return the most-recent error message that occurred on the\n"
           "connection @var{conn}, or an empty string.\n"
           "For backward compatability, if @var{conn} is actually\n"
           "a result object returned from calling @code{pg-exec},\n"
           "delegate the call to @code{pg-result-error-message}\n"
           "transparently (new code should call that procedure\n"
           "directly).")
{
#define FUNC_NAME s_pg_error_message
  if (xr_p (conn))
    return pg_result_error_message (conn);

  {
    PGconn *dbconn;
    SCM rv;
    char *msg;

    VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
    SCM_DEFER_INTS;
    msg = PQerrorMessage (dbconn);
    rv = strip_newlines (msg);
    SCM_ALLOW_INTS;

    return rv;
  }
#undef FUNC_NAME
}

PG_DEFINE (pg_get_db, "pg-get-db", 1, 0, 0,
           (SCM conn),
           "Return a string containing the name of the database\n"
           "to which @var{conn} represents a connection.")
{
#define FUNC_NAME s_pg_get_db
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  rv = PQdb (dbconn);
  SCM_ALLOW_INTS;

  return gh_str02scm (rv);
#undef FUNC_NAME
}

PG_DEFINE (pg_get_user, "pg-get-user", 1, 0, 0,
           (SCM conn),
           "Return a string containing the user name used to\n"
           "authenticate the connection @var{conn}.")
{
#define FUNC_NAME s_pg_get_user
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  rv = PQuser (dbconn);
  SCM_ALLOW_INTS;

  return gh_str02scm (rv);
#undef FUNC_NAME
}

PG_DEFINE (pg_get_pass, "pg-get-pass", 1, 0, 0,
           (SCM conn),
           "Return a string containing the password used to\n"
           "authenticate the connection @var{conn}.\n"
           "If the installation does not support @code{PQPASS}, return #f.")
{
#ifdef HAVE_PQPASS

#define FUNC_NAME s_pg_get_pass
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  rv = PQpass (dbconn);
  SCM_ALLOW_INTS;

  return gh_str02scm (rv);
#undef FUNC_NAME

#else  /* !HAVE_PQPASS */

  return SCM_BOOL_F;

#endif /* !HAVE_PQPASS */
}

PG_DEFINE (pg_get_host, "pg-get-host", 1, 0, 0,
           (SCM conn),
           "Return a string containing the name of the host to which\n"
           "@var{conn} represents a connection.")
{
#define FUNC_NAME s_pg_get_host
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  rv = PQhost (dbconn);
  SCM_ALLOW_INTS;

  return gh_str02scm (rv);
#undef FUNC_NAME
}

PG_DEFINE (pg_get_port,"pg-get-port", 1, 0, 0,
           (SCM conn),
           "Return a string containing the port number to which\n"
           "@var{conn} represents a connection.")
{
#define FUNC_NAME s_pg_get_port
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  rv = PQport (dbconn);
  SCM_ALLOW_INTS;

  return gh_str02scm (rv);
#undef FUNC_NAME
}

PG_DEFINE (pg_get_tty, "pg-get-tty", 1, 0, 0,
           (SCM conn),
           "Return a string containing the the name of the\n"
           "diagnostic tty for @var{conn}.")
{
#define FUNC_NAME s_pg_get_tty
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  rv = PQtty (dbconn);
  SCM_ALLOW_INTS;

  return gh_str02scm (rv);
#undef FUNC_NAME
}

PG_DEFINE (pg_get_options, "pg-get-options", 1, 0, 0,
           (SCM conn),
           "Return a string containing the the options string for @var{conn}.")
{
#define FUNC_NAME s_pg_get_options
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  rv = PQoptions (dbconn);
  SCM_ALLOW_INTS;

  return gh_str02scm (rv);
#undef FUNC_NAME
}

PG_DEFINE (pg_get_connection, "pg-get-connection", 1, 0, 0,
           (SCM result),
           "Return the @code{PG_CONN} object representing the connection\n"
           "from which a @var{result} was returned.")
{
#define FUNC_NAME s_pg_get_connection
  xr_t *xr; PGresult *res;
  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  return (xr->conn);
#undef FUNC_NAME
}

PG_DEFINE (pg_backend_pid, "pg-backend-pid", 1, 0, 0,
           (SCM conn),
           "Return an integer which is the the PID of the backend\n"
           "process for @var{conn}.\n"
           "If the installation does not support @code{PQBACKENDPID},\n"
           "return -1.")
{
#ifdef HAVE_PQBACKENDPID

#define FUNC_NAME s_pg_backend_pid
  PGconn *dbconn;
  int pid;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  pid = PQbackendPID (dbconn);
  SCM_ALLOW_INTS;

  return gh_int2scm (pid);
#undef FUNC_NAME

#else /* !HAVE_PQBACKENDPID */

  return gh_int2scm (-1);

#endif /* !HAVE_PQBACKENDPID */
}

PG_DEFINE (pg_result_status, "pg-result-status", 1, 0, 0,
           (SCM result),
           "Return the symbolic status of a @code{PG_RESULT} object\n"
           "returned by @code{pg-exec}.")
{
#define FUNC_NAME s_pg_result_status
  xr_t *xr; PGresult *res;
  int result_status;
  int pgrs_index;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);

  SCM_DEFER_INTS;
  result_status = PQresultStatus (res);
  SCM_ALLOW_INTS;

  for (pgrs_index = 0; pgrs_index < pgrs_count; pgrs_index++)
    if (result_status == pgrs[pgrs_index].n)
      return pgrs[pgrs_index].sym;

  /* FIXME: Although we should never get here, be slackful for now.  */
  /* abort(); */
  return gh_int2scm (result_status);
#undef FUNC_NAME
}

PG_DEFINE (pg_ntuples, "pg-ntuples", 1, 0, 0,
           (SCM result),
           "Return the number of tuples in @var{result}.")
{
#define FUNC_NAME s_pg_ntuples
  xr_t *xr; PGresult *res;
  int ntuples;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);

  SCM_DEFER_INTS;
  ntuples = PQntuples (res);
  SCM_ALLOW_INTS;

  return gh_int2scm (ntuples);
#undef FUNC_NAME
}

PG_DEFINE (pg_nfields, "pg-nfields", 1, 0, 0,
           (SCM result),
           "Return the number of fields in @var{result}.")
{
#define FUNC_NAME s_pg_nfields
  xr_t *xr; PGresult *res;
  SCM rv;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);

  SCM_DEFER_INTS;
  rv = gh_int2scm (PQnfields (res));
  SCM_ALLOW_INTS;

  return rv;
#undef FUNC_NAME
}

PG_DEFINE (pg_cmdtuples, "pg-cmdtuples", 1, 0, 0,
           (SCM result),
           "Return the number of tuples in @var{result} affected by a\n"
           "command.  This is a string which is empty in the case of\n"
           "commands like @code{CREATE TABLE}, @code{GRANT}, @code{REVOKE}\n"
           "etc. which don't affect tuples.")
{
#define FUNC_NAME s_pg_cmdtuples
  xr_t *xr; PGresult *res;
  const char *cmdtuples;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);

  SCM_DEFER_INTS;
  cmdtuples = PQcmdTuples (res);
  SCM_ALLOW_INTS;

  return gh_str02scm (cmdtuples);
#undef FUNC_NAME
}

PG_DEFINE (pg_oid_value, "pg-oid-value", 1, 0, 0,
           (SCM result),
           "If the @var{result} is that of an SQL @code{INSERT} command,\n"
           "return the integer OID of the inserted tuple, otherwise return\n"
           "@code{#f}.\n"
           "If the installation does not support @code{PQOIDVALUE}, return #f.")
{
#ifdef HAVE_PQOIDVALUE

#define FUNC_NAME s_pg_oid_value
  xr_t *xr; PGresult *res;
  Oid oid_value;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);

  SCM_DEFER_INTS;
  oid_value = PQoidValue (res);
  SCM_ALLOW_INTS;

  if (oid_value == InvalidOid)
    return SCM_BOOL_F;

  return gh_int2scm (oid_value);
#undef FUNC_NAME

#else /* !HAVE_PQOIDVALUE */

  return SCM_BOOL_F;

#endif /* !HAVE_PQOIDVALUE */
}

PG_DEFINE (pg_fname, "pg-fname", 2, 0, 0,
           (SCM result, SCM num),
           "Return a string containing the canonical lower-case name\n"
           "of the field number @var{num} in @var{result}.  SQL variables\n"
           "and field names are not case-sensitive.")
{
#define FUNC_NAME s_pg_fname
  xr_t *xr; PGresult *res;
  int field;
  const char *fname;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fname = PQfname (res, field);
  return gh_str02scm (fname);
#undef FUNC_NAME
}

PG_DEFINE (pg_fnumber, "pg-fnumber", 2, 0, 0,
           (SCM result, SCM fname),
           "Return the integer field-number corresponding to field\n"
           "@var{fname} if this exists in @var{result}, or @code{-1}\n"
           "otherwise.")
{
#define FUNC_NAME s_pg_fnumber
  xr_t *xr; PGresult *res;
  int fnum;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  SCM_ASSERT (SCM_NIMP (fname) && SCM_ROSTRINGP (fname), fname,
              SCM_ARG2, FUNC_NAME);
  ROZT_X (fname);

  SCM_DEFER_INTS;
  fnum = PQfnumber (res, ROZT (fname));
  SCM_ALLOW_INTS;

  return gh_int2scm (fnum);
#undef FUNC_NAME
}

PG_DEFINE (pg_ftype, "pg-ftype", 2, 0, 0,
           (SCM result, SCM num),
           "Return the PostgreSQL internal integer representation of\n"
           "the type of the given attribute.  The integer is actually an\n"
           "OID (object ID) which can be used as the primary key to\n"
           "reference a tuple from the system table @code{pg_type}.  A\n"
           "@code{misc-error} is thrown if the @code{field-number} is\n"
           "not valid for the given @code{result}.")
{
#define FUNC_NAME s_pg_ftype
  xr_t *xr; PGresult *res;
  int field;
  int ftype;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  ftype = PQftype (res, field);

  return gh_int2scm (ftype);
#undef FUNC_NAME
}

PG_DEFINE (pg_fsize, "pg-fsize", 2, 0, 0,
           (SCM result, SCM num),
           "Return the size of a @var{result} field @var{num} in bytes,\n"
           "or -1 if the field is variable-length.")
{
#define FUNC_NAME s_pg_fsize
  xr_t *xr; PGresult *res;
  int field;
  int fsize;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fsize = PQfsize (res, field);
  return gh_int2scm (fsize);
#undef FUNC_NAME
}

PG_DEFINE (pg_getvalue, "pg-getvalue", 3, 0, 0,
           (SCM result, SCM stuple, SCM sfield),
           "Return a string containing the value of the attribute\n"
           "@var{sfield}, tuple @var{stuple} of @var{result}.  It is\n"
           "up to the caller to convert this to the required type.")
{
#define FUNC_NAME s_pg_getvalue
  xr_t *xr; PGresult *res;
  int maxtuple, tuple;
  int maxfield, field;
  const char *val;
#ifdef HAVE_PQBINARYTUPLES
  int isbinary, veclen = 0;
#endif
  SCM srv;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  SCM_VALIDATE_INUM_MIN_COPY (2, stuple, 0, tuple);
  SCM_VALIDATE_INUM_MIN_COPY (3, sfield, 0, field);
  SCM_DEFER_INTS;
  maxtuple = PQntuples (res);
  maxfield = PQnfields (res);
  SCM_ALLOW_INTS;
  SCM_ASSERT (tuple < maxtuple, stuple, SCM_OUTOFRANGE, FUNC_NAME);
  SCM_ASSERT (field < maxfield, sfield, SCM_OUTOFRANGE, FUNC_NAME);
  SCM_DEFER_INTS;
  val = PQgetvalue (res, tuple, field);
#ifdef HAVE_PQBINARYTUPLES
  if ((isbinary = PQbinaryTuples (res)) != 0)
    veclen = PQgetlength (res, tuple, field);
#endif
  SCM_ALLOW_INTS;

#ifdef HAVE_PQBINARYTUPLES
  if (isbinary)
    srv = gh_str2scm (val, veclen);
  else
#endif
    srv = gh_str02scm (val);

  return srv;
#undef FUNC_NAME
}

PG_DEFINE (pg_getlength, "pg-getlength", 3, 0, 0,
           (SCM result, SCM stuple, SCM sfield),
           "The size of the datum in bytes.")
{
#define FUNC_NAME s_pg_getlength
  xr_t *xr; PGresult *res;
  int maxtuple, tuple;
  int maxfield, field;
  int len;
  SCM ret;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  SCM_VALIDATE_INUM_MIN_COPY (2, stuple, 0, tuple);
  SCM_VALIDATE_INUM_MIN_COPY (3, sfield, 0, field);
  SCM_DEFER_INTS;
  maxtuple = PQntuples (res);
  maxfield = PQnfields (res);
  SCM_ALLOW_INTS;
  SCM_ASSERT (tuple < maxtuple, stuple, SCM_OUTOFRANGE, FUNC_NAME);
  SCM_ASSERT (field < maxfield, sfield, SCM_OUTOFRANGE, FUNC_NAME);
  SCM_DEFER_INTS;
  len = PQgetlength (res, tuple, field);
  SCM_ALLOW_INTS;

  ret = gh_int2scm (len);
  return ret;
#undef FUNC_NAME
}

PG_DEFINE (pg_getisnull, "pg-getisnull", 3, 0, 0,
           (SCM result, SCM stuple, SCM sfield),
           "Return @code{#t} if the attribute is @code{NULL},\n"
           "@code{#f} otherwise.")
{
#define FUNC_NAME s_pg_getisnull
  xr_t *xr; PGresult *res;
  int maxtuple, tuple;
  int maxfield, field;
  SCM rv;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  SCM_VALIDATE_INUM_MIN_COPY (2, stuple, 0, tuple);
  SCM_VALIDATE_INUM_MIN_COPY (3, sfield, 0, field);
  SCM_DEFER_INTS;
  maxtuple = PQntuples (res);
  maxfield = PQnfields (res);
  SCM_ALLOW_INTS;
  SCM_ASSERT (tuple < maxtuple, stuple, SCM_OUTOFRANGE, FUNC_NAME);
  SCM_ASSERT (field < maxfield, sfield, SCM_OUTOFRANGE, FUNC_NAME);
  SCM_DEFER_INTS;
  if (PQgetisnull (res, tuple, field))
    rv = SCM_BOOL_T;
  else
    rv = SCM_BOOL_F;
  SCM_ALLOW_INTS;

  return rv;
#undef FUNC_NAME
}

PG_DEFINE (pg_binary_tuples, "pg-binary-tuples?", 1, 0, 0,
           (SCM result),
           "Return @code{#t} if @var{result} contains binary tuple\n"
           "data, @code{#f} otherwise.\n"
           "If the installation does not support @code{PQBINARYTUPLES},\n"
           "return #f.")
{
#ifdef HAVE_PQBINARYTUPLES

#define FUNC_NAME s_pg_binary_tuples
  xr_t *xr; PGresult *res;
  SCM rv;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);

  SCM_DEFER_INTS;
  if (PQbinaryTuples (res))
    rv = SCM_BOOL_T;
  else
    rv = SCM_BOOL_F;
  SCM_ALLOW_INTS;

  return rv;
#undef FUNC_NAME

#else /* !HAVE_PQBINARYTUPLES */

  return SCM_BOOL_F;

#endif /* !HAVE_PQBINARYTUPLES */
}

PG_DEFINE (pg_fmod, "pg-fmod", 2, 0, 0,
           (SCM result, SCM num),
           "Return the integer type-specific modification data for\n"
           "the given field (field number @var{num}) of @var{result}.\n"
           "If the installation does not support @code{PQFMOD}, return -1.")
{
#ifdef HAVE_PQFMOD

#define FUNC_NAME s_pg_fmod
  xr_t *xr; PGresult *res;
  int field;
  int fmod;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fmod = PQfmod (res, field);
  return gh_int2scm (fmod);
#undef FUNC_NAME

#else /* !HAVE_PQFMOD */

  return gh_int2scm (-1);

#endif /* !HAVE_PQFMOD */
}

PG_DEFINE (pg_getline, "pg-getline", 1, 0, 0,
           (SCM conn),
           "Read a line from @var{conn} on which a @code{COPY <table> TO\n"
           "STDOUT} has been issued.  Return a string from the connection.\n"
           "A returned string consisting of a backslash followed by a full\n"
           "stop signifies an end-of-copy marker.")
{
#define FUNC_NAME s_pg_getline
  PGconn *dbconn;
  char buf[BUF_LEN];
  int ret = 1;
  SCM str = SCM_UNDEFINED;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  while (ret != 0 && ret != EOF)
    {
      SCM_DEFER_INTS;
      ret = PQgetline (dbconn, buf, BUF_LEN);
      SCM_ALLOW_INTS;
      if (str == SCM_UNDEFINED)
        str = gh_str02scm (buf);
      else
        str = scm_string_append (SCM_LIST2 (str, gh_str02scm (buf)));
    }
  return str;
#undef FUNC_NAME
}

PG_DEFINE (pg_getlineasync, "pg-getlineasync", 2, 1, 0,
           (SCM conn, SCM buf, SCM tickle),
           "Read a line from @var{conn} on which a @code{COPY <table> TO\n"
           "STDOUT} has been issued, into @var{buf} (a string).\n"
           "Return -1 to mean end-of-copy marker recognized, or a number\n"
           "(possibly zero) indicating how many bytes of data were read.\n"
           "The returned data may contain at most one newline (in the last\n"
           "byte position).\n"
           "Optional arg @var{tickle} non-#f means to do a\n"
           "\"consume input\" operation prior to the read.")
{
#define FUNC_NAME s_pg_getlineasync
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_ASSERT (SCM_STRINGP (buf), buf, SCM_ARG2, FUNC_NAME);

  if (tickle != SCM_UNDEFINED && NOT_FALSEP (tickle))
    /* We don't care if there was an error consuming input; caller can use
       `pg_error_message' to find out afterwards, or simply avoid tickling in
       the first place.  */
    (void) PQconsumeInput (dbconn);

  return gh_int2scm (PQgetlineAsync (dbconn,
                                     SCM_ROCHARS (buf),
                                     SCM_ROLENGTH (buf)));
#undef FUNC_NAME
}

PG_DEFINE (pg_putline, "pg-putline", 2, 0, 0,
           (SCM conn, SCM str),
           "Write a line to the connection on which a @code{COPY <table>\n"
           "FROM STDIN} has been issued.  The lines written should include\n"
           "the final newline characters.  The last line should be a\n"
           "backslash, followed by a full-stop.  After this, the\n"
           "@code{pg-endcopy} procedure should be called for this\n"
           "connection before any further @code{pg-exec} call is made.\n"
           "Return #t if successful.")
{
#define FUNC_NAME s_pg_putline
  PGconn *dbconn;
  int status;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_ASSERT (SCM_NIMP (str)&&SCM_ROSTRINGP (str), str, SCM_ARG2, FUNC_NAME);
  SCM_DEFER_INTS;
#ifdef HAVE_PQPUTNBYTES
  status = PQputnbytes (dbconn, SCM_ROCHARS (str), SCM_ROLENGTH (str));
#else
  ROZT_X (str);
  status = PQputline (dbconn, ROZT (str));
#endif
  SCM_ALLOW_INTS;
  return (0 == status ? SCM_BOOL_T : SCM_BOOL_F);
#undef FUNC_NAME
}

PG_DEFINE (pg_endcopy, "pg-endcopy", 1, 0, 0,
           (SCM conn),
           "Resynchronize with the backend process.  This procedure\n"
           "must be called after the last line of a table has been\n"
           "transferred using @code{pg-getline}, @code{pg-getlineasync}\n"
           "or @code{pg-putline}.  Return #t if successful.")
{
#define FUNC_NAME s_pg_endcopy
  PGconn *dbconn;
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_DEFER_INTS;
  ret = PQendcopy (dbconn);
  SCM_ALLOW_INTS;

  return (0 == ret ? SCM_BOOL_T : SCM_BOOL_F);
#undef FUNC_NAME
}

PG_DEFINE (pg_trace, "pg-trace", 2, 0, 0,
           (SCM conn, SCM port),
           "Start outputting low-level trace information on the\n"
           "connection @var{conn} to @var{port}, which must have been\n"
           "opened for writing.  This trace is more useful for debugging\n"
           "PostgreSQL than it is for debugging its clients.\n"
           "The return value is unspecified.")
{
#define FUNC_NAME s_pg_trace
  PGconn *dbconn;
  struct scm_fport *fp = SCM_FSTREAM (port);
  int fd;
  FILE *fpout;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_ASSERT (CONN_FPTRACE (conn) == NULL, conn, SCM_ARG1, FUNC_NAME);
  port = SCM_COERCE_OUTPORT (port);
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPOUTFPORTP (port),
              port, SCM_ARG2, FUNC_NAME);

  SCM_SYSCALL (fd = dup (fp->fdes));
  if (fd == -1)
    scm_syserror (FUNC_NAME);
  SCM_SYSCALL (fpout = fdopen (fd, "w"));
  if (fpout == NULL)
    scm_syserror (FUNC_NAME);

  SCM_DEFER_INTS;
  (void) PQtrace (dbconn, fpout);
  CONN_FPTRACE (conn) = fpout;
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
#undef FUNC_NAME
}

PG_DEFINE (pg_untrace, "pg-untrace", 1, 0, 0,
           (SCM conn),
           "Stop tracing on connection @var{conn}.\n"
           "The return value is unspecified.")
{
#define FUNC_NAME s_pg_untrace
  PGconn *dbconn;
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  if (! CONN_FPTRACE (conn))
    /* We could throw an error here but that's not cool.  An error would be
       warranted in the future, however, if tracing state were to become
       nestable (with fluids, say) -- a relatively unlikely scenario.  */
    return SCM_UNSPECIFIED;

  SCM_DEFER_INTS;
  (void) PQuntrace (dbconn);
  SCM_SYSCALL (ret = fclose (CONN_FPTRACE (conn)));
  CONN_FPTRACE (conn) = (FILE *) NULL;
  SCM_ALLOW_INTS;
  if (ret)
    scm_syserror (FUNC_NAME);

  return SCM_UNSPECIFIED;
#undef FUNC_NAME
}


/*
 * printing -- this is arguably more trouble than its worth
 */

static long sepo_type_tag;

static int
sepo_p (SCM obj)
{
  return SCM_SMOB_PREDICATE (sepo_type_tag, obj);
}

static SCM
sepo_box (PQprintOpt *sepo)
{
  SCM z;

  SCM_ENTER_A_SECTION;
  SCM_NEWCELL (z);
  SCM_SETCDR (z, (SCM)sepo);
  SCM_SETCAR (z, sepo_type_tag);
  SCM_EXIT_A_SECTION;

  return z;
}

static PQprintOpt *
sepo_unbox (SCM obj)
{
  return ((PQprintOpt *) gh_cdr (obj));
}

static SCM
sepo_mark (SCM obj)
{
  GC_PRINT (fprintf (stderr, "marking PG-PRINT-OPTION %p\n", obj));
  return SCM_BOOL_F;
}

static scm_sizet
sepo_free (SCM obj)
{
  PQprintOpt *po = sepo_unbox (obj);
  scm_sizet size = 0;

  GC_PRINT (fprintf (stderr, "sweeping PG-PRINT-OPTION %p\n", obj));

#define _FREE_STRING(p)                         \
  do {                                          \
    if (p)                                      \
      {                                         \
        size += 1 + strlen (p);                 \
        free (p);                               \
      }                                         \
  } while (0)

  _FREE_STRING (po->fieldSep);
  _FREE_STRING (po->tableOpt);
  _FREE_STRING (po->caption);

  if (po->fieldName)
    {
      int i = 0;
      while (po->fieldName[i])
        {
          _FREE_STRING (po->fieldName[i]);
          i++;
        }
      size += i * sizeof (char *);
      free (po->fieldName);
    }
#undef _FREE_STRING

  size += sizeof (PQprintOpt);
  free (po);

  /* set extension data to NULL */
  SCM_SETCDR (obj, (SCM)NULL);

  return size;
}

static int
sepo_display (SCM sepo, SCM port, scm_print_state *pstate)
{
  scm_puts ("#<PG-PRINT-OPTION>", port);
  return 1;
}

static PQprintOpt default_print_options = {
  1,            /* print output field headings and row count */
  1,            /* fill align the fields */
  0,            /* old brain dead format */
  0,            /* output html tables */
  0,            /* expand tables */
  0,            /* use pager for output if needed */
  "|",          /* field separator */
  NULL,         /* insert to HTML <table ...> */
  NULL,         /* HTML <caption> */
  NULL          /* null terminated array of replacement field names */
};

SCM_SYMBOL (pg_sym_header, "header");
SCM_SYMBOL (pg_sym_no_header, "no-header");
SCM_SYMBOL (pg_sym_align, "align");
SCM_SYMBOL (pg_sym_no_align, "no-align");
SCM_SYMBOL (pg_sym_standard, "standard");
SCM_SYMBOL (pg_sym_no_standard, "no-standard");
SCM_SYMBOL (pg_sym_html3, "html3");
SCM_SYMBOL (pg_sym_no_html3, "no-html3");
SCM_SYMBOL (pg_sym_expanded, "expanded");
SCM_SYMBOL (pg_sym_no_expanded, "no-expanded");
SCM_SYMBOL (pg_sym_field_sep, "field-sep");
SCM_SYMBOL (pg_sym_table_opt, "table-opt");
SCM_SYMBOL (pg_sym_caption, "caption");
SCM_SYMBOL (pg_sym_field_names, "field-names");

static SCM valid_print_option_flags;
static SCM valid_print_option_keys;

PG_DEFINE (pg_make_print_options, "pg-make-print-options", 1, 0, 0,
           (SCM spec),
           "Return an opaque print options object created from @var{spec},\n"
           "suitable for use with @code{pg-print}.  @var{spec} is a list\n"
           "of elements, each either a flag (symbol) or a key-value pair\n"
           "(with the key being a symbol).  Recognized flags:\n\n"
           "@itemize\n"
           "@item header: Print output field headings and row count.\n"
           "@item align: Fill align the fields.\n"
           "@item standard: Old brain-dead format.\n"
           "@item html3: Output HTML tables.\n"
           "@item expanded: Expand tables.\n"
           "@end itemize\n\n"
           "To specify a disabled flag, use @dfn{no-FLAG}, e.g.,"
           "@code{no-header}.  Recognized keys:\n\n"
           "@itemize\n"
           "@item field-sep\n\n"
           "String specifying field separator.\n"
           "@item table-opt\n\n"
           "String specifying HTML table attributes.\n"
           "@item caption\n\n"
           "String specifying caption to use in HTML table.\n"
           "@item field-names\n\n"
           "List of replacement field names, each a string.\n"
           "@end itemize\n\n")
{
#define FUNC_NAME s_pg_make_print_options
  PQprintOpt *po;
  int count = 0;                        /* of substnames */
  SCM check, substnames = SCM_BOOL_F, flags = SCM_EOL, keys = SCM_EOL;

  SCM_ASSERT (SCM_NULLP (spec) || SCM_CONSP (spec),
              spec, SCM_ARG1, FUNC_NAME);

  /* hairy validation/collection: symbols in `flags', pairs in `keys' */
  check = spec;
  while (SCM_NNULLP (check))
    {
      SCM head = gh_car (check);
      if (SCM_SYMBOLP (head))
        {
          SCM_ASSERT (NOT_FALSEP (scm_memq (head, valid_print_option_flags)),
                      head, SCM_ARG1, FUNC_NAME);
          flags = gh_cons (head, flags);
        }
      else if (SCM_CONSP (head))
        {
          SCM key = gh_car (head);
          SCM val = gh_cdr (head);
          SCM_ASSERT (NOT_FALSEP (scm_memq (key, valid_print_option_keys)),
                      key, SCM_ARG1, FUNC_NAME);
          if (key == pg_sym_field_names)
            {
              SCM_ASSERT (SCM_NNULLP (val), head, SCM_ARG1, FUNC_NAME);
              while (SCM_NNULLP (val))
                {
                  SCM_ASSERT (SCM_STRINGP (gh_car (val)),
                              head, SCM_ARG1, FUNC_NAME);
                  count++;
                  val = gh_cdr (val);
                }
              substnames = gh_cdr (head);    /* i.e., `val' */
            }
          else
            {
              SCM_ASSERT (SCM_STRINGP (val), val, SCM_ARG1, FUNC_NAME);
              keys = gh_cons (head, keys);
            }
        }
      check = gh_cdr (check);
    }

  po = scm_must_malloc (sizeof (PQprintOpt), "PG-PRINT-OPTION");

#define _FLAG_CHECK(m)                                 \
  (NOT_FALSEP (scm_memq (pg_sym_no_ ## m, flags))      \
   ? 0 : (NOT_FALSEP (scm_memq (pg_sym_ ## m, flags))  \
          ? 1 : default_print_options.m))

  po->header   = _FLAG_CHECK (header);
  po->align    = _FLAG_CHECK (align);
  po->standard = _FLAG_CHECK (standard);
  po->html3    = _FLAG_CHECK (html3);
  po->expanded = _FLAG_CHECK (expanded);
  po->pager    = 0;                     /* never */
#undef _FLAG_CHECK

#define _STRING_CHECK_SETX(k,m)                         \
  do {                                                  \
    SCM stemp = scm_assq_ref (keys, pg_sym_ ## k);      \
    po->m = (NOT_FALSEP (stemp)                         \
             ? strdup (SCM_ROCHARS (stemp))             \
             : (default_print_options.m                 \
                ? strdup (default_print_options.m)      \
                : NULL));                               \
  } while (0)

  _STRING_CHECK_SETX (field_sep, fieldSep);
  _STRING_CHECK_SETX (table_opt, tableOpt);
  _STRING_CHECK_SETX (caption, caption);
#undef _STRING_CHECK_SETX

  if (EXACTLY_FALSEP (substnames))
    po->fieldName = NULL;
  else
    {
      int i;
      po->fieldName = (char **) scm_must_malloc ((1 + count) * sizeof (char *),
                                                 "PG-PRINT-OPTION fieldname");
      po->fieldName[count] = NULL;
      for (i = 0; i < count; i++)
        {
          po->fieldName[i] = strdup (SCM_ROCHARS (gh_car (substnames)));
          substnames = gh_cdr (substnames);
        }
    }

  return sepo_box (po);
#undef FUNC_NAME
}

PG_DEFINE (pg_print, "pg-print", 1, 1, 0,
           (SCM result, SCM options),
           "Display @var{result} on the current output port.\n"
           "Optional second arg @var{options} is an\n"
           "object returned by @code{pg-make-print-options} that\n"
           "specifies various parameters of the output format.")
{
#define FUNC_NAME s_pg_print
  xr_t *xr; PGresult *res;
  FILE *fout;
  int redir_p;

  VALIDATE_RESULT_UNBOX2 (1, result, xr, res);
  options = ((options == SCM_UNDEFINED)
             ? pg_make_print_options (SCM_EOL)
             : options);
  SCM_ASSERT (sepo_p (options), options, SCM_ARG2, FUNC_NAME);

  redir_p = (! SCM_OPFPORTP (scm_current_output_port ())
             || (gh_scm2int (scm_fileno (scm_current_output_port ()))
                 != fileno (stdout)));
  fout = (redir_p ? tmpfile () : stdout);
  if (fout == NULL)
    scm_syserror (FUNC_NAME);

  (void) scm_force_output (scm_current_output_port ());
  (void) PQprint (fout, res, sepo_unbox (options));

  if (redir_p)
    {
      char buf[BUF_LEN];
      SCM outp = scm_current_output_port ();
      int howmuch = 0;

      buf[BUF_LEN - 1] = '\0';          /* elephant */
      fseek (fout, 0, SEEK_SET);

      while (BUF_LEN - 1 == (howmuch = fread (buf, 1, BUF_LEN - 1, fout)))
        scm_display (gh_str02scm (buf), outp);
      if (feof (fout))
        {
          buf[howmuch] = '\0';
          scm_display (gh_str02scm (buf), outp);
        }
      fclose (fout);
    }

  return SCM_UNSPECIFIED;
#undef FUNC_NAME
}


/* Modify notice processing.

   Note that this is not a simple wrap of `PQsetNoticeProcessor'.  Instead,
   we simply modify `xc->notice'.  Also, the value can either be a port or
   procedure that takes a string.  For these reasons, we name the procedure
   `pg-set-notice-out!' to help avoid confusion.  */

PG_DEFINE (pg_set_notice_out_x, "pg-set-notice-out!", 2, 0, 0,
           (SCM conn, SCM out),
           "Set notice output handler of @var{conn} to @var{out}.\n"
           "@var{out} can be #f, which means discard notices;\n"
           "#t, which means send them to the current error port;\n"
           "an output port to send the notice to; or a procedure that\n"
           "takes one argument, the notice string.  It's usually a good\n"
           "idea to call @code{pg-set-notice-out!} soon after establishing\n"
           "the connection.")
{
#define FUNC_NAME s_pg_set_notice_out_x
  ASSERT_CONNECTION (1, conn);

  if (EXACTLY_TRUEP (out) ||
      EXACTLY_FALSEP (out) ||
      SCM_OUTPORTP (out) ||
      NOT_FALSEP (scm_procedure_p (out)))
    CONN_NOTICE (conn) = out;
  else
    SCM_WTA (SCM_ARG2, out);

  return SCM_UNSPECIFIED;
#undef FUNC_NAME
}


/* Fetch asynchronous notifications.  */

PG_DEFINE (pg_notifies, "pg-notifies", 1, 1, 0,
           (SCM conn, SCM tickle),
           "Return the next as-yet-unhandled notification\n"
           "from @var{conn}, or #f if there are none available.\n"
           "The notification is a pair with @sc{car} @var{relname},\n"
           "a string naming the relation containing data; and\n"
           "@sc{cdr} @var{pid}, the integer pid\n"
           "of the backend delivering the notification.\n"
           "Optional arg @var{tickle} non-#f means to do a\n"
           "\"consume input\" operation prior to the query.")
{
#define FUNC_NAME s_pg_notifies
  PGconn *dbconn;
  PGnotify *n;
  SCM rv = SCM_BOOL_F;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  if (tickle != SCM_UNDEFINED && NOT_FALSEP (tickle))
    /* We don't care if there was an error consuming input; caller can use
       `pg_error_message' to find out afterwards, or simply avoid tickling in
       the first place.  */
    (void) PQconsumeInput (dbconn);
  n = PQnotifies (dbconn);
  if (n)
    {
      rv = gh_str02scm (n->relname);
      rv = gh_cons (rv, gh_int2scm (n->be_pid));
      free (n);
    }
  return rv;
#undef FUNC_NAME
}


/* Client encoding.  */

/* Hmmm, `pg_encoding_to_char' is not in the headers.  However, it is
   mentioned in the Multibyte Support chapter (section 7.2.2 -- Setting
   the Encoding), and seems to work w/ PostgreSQL 7.3.8.  */
extern char * pg_encoding_to_char (int encoding);

PG_DEFINE (pg_client_encoding, "pg-client-encoding", 1, 0, 0,
           (SCM conn),
           "Return the current client encoding for @var{conn}.")
{
#define FUNC_NAME s_pg_client_encoding
  PGconn *dbconn;
  SCM enc;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  enc = gh_str02scm (pg_encoding_to_char (PQclientEncoding (dbconn)));
  return enc;
#undef FUNC_NAME
}

PG_DEFINE (pg_set_client_encoding_x, "pg-set-client-encoding!", 2, 0, 0,
           (SCM conn, SCM encoding),
           "Set the client encoding for @var{conn} to @var{encoding}.\n"
           "Return #t if successful, #f otherwise.")
{
#define FUNC_NAME s_pg_set_client_encoding_x
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_ASSERT (SCM_STRINGP (encoding), encoding, SCM_ARG2, FUNC_NAME);
  ROZT_X (encoding);

  return (0 == (PQsetClientEncoding (dbconn, ROZT (encoding)))
          ? SCM_BOOL_T
          : SCM_BOOL_F);
#undef FUNC_NAME
}


/*
 * non-blocking connection mode
 */

PG_DEFINE (pg_set_nonblocking_x, "pg-set-nonblocking!", 2, 0, 0,
           (SCM conn, SCM mode),
           "Set the nonblocking status of @var{conn} to @var{mode}.\n"
           "If @var{mode} is non-#f, set it to nonblocking, otherwise\n"
           "set it to blocking.  Return #t if successful.\n"
           "If @code{pg-guile-pg-loaded} does not include\n"
           "@code{PQSETNONBLOCKING}, do nothing and return #f.")
{
#define FUNC_NAME s_pg_set_nonblocking_x
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

#ifdef HAVE_PQSETNONBLOCKING
  return (0 == PQsetnonblocking (dbconn, ! EXACTLY_FALSEP (mode))
          ? SCM_BOOL_T
          : SCM_BOOL_F);
#else
  return SCM_BOOL_F;
#endif
#undef FUNC_NAME
}

PG_DEFINE (pg_is_nonblocking_p, "pg-is-nonblocking?", 1, 0, 0,
           (SCM conn),
           "Return #t if @var{conn} is in nonblocking mode.\n"
           "If @code{pg-guile-pg-loaded} does not include\n"
           "@code{PQISNONBLOCKING}, do nothing and return #f.")
{
#define FUNC_NAME s_pg_is_nonblocking_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

#ifdef HAVE_PQISNONBLOCKING
  return (PQisnonblocking (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
#else
  return SCM_BOOL_F;
#endif
#undef FUNC_NAME
}


/*
 * non-blocking query operations
 */

PG_DEFINE (pg_send_query, "pg-send-query", 2, 0, 0,
           (SCM conn, SCM query),
           "Send @var{conn} a non-blocking @var{query} (string).\n"
           "Return #t iff successful.  If not successful, error\n"
           "message is retrievable with @code{pg-error-message}.")
{
#define FUNC_NAME s_pg_send_query
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_ASSERT (SCM_STRINGP (query), query, SCM_ARG2, FUNC_NAME);
  ROZT_X (query);

  return (PQsendQuery (dbconn, ROZT (query))
          ? SCM_BOOL_T
          : SCM_BOOL_F);
#undef FUNC_NAME
}

PG_DEFINE (pg_get_result, "pg-get-result", 1, 0, 0,
           (SCM conn),
           "Return a result from @var{conn}, or #f.")
{
#define FUNC_NAME s_pg_send_query
  PGconn *dbconn;
  PGresult *result;
  SCM z;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  SCM_DEFER_INTS;
  result = PQgetResult (dbconn);
  z = (result
       ? make_xr (result, conn)
       : SCM_BOOL_F);
  SCM_ALLOW_INTS;
  return z;
#undef FUNC_NAME
}

PG_DEFINE (pg_consume_input, "pg-consume-input", 1, 0, 0,
           (SCM conn),
           "Consume input from @var{conn}.  Return #t iff successful.")
{
#define FUNC_NAME s_pg_consume_input
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return (PQconsumeInput (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
#undef FUNC_NAME
}

PG_DEFINE (pg_is_busy_p, "pg-is-busy?", 1, 0, 0,
           (SCM conn),
           "Return #t if there is data waiting for\n"
           "@code{pg-consume-input}, otherwise #f.")
{
#define FUNC_NAME s_pg_is_busy_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return (PQisBusy (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
#undef FUNC_NAME
}

PG_DEFINE (pg_request_cancel, "pg-request-cancel", 1, 0, 0,
           (SCM conn),
           "Request a cancellation on @var{conn}.\n"
           "Return #t iff the cancel request was successfully\n"
           "dispatched.  If not, @code{pg-error-message}\n"
           "tells why not.  Successful dispatch is no guarantee\n"
           "that the request will have any effect, however.\n"
           "Regardless of the return value,\n"
           "the client must continue with the normal\n"
           "result-reading sequence using @code{pg-get-result}.\n"
           "If the cancellation is effective, the current query\n"
           "will terminate early and return an error result.\n"
           "If the cancellation fails (say, because the backend\n"
           "was already done processing the query), then there\n"
           "will be no visible result at all.\n\n"
           "Note that if the current query is part of a transaction,\n"
           "cancellation will abort the whole transaction.")
{
#define FUNC_NAME s_pg_is_busy_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return (PQrequestCancel (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
#undef FUNC_NAME
}


/*
 * installation features
 */

#define SIMPLE_SYMBOL(s) \
  SCM_SYMBOL (pg_sym_ ## s, # s)

#define SYM(s)  (pg_sym_ ## s)

#ifdef HAVE_PQPROTOCOLVERSION
SIMPLE_SYMBOL (PQPROTOCOLVERSION);
#endif
#ifdef HAVE_PQRESULTERRORMESSAGE
SIMPLE_SYMBOL (PQRESULTERRORMESSAGE);
#endif
#ifdef HAVE_PQPASS
SIMPLE_SYMBOL (PQPASS);
#endif
#ifdef HAVE_PQBACKENDPID
SIMPLE_SYMBOL (PQBACKENDPID);
#endif
#ifdef HAVE_PQOIDVALUE
SIMPLE_SYMBOL (PQOIDVALUE);
#endif
#ifdef HAVE_PQBINARYTUPLES
SIMPLE_SYMBOL (PQBINARYTUPLES);
#endif
#ifdef HAVE_PQFMOD
SIMPLE_SYMBOL (PQFMOD);
#endif
#ifdef HAVE_PQSETNONBLOCKING
SIMPLE_SYMBOL (PQSETNONBLOCKING);
#endif
#ifdef HAVE_PQISNONBLOCKING
SIMPLE_SYMBOL (PQISNONBLOCKING);
#endif


/*
 * init
 */

static
void
init_module (void)
{
#ifdef USE_OLD_SMOB_INTERFACE
  static scm_smobfuns type_rec;
#endif
  extern void init_libpostgres_lo (void);

  INIT_PRINT (fprintf (stderr, "entered init_postgres function.\n"));

#ifdef USE_OLD_SMOB_INTERFACE

  /* add new scheme type for connections */
  type_rec.mark = xc_mark;
  type_rec.free = xc_free;
  type_rec.print = xc_display;
  type_rec.equalp = 0;
  pg_conn_tag.type_tag = scm_newsmob (&type_rec);

  /* add new scheme type for results */
  type_rec.mark = xr_mark;
  type_rec.free = xr_free;
  type_rec.print = xr_display;
  type_rec.equalp = 0;
  pg_result_tag.type_tag = scm_newsmob (&type_rec);

  /* add new scheme type for print options */
  type_rec.mark = sepo_mark;
  type_rec.free = sepo_free;
  type_rec.print = sepo_display;
  type_rec.equalp = 0;
  sepo_type_tag = scm_newsmob (&type_rec);

#else /* !USE_OLD_SMOB_INTERFACE */

  pg_conn_tag.type_tag = scm_make_smob_type ("PG-CONN", 0);
  scm_set_smob_mark (pg_conn_tag.type_tag, xc_mark);
  scm_set_smob_free (pg_conn_tag.type_tag, xc_free);
  scm_set_smob_print (pg_conn_tag.type_tag, xc_display);

  pg_result_tag.type_tag = scm_make_smob_type ("PG-RESULT", 0);
  scm_set_smob_mark (pg_result_tag.type_tag, xr_mark);
  scm_set_smob_free (pg_result_tag.type_tag, xr_free);
  scm_set_smob_print (pg_result_tag.type_tag, xr_display);

  sepo_type_tag = scm_make_smob_type ("PG-PRINT-OPTION", 0);
  scm_set_smob_mark (sepo_type_tag, sepo_mark);
  scm_set_smob_free (sepo_type_tag, sepo_free);
  scm_set_smob_print (sepo_type_tag, sepo_display);

#endif /* !USE_OLD_SMOB_INTERFACE */

#include "libpostgres.x"

  valid_print_option_keys
    = (scm_protect_object (SCM_LIST4 (pg_sym_field_sep,
                                      pg_sym_table_opt,
                                      pg_sym_caption,
                                      pg_sym_field_names)));

  valid_print_option_flags
    = (scm_protect_object
       (scm_append (SCM_LIST2
                    (SCM_LIST5 (pg_sym_header,
                                pg_sym_align,
                                pg_sym_standard,
                                pg_sym_html3,
                                pg_sym_expanded),
                     SCM_LIST5 (pg_sym_no_header,
                                pg_sym_no_align,
                                pg_sym_no_standard,
                                pg_sym_no_html3,
                                pg_sym_no_expanded)))));

  {
    int i;
    for (i = 0; i < pgrs_count; i++)
      pgrs[i].sym = scm_protect_object (gh_car (scm_sysintern0 (pgrs[i].s)));
  }

  init_libpostgres_lo ();

  goodies = SCM_EOL;

#define PUSH(x)  goodies = gh_cons (SYM (x), goodies)

#ifdef HAVE_PQPROTOCOLVERSION
  PUSH (PQPROTOCOLVERSION);
#endif
#ifdef HAVE_PQRESULTERRORMESSAGE
  PUSH (PQRESULTERRORMESSAGE);
#endif
#ifdef HAVE_PQPASS
  PUSH (PQPASS);
#endif
#ifdef HAVE_PQBACKENDPID
  PUSH (PQBACKENDPID);
#endif
#ifdef HAVE_PQOIDVALUE
  PUSH (PQOIDVALUE);
#endif
#ifdef HAVE_PQBINARYTUPLES
  PUSH (PQBINARYTUPLES);
#endif
#ifdef HAVE_PQFMOD
  PUSH (PQFMOD);
#endif
#ifdef HAVE_PQSETNONBLOCKING
  PUSH (PQSETNONBLOCKING);
#endif
#ifdef HAVE_PQISNONBLOCKING
  PUSH (PQISNONBLOCKING);
#endif

  goodies = scm_protect_object (goodies);
}

GH_MODULE_LINK_FUNC ("database postgres"
                     ,database_postgres
                     ,init_module)

/* libpostgres.c ends here */
