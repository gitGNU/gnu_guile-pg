/*  Guile-pg - A Guile interface to PostgreSQL
    Copyright (C) 1999, 2002, 2003 Free Software Foundation, Inc.

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

#include <config.h>

#include <stdio.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef HAVE_STRING_H
#include <string.h>
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
#define QUERY_BUF_LEN 8000

#ifdef GC_DEBUG
#define GC_PRINT(x) x
#else
#define GC_PRINT(x) (void)0;
#endif

#ifdef INIT_DEBUG
#define INIT_PRINT(x) x
#else
#define INIT_PRINT(x) (void)0;
#endif


/*
 * support
 */

static char *strncpy0 (char *dest, const char *source, int maxlen);
static char *strip_newlines (char *str);
void init_postgres (void);
void scm_init_database_interface_postgres_module (void);

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
guile_pg_sec_p (SCM obj)
{
  return (SCM_NIMP (obj) && SCM_CAR (obj) == pg_conn_tag.type_tag);
}

#define sec_p guile_pg_sec_p

scm_extended_dbconn *
guile_pg_sec_unbox (SCM obj)
{
  return ((scm_extended_dbconn *) SCM_CDR (obj));
}

#define sec_unbox guile_pg_sec_unbox

static SCM
sec_box (scm_extended_dbconn *sec)
{
  SCM z;

  SCM_ENTER_A_SECTION;
  SCM_NEWCELL (z);
  SCM_SETCDR (z, (SCM)sec);
  SCM_SETCAR (z, pg_conn_tag.type_tag);
  SCM_EXIT_A_SECTION;

  return z;
}

static int
sec_display (SCM exp, SCM port, scm_print_state *pstate)
{
  scm_extended_dbconn *sec = sec_unbox (exp);
  char *dbstr = PQdb (sec->dbconn);
  char *hoststr = PQhost (sec->dbconn);
  char *portstr = PQport (sec->dbconn);
  char *optionsstr = PQoptions (sec->dbconn);

  scm_puts ("#<PG-CONN:", port);
  scm_intprint (sec->count, 10, port); scm_putc (':', port);
  scm_puts (IFNULL (dbstr,"db?"), port); scm_putc (':', port);
  scm_puts (IFNULL (hoststr,"localhost"), port); scm_putc (':', port);
  scm_puts (IFNULL (portstr,"port?"), port); scm_putc (':', port);
  scm_puts (IFNULL (optionsstr,"options?"), port);
  scm_puts (">", port);

  return 1;
}

static SCM
sec_mark (SCM obj)
{
  scm_extended_dbconn *sec = sec_unbox (obj);

  GC_PRINT (fprintf (stderr, "marking PG-CONN %d\n", sec->count));
  return sec->client;
}

static scm_sizet
sec_free (SCM obj)
{
  scm_extended_dbconn *sec = sec_unbox (obj);
  scm_sizet size = sizeof (scm_extended_dbconn);

  GC_PRINT (fprintf (stderr, "sweeping PG-CONN %d\n", sec->count));

  /* close connection to postgres */
  if (sec->dbconn) {
    PQfinish (sec->dbconn);
  }

  /* free this object itself */
  free (sec);

  /* set extension data to NULL */
  SCM_SETCDR (obj, (SCM)NULL);

  return size;
}

static int
ser_p (SCM obj)
{
  return (SCM_NIMP (obj) && SCM_CAR (obj) == pg_result_tag.type_tag);
}

static scm_extended_result *
ser_unbox (SCM obj)
{
  return ((scm_extended_result*)SCM_CDR (obj));
}

static SCM
ser_box (scm_extended_result *ser)
{
  SCM z;

  SCM_ENTER_A_SECTION;
  SCM_NEWCELL (z);
  SCM_SETCDR (z, (SCM)ser);
  SCM_SETCAR (z, pg_result_tag.type_tag);
  SCM_EXIT_A_SECTION;

  return z;
}

static int
ser_display (SCM exp, SCM port, scm_print_state *pstate)
{
  scm_extended_result *ser = ser_unbox (exp);
  ExecStatusType status;
  int ntuples = 0;
  int nfields = 0;
  int pgrs_index;

  SCM_DEFER_INTS;
  status = PQresultStatus (ser->result);
  if (status == PGRES_TUPLES_OK) {
    ntuples = PQntuples (ser->result);
    nfields = PQnfields (ser->result);
  }
  SCM_ALLOW_INTS;

  for (pgrs_index = 0; pgrs_index < pgrs_count; pgrs_index++)
    if (status == pgrs[pgrs_index].n)
      break;

  scm_puts ("#<PG-RESULT:", port);
  scm_intprint (ser->count, 10, port); scm_putc (':', port);
  scm_puts (pgrs[pgrs_index].s, port); scm_putc (':', port);
  scm_intprint (ntuples, 10, port); scm_putc (':', port);
  scm_intprint (nfields, 10, port);
  scm_putc ('>', port);

  return 1;
}

static SCM
ser_mark (SCM obj)
{
  scm_extended_result *ser = ser_unbox (obj);

  GC_PRINT (fprintf (stderr, "marking PG-RESULT %d\n", ser->count));
  return ser->conn;
}

static scm_sizet
ser_free (SCM obj)
{
  scm_extended_result *ser = ser_unbox (obj);
  scm_sizet size = sizeof (scm_extended_result);

  GC_PRINT (fprintf (stderr, "sweeping PG-RESULT %d\n", ser->count));

  /* clear the result */
  if (ser->result)
    PQclear (ser->result);

  /* free this object itself */
  free (ser);

  /* set extension data to NULL */
  SCM_SETCDR (obj, (SCM)NULL);

  return size;
}

static char *strncpy0 (char *dest, const char *source, int maxlen)
{
  int len;

  (void) strncpy (dest, source, maxlen);
  if ((len = strlen (source)) < maxlen)
    *(dest + len) = '\0';
  else
    *(dest + maxlen - 1) = '\0';

  return dest;
}

static char *
strip_newlines (char *str)
{
  while (str[strlen (str) - 1] == '\n')
    str[strlen (str) - 1] = '\0';

  return str;
}


/*
 * everything (except printing and lo support)
 */

PG_DEFINE (pg_guile_pg_loaded, "pg-guile-pg-loaded", 0, 0, 0,
           (void),
           "Return @code{#t} indicating that the binary part of\n"
           "@code{guile-pg} is loaded.  Thus, to test if @code{guile-pg}\n"
           "is loaded, use:\n"
           "@lisp\n"
           "(defined? 'pg-guile-pg-loaded)\n"
           "@end lisp")
#define FUNC_NAME s_pg_guile_pg_loaded
{
  return SCM_BOOL_T;                    /* todo: other checks */
}
#undef FUNC_NAME

PG_DEFINE (pg_connectdb, "pg-connectdb", 1, 0, 0,
            (SCM constr),
            "Open a connection to a database.  @var{connect-string} should be\n"
            "a string consisting of zero or more space-separated @code{name=value} pairs.\n"
            "If the value contains spaces it must be enclosed in single quotes and any\n"
            "single quotes appearing in the value must be escaped using backslashes.\n"
            "Backslashes appearing in the value must similarly be escaped.  Note that\n"
            "if the @code{connect-string} is a Guile string literal then all the backslashes\n"
            "will themselves require to be escaped a second time.\n"
            "The @code{name} strings can be any of:\n\n"
            "@table @code\n\n"
            "@item host\n"
            "The host-name or dotted-decimal IP address of the host on which the postmaster\n"
            "is running.  If no @code{host=} sub-string is given then the host is assumed\n"
            "to be the value of the environment variable @code{PGHOST} or the local\n"
            "host if @code{PGHOST} is not defined.\n\n"
            "@item port\n"
            "The TCP or Unix socket on which the backend is listening.  If this is not\n"
            "specified then the value of the @code{PGHOST} environment variable is used.\n"
            "If that too is not defined then the default port 5432 is assumed.\n\n"
            "@item options\n"
            "A string containing the options to the backend server.  The options given\n"
            "here are in addition to the options given by the environment variable\n"
            "@code{PGOPTIONS}.  The options string should be a set of command line\n"
            "switches as would be passed to the backend.  See the @code{postgres(1)}\n"
            "man page for more details.\n\n"
            "@item tty\n"
            "A string defining the file or device on which error messages from the backend\n"
            "are to be displayed.  If this is empty (@code{""}) then the environment variable\n"
            "@code{PGTTY} is checked.  If the specified @code{tty} is a file then the file\n"
            "will be readable only by the user the postmaster runs as (usually\n"
            "@code{postgres}).  Similarly, if the specified @code{tty} is a device then it\n"
            "must have permissions allowing the postmaster user to write to it.\n\n"
            "@item dbname\n"
            "The name of the database.  If no @code{dbname=} sub-string is given then the\n"
            "database name is assumed to be that given by the value of the @code{PGDATABASE}\n"
            "environment variable, or the @code{USER} environment variable if the\n"
            "@code{PGDATABASE} environment variable is not defined.  If the @code{USER}\n"
            "environment variable is not specified either then the value of the\n"
            "@code{user} option is taken as the database name.\n\n"
            "@item user\n"
            "The login name of the user to authenticate.  If none is given then the\n"
            "@code{PGUSER} environment variable is checked.  If that is not given then the\n"
            "login of the user owning the process is used.\n\n"
            "@item password\n"
            "The password.  Whether or not this is used depends upon the contents of the\n"
            "@code{pg_hba.conf} file.  See the @code{pg_hba.conf(5)} man page for details.\n\n"
            "@item authtype\n"
            "This must be set to @code{password} if password authentication is in use,\n"
            "otherwise it must not be specified.\n\n"
            "@end table")
#define FUNC_NAME s_pg_connectdb
{
  scm_extended_dbconn *sec;
  SCM z;
  PGconn *dbconn;
  ConnStatusType connstat;
  char pgconstr[BUF_LEN];
  char pgerrormsg[BUF_LEN];

  SCM_ASSERT (SCM_NIMP (constr) && SCM_ROSTRINGP (constr), constr,
              SCM_ARG1, FUNC_NAME);
  strncpy0 (pgconstr, SCM_CHARS (constr), BUF_LEN);

  SCM_DEFER_INTS;;
  dbconn = PQconnectdb (pgconstr);
  strncpy0 (pgerrormsg, PQerrorMessage (dbconn), BUF_LEN);
  if ((connstat = PQstatus (dbconn)) == CONNECTION_BAD)
    PQfinish (dbconn);
  SCM_ALLOW_INTS;

  if (connstat == CONNECTION_BAD)
    scm_misc_error (FUNC_NAME, strip_newlines (pgerrormsg), SCM_EOL);

  z = sec_box ((scm_extended_dbconn*)
               scm_must_malloc (sizeof (scm_extended_dbconn), "PG-CONN"));
  sec = sec_unbox (z);

  sec->dbconn = dbconn;
  sec->count = ++pg_conn_tag.count;
  sec->client = SCM_BOOL_F;
  sec->fptrace = (FILE *) NULL;
  return z;
}
#undef FUNC_NAME

PG_DEFINE (pg_connection_p, "pg-connection?", 1, 0, 0,
           (SCM obj),
           "Return #t iff @var{obj} is a connection object\n"
           "returned by @code{pg-connectdb}.")
#define FUNC_NAME s_pg_connection_p
{
  return sec_p (obj) ? SCM_BOOL_T : SCM_BOOL_F;
}
#undef FUNC_NAME

PG_DEFINE (pg_reset, "pg-reset", 1, 0, 0,
           (SCM conn),
           "Reset the connection @var{conn} with the backend.\n"
           "Equivalent to closing the connection and re-opening it again\n"
           "with the same connect options as given to @code{pg-connectdb}.\n"
           "@var{conn} must be a valid @code{PG_CONN} object returned by\n"
           "@code{pg-connectdb}.")
#define FUNC_NAME s_pg_reset
{
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_DEFER_INTS;
  PQreset (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
}
#undef FUNC_NAME

PG_DEFINE (pg_get_client_data, "pg-get-client-data", 1, 0, 0,
           (SCM conn),
           "Return the the client data associated with @var{conn}.")
#define FUNC_NAME s_pg_get_client_data
{
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  return (sec_unbox (conn)->client);
}
#undef FUNC_NAME

PG_DEFINE (pg_set_client_data, "pg-set-client-data!", 2, 0, 0,
           (SCM conn, SCM data),
           "Associate @var{data} with @var{conn}.")
#define FUNC_NAME s_pg_set_client_data
{
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_DEFER_INTS;
  sec_unbox (conn)->client = data;
  SCM_ALLOW_INTS;
  return (data);
}
#undef FUNC_NAME

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
#define FUNC_NAME s_pg_exec
{
  scm_extended_result *ser;
  SCM z;
  PGconn *dbconn;
  PGresult *result;
  char pgquery[QUERY_BUF_LEN];

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (statement) && SCM_ROSTRINGP (statement),
              statement, SCM_ARG2, FUNC_NAME);

  SCM_DEFER_INTS;;

  strncpy0 (pgquery, SCM_CHARS (statement), QUERY_BUF_LEN);

  dbconn = sec_unbox (conn)->dbconn;

  if ((result= PQexec (dbconn, pgquery)) != NULL) {
    /* successfully exec'ed command; create data structure */
    SCM_ALLOW_INTS; /* Looks weird, but they're SCM_DEFERred in ser_box. */
    z = ser_box ((scm_extended_result *)
                 scm_must_malloc (sizeof (scm_extended_result), "PG-RESULT"));
    ser = ser_unbox (z);
    SCM_DEFER_INTS;

    /* initialize the dbconn */
    ser->result = result;
    ser->count = ++pg_result_tag.count;
    ser->conn = conn;
  } else {
    z = SCM_BOOL_F;
  }
  SCM_ALLOW_INTS;
  return z;
}
#undef FUNC_NAME

PG_DEFINE (pg_result_p, "pg-result?", 1, 0, 0,
           (SCM obj),
           "Return #t iff @var{obj} is a result object\n"
           "returned by @code{pg-exec}.")
#define FUNC_NAME s_pg_result_p
{
  return ser_p (obj) ? SCM_BOOL_T : SCM_BOOL_F;
}
#undef FUNC_NAME

PG_DEFINE (pg_error_message, "pg-error-message", 1, 0, 0,
           (SCM obj),
           "Return the most-recent error message that occurred on this\n"
           "connection, or an empty string if the previous @code{pg-exec}\n"
           "succeeded.")
#define FUNC_NAME s_pg_error_message
{
  char pgerrormsg[BUF_LEN];

#ifdef HAVE_PQRESULTERRORMESSAGE
  SCM_ASSERT ((sec_p (obj) || ser_p (obj)), obj, SCM_ARG1, FUNC_NAME);
#else
  SCM_ASSERT ((sec_p (obj)), obj, SCM_ARG1, FUNC_NAME);
#endif
  SCM_DEFER_INTS;
#ifdef HAVE_PQRESULTERRORMESSAGE
  if (sec_p (obj))
#endif
    strncpy0 (pgerrormsg, PQerrorMessage (sec_unbox (obj)->dbconn), BUF_LEN);
#ifdef HAVE_PQRESULTERRORMESSAGE
  else
    strncpy0 (pgerrormsg,
              (const char *) PQresultErrorMessage (ser_unbox (obj)->result), BUF_LEN);
#endif
  SCM_ALLOW_INTS;

  return scm_makfrom0str (strip_newlines (pgerrormsg));
}
#undef FUNC_NAME

PG_DEFINE (pg_get_db, "pg-get-db", 1, 0, 0,
           (SCM conn),
           "Return a string containing the name of the database\n"
           "to which @var{conn} represents a connection.")
#define FUNC_NAME s_pg_get_db
{
  const char *rv;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  rv = PQdb (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#undef FUNC_NAME

PG_DEFINE (pg_get_user, "pg-get-user", 1, 0, 0,
           (SCM conn),
           "Return a string containing the user name used to\n"
           "authenticate the connection @var{conn}.")
#define FUNC_NAME s_pg_get_user
{
  const char *rv;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  rv = PQuser (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#undef FUNC_NAME

#ifdef HAVE_PQPASS
PG_DEFINE (pg_get_pass, "pg-get-pass", 1, 0, 0,
           (SCM conn),
           "Return a string containing the password used to\n"
           "authenticate the connection @var{conn}.")
#define FUNC_NAME s_pg_get_pass
{
  const char *rv;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  rv = PQpass (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#undef FUNC_NAME
#endif /* HAVE_PQPASS */

PG_DEFINE (pg_get_host, "pg-get-host", 1, 0, 0,
           (SCM conn),
           "Return a string containing the name of the host to which\n"
           "@var{conn} represents a connection.")
#define FUNC_NAME s_pg_get_host
{
  const char *rv;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  rv = PQhost (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#undef FUNC_NAME

PG_DEFINE (pg_get_port,"pg-get-port", 1, 0, 0,
           (SCM conn),
           "Return a string containing the port number to which\n"
           "@var{conn} represents a connection.")
#define FUNC_NAME s_pg_get_port
{
  const char *rv;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  rv = PQport (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#undef FUNC_NAME

PG_DEFINE (pg_get_tty, "pg-get-tty", 1, 0, 0,
           (SCM conn),
           "Return a string containing the the name of the\n"
           "diagnostic tty for @var{conn}.")
#define FUNC_NAME s_pg_get_tty
{
  const char *rv;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  rv = PQtty (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#undef FUNC_NAME

PG_DEFINE (pg_get_options, "pg-get-options", 1, 0, 0,
           (SCM conn),
           "Return a string containing the the options string for @var{conn}.")
#define FUNC_NAME s_pg_get_options
{
  const char *rv;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  rv = PQoptions (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#undef FUNC_NAME

PG_DEFINE (pg_get_connection, "pg-get-connection", 1, 0, 0,
           (SCM result),
           "Return the @code{PG_CONN} object representing the connection\n"
           "from which a @var{result} was returned.")
#define FUNC_NAME s_pg_get_connection
{
  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  return (ser_unbox (result)->conn);
}
#undef FUNC_NAME

#ifdef HAVE_PQBACKENDPID
PG_DEFINE (pg_backend_pid, "pg-backend-pid", 1, 0, 0,
           (SCM conn),
           "Return an integer which is the the PID of the backend\n"
           "process for @var{conn}.")
#define FUNC_NAME s_pg_backend_pid
{
  int pid;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  pid = PQbackendPID (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  return SCM_MAKINUM (pid);
}
#undef FUNC_NAME
#endif /* HAVE_PQBACKENDPID */

PG_DEFINE (pg_result_status, "pg-result-status", 1, 0, 0,
           (SCM result),
           "Return the symbolic status of a @code{PG_RESULT} object\n"
           "returned by @code{pg-exec}.")
#define FUNC_NAME s_pg_result_status
{
  int result_status;
  int pgrs_index;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  result_status = PQresultStatus (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  for (pgrs_index = 0; pgrs_index < pgrs_count; pgrs_index++)
    if (result_status == pgrs[pgrs_index].n)
      return pgrs[pgrs_index].sym;

  /* FIXME: Although we should never get here, be slackful for now.  */
  /* abort(); */
  return SCM_MAKINUM (result_status);
}
#undef FUNC_NAME

PG_DEFINE (pg_ntuples, "pg-ntuples", 1, 0, 0,
           (SCM result),
           "Return the number of tuples in @var{result}.")
#define FUNC_NAME s_pg_ntuples
{
  int ntuples;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  ntuples = PQntuples (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  return SCM_MAKINUM (ntuples);
}
#undef FUNC_NAME

PG_DEFINE (pg_nfields, "pg-nfields", 1, 0, 0,
           (SCM result),
           "Return the number of fields in @var{result}.")
#define FUNC_NAME s_pg_nfields
{
  SCM scm_inum;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  scm_inum = SCM_MAKINUM (PQnfields (ser_unbox (result)->result));
  SCM_ALLOW_INTS;

  return scm_inum;
}
#undef FUNC_NAME

PG_DEFINE (pg_cmdtuples, "pg-cmdtuples", 1, 0, 0,
           (SCM result),
           "Return the number of tuples in @var{result} affected by a command.\n"
           "This is a string which is empty in the case of commands\n"
           "like @code{CREATE TABLE}, @code{GRANT}, @code{REVOKE} etc.\n"
           "which don't affect tuples.")
#define FUNC_NAME s_pg_cmdtuples
{
  const char *cmdtuples;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  cmdtuples = PQcmdTuples (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (cmdtuples);
}
#undef FUNC_NAME

PG_DEFINE (pg_oid_status, "pg-oid-status", 1, 0, 0,
           (SCM result),
           "Return a string which contains the integer OID (greater than\n"
           "or equal to 0) of the tuple inserted, or is empty if the\n"
           "command to which @var{result} pertains was not @code{INSERT}.")
#define FUNC_NAME s_pg_oid_status
{
  const char *oid_status;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  oid_status = PQoidStatus (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (oid_status);
}
#undef FUNC_NAME

#ifdef HAVE_PQOIDVALUE
PG_DEFINE (pg_oid_value, "pg-oid-value", 1, 0, 0,
           (SCM result),
           "If the @var{result} is that of an SQL @code{INSERT} command,\n"
           "return the integer OID of the inserted tuple, otherwise return\n"
           "@code{#f}.")
#define FUNC_NAME s_pg_oid_value
{
  Oid oid_value;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  oid_value = PQoidValue (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  if (oid_value == InvalidOid)
    return SCM_BOOL_F;

  return SCM_MAKINUM (oid_value);
}
#undef FUNC_NAME
#endif /* HAVE_PQOIDVALUE */

PG_DEFINE (pg_fname, "pg-fname", 2, 0, 0,
           (SCM result, SCM num),
           "Return a string containing the canonical lower-case name\n"
           "of the field number @var{num} in @var{result}.  SQL variables\n"
           "and field names are not case-sensitive.")
#define FUNC_NAME s_pg_fname
{
  int field;
  const char *fname;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, FUNC_NAME);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (result)->result) && field >= 0) {
    fname = PQfname (ser_unbox (result)->result, field);
    SCM_ALLOW_INTS;
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (FUNC_NAME, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  return scm_makfrom0str (fname);
}
#undef FUNC_NAME

PG_DEFINE (pg_fnumber, "pg-fnumber", 2, 0, 0,
           (SCM result, SCM fname),
           "Return the integer field-number corresponding to field\n"
           "@var{fname} if this exists in @var{result}, or @code{-1}\n"
           "otherwise.")
#define FUNC_NAME s_pg_fnumber
{
  int fnum;
  char name[BUF_LEN];

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (fname) && SCM_ROSTRINGP (fname), fname,
              SCM_ARG2, FUNC_NAME);

  strncpy0 (name, SCM_CHARS (fname), BUF_LEN);

  SCM_DEFER_INTS;
  fnum = PQfnumber (ser_unbox (result)->result, name);
  SCM_ALLOW_INTS;

  return SCM_MAKINUM (fnum);
}
#undef FUNC_NAME

PG_DEFINE (pg_ftype, "pg-ftype", 2, 0, 0,
           (SCM result, SCM num),
           "Return the PostgreSQL internal integer representation of\n"
           "the type of the given attribute.  The integer is actually an\n"
           "OID (object ID) which can be used as the primary key to\n"
           "reference a tuple from the system table @code{pg_type}.  A\n"
           "@code{misc-error} is thrown if the @code{field-number} is\n"
           "not valid for the given @code{result}.")
#define FUNC_NAME s_pg_ftype
{
  int field;
  int ftype;
  SCM scm_inum;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, FUNC_NAME);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (result)->result) && field >= 0) {
    ftype = PQftype (ser_unbox (result)->result, field);
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (FUNC_NAME, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (ftype);
  return scm_inum;
}
#undef FUNC_NAME

PG_DEFINE (pg_fsize, "pg-fsize", 2, 0, 0,
           (SCM result, SCM num),
           "Return the size of a @var{result} field @var{num} in bytes,\n"
           "or -1 if the field is variable-length.")
#define FUNC_NAME s_pg_fsize
{
  int field;
  int fsize;
  SCM scm_inum;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, FUNC_NAME);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (result)->result) && field >= 0) {
    fsize = PQfsize (ser_unbox (result)->result, field);
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (FUNC_NAME, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (fsize);
  return scm_inum;
}
#undef FUNC_NAME

PG_DEFINE (pg_getvalue, "pg-getvalue", 3, 0, 0,
           (SCM result, SCM stuple, SCM sfield),
           "Return a string containing the value of the attribute\n"
           "@var{sfield}, tuple @var{stuple} of @var{result}.  It is\n"
           "up to the caller to convert this to the required type.")
#define FUNC_NAME s_pg_getvalue
{
  int maxtuple, tuple;
  int maxfield, field;
  const char *val;
#ifdef HAVE_PQBINARYTUPLES
  int isbinary, veclen = 0;
#endif
  SCM srv;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_IMP (stuple) && SCM_INUMP (stuple), stuple, SCM_ARG2,
              FUNC_NAME);
  SCM_ASSERT (SCM_IMP (sfield) && SCM_INUMP (sfield), sfield, SCM_ARG3,
              FUNC_NAME);
  tuple = SCM_INUM (stuple);
  field = SCM_INUM (sfield);

  SCM_DEFER_INTS;
  maxtuple = PQntuples (ser_unbox (result)->result);
  maxfield = PQnfields (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  SCM_ASSERT (tuple < maxtuple && tuple >= 0, stuple, SCM_OUTOFRANGE,
              FUNC_NAME);
  SCM_ASSERT (field < maxfield && field >= 0, sfield, SCM_OUTOFRANGE,
              FUNC_NAME);
  SCM_DEFER_INTS;
  val = PQgetvalue (ser_unbox (result)->result, tuple, field);
#ifdef HAVE_PQBINARYTUPLES
  if ((isbinary = PQbinaryTuples (ser_unbox (result)->result)) != 0)
    veclen = PQgetlength (ser_unbox (result)->result, tuple, field);
#endif
  SCM_ALLOW_INTS;

#ifdef HAVE_PQBINARYTUPLES
  if (isbinary)
    srv = scm_makfromstr (val, veclen, 0);
  else
#endif
    srv = scm_makfrom0str (val);

  return srv;
}
#undef FUNC_NAME

PG_DEFINE (pg_getlength, "pg-getlength", 3, 0, 0,
           (SCM result, SCM stuple, SCM sfield),
           "The size of the datum in bytes.")
#define FUNC_NAME s_pg_getlength
{
  int maxtuple, tuple;
  int maxfield, field;
  int len;
  SCM ret;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_IMP (stuple) && SCM_INUMP (stuple), stuple, SCM_ARG2,
              FUNC_NAME);
  SCM_ASSERT (SCM_IMP (sfield) && SCM_INUMP (sfield), sfield, SCM_ARG3,
              FUNC_NAME);
  tuple = SCM_INUM (stuple);
  field = SCM_INUM (sfield);

  SCM_DEFER_INTS;
  maxtuple = PQntuples (ser_unbox (result)->result);
  maxfield = PQnfields (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  SCM_ASSERT (tuple < maxtuple && tuple >= 0, stuple, SCM_OUTOFRANGE,
              FUNC_NAME);
  SCM_ASSERT (field < maxfield && field >= 0, sfield, SCM_OUTOFRANGE,
              FUNC_NAME);
  SCM_DEFER_INTS;
  len = PQgetlength (ser_unbox (result)->result, tuple, field);
  SCM_ALLOW_INTS;

  ret = SCM_MAKINUM (len);
  return ret;
}
#undef FUNC_NAME

PG_DEFINE (pg_getisnull, "pg-getisnull", 3, 0, 0,
           (SCM result, SCM stuple, SCM sfield),
           "Return @code{#t} if the attribute is @code{NULL},\n"
           "@code{#f} otherwise.")
#define FUNC_NAME s_pg_getisnull
{
  int maxtuple, tuple;
  int maxfield, field;
  SCM scm_bool;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_IMP (stuple) && SCM_INUMP (stuple), stuple, SCM_ARG2,
              FUNC_NAME);
  SCM_ASSERT (SCM_IMP (sfield) && SCM_INUMP (sfield), sfield, SCM_ARG3,
              FUNC_NAME);
  tuple = SCM_INUM (stuple);
  field = SCM_INUM (sfield);

  SCM_DEFER_INTS;
  maxtuple = PQntuples (ser_unbox (result)->result);
  maxfield = PQnfields (ser_unbox (result)->result);
  SCM_ALLOW_INTS;

  SCM_ASSERT (tuple < maxtuple && tuple >= 0, stuple, SCM_OUTOFRANGE,
              FUNC_NAME);
  SCM_ASSERT (field < maxfield && field >= 0, sfield, SCM_OUTOFRANGE,
              FUNC_NAME);
  SCM_DEFER_INTS;
  if (PQgetisnull (ser_unbox (result)->result, tuple, field))
    scm_bool = SCM_BOOL_T;
  else
    scm_bool = SCM_BOOL_F;
  SCM_ALLOW_INTS;

  return scm_bool;
}
#undef FUNC_NAME

#ifdef HAVE_PQBINARYTUPLES
PG_DEFINE (pg_binary_tuples, "pg-binary-tuples?", 1, 0, 0,
           (SCM result),
           "Return @code{#t} if @var{result} contains binary tuple\n"
           "data, @code{#f} otherwise.")
#define FUNC_NAME s_pg_binary_tuples
{
  SCM rv;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  if (PQbinaryTuples (ser_unbox (result)->result))
    rv = SCM_BOOL_T;
  else
    rv = SCM_BOOL_F;
  SCM_ALLOW_INTS;

  return rv;
}
#undef FUNC_NAME
#endif /* HAVE_PQBINARYTUPLES */

#ifdef HAVE_PQFMOD
PG_DEFINE (pg_fmod, "pg-fmod", 2, 0, 0,
           (SCM result, SCM num),
           "Return the integer type-specific modification data for\n"
           "the given field (field number @var{num}) of @var{result}.")
#define FUNC_NAME s_pg_fmod
{
  int field;
  int fmod;
  SCM scm_inum;

  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, FUNC_NAME);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (result)->result) && field >= 0) {
    fmod = PQfmod (ser_unbox (result)->result, field);
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (FUNC_NAME, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (fmod);
  return scm_inum;
}
#undef FUNC_NAME
#endif /* HAVE_PQFMOD */

PG_DEFINE (pg_guile_pg_version, "pg-guile-pg-version", 0, 0, 0,
           (void),
           "Return a string giving the version of @code{guile-pg}.\n"
           "The form is \"M.m\" giving major and minor versions.")
#define FUNC_NAME s_pg_guile_pg_version
{
  return scm_makfrom0str (VERSION);
}
#undef FUNC_NAME

PG_DEFINE (pg_getline, "pg-getline", 1, 0, 0,
           (SCM conn),
           "Read a line from a connection on which a @code{COPY <table> TO\n"
           "STDOUT} has been issued.  Return a string from the connection.\n"
           "If the returned string consists of only a backslash followed by\n"
           "a full stop, then the results from the @code{COPY} command have\n"
           "all been read and @code{pg-endcopy} should be called to\n"
           "resynchronise the connection before any further calls to\n"
           "@code{pg-exec} on this connection.")
#define FUNC_NAME s_pg_getline
{
  char buf[BUF_LEN];
  int ret = 1;
  SCM str = SCM_UNDEFINED;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  while (ret != 0 && ret != EOF) {
    SCM_DEFER_INTS;
    ret = PQgetline (sec_unbox (conn)->dbconn, buf, BUF_LEN);
    SCM_ALLOW_INTS;
    if (str == SCM_UNDEFINED)
      str = scm_makfrom0str (buf);
    else
      str = scm_string_append (SCM_LIST2 (str, scm_makfrom0str (buf)));
  }
  return str;
}
#undef FUNC_NAME

PG_DEFINE (pg_putline, "pg-putline", 2, 0, 0,
           (SCM conn, SCM str),
           "Write a line to the connection on which a @code{COPY <table>\n"
           "FROM STDIN} has been issued.  The lines written should include\n"
           "the final newline characters.  The last line should be a\n"
           "backslash, followed by a full-stop.  After this, the\n"
           "@code{pg-endcopy} procedure should be called for this\n"
           "connection before any further @code{pg-exec} call is made.\n"
           "The return value is undefined.")
#define FUNC_NAME s_pg_putline
{
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (str)&&SCM_ROSTRINGP (str), str, SCM_ARG2, FUNC_NAME);
  SCM_DEFER_INTS;
#ifdef HAVE_PQPUTNBYTES
  PQputnbytes (sec_unbox (conn)->dbconn, SCM_CHARS (str), SCM_ROLENGTH (str));
#else
  PQputline (sec_unbox (conn)->dbconn, SCM_CHARS (str));
#endif
  SCM_ALLOW_INTS;
  return SCM_UNSPECIFIED;
}
#undef FUNC_NAME

PG_DEFINE (pg_endcopy, "pg-endcopy", 1, 0, 0,
           (SCM conn),
           "Resynchronize with the backend process.  This procedure\n"
           "must be called after the last line of a table has been\n"
           "transferred using @code{pg-getline} or ???.\n\n"
           "Return an integer: zero if successful, non-zero otherwise.")
#define FUNC_NAME s_pg_endcopy
{
  int ret;
  SCM scm_inum;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_DEFER_INTS;
  ret = PQendcopy (sec_unbox (conn)->dbconn);
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (ret);
  return scm_inum;
}
#undef FUNC_NAME

PG_DEFINE (pg_trace, "pg-trace", 2, 0, 0,
           (SCM conn, SCM port),
           "Start outputting low-level trace information on the\n"
           "connection @var{conn} to @var{port}, which must have been\n"
           "opened for writing.  This trace is more useful for debugging\n"
           "Postgres than it is for debugging applications.\n"
           "The return value is unspecified.")
#define FUNC_NAME s_pg_trace
{
  struct scm_fport *fp = SCM_FSTREAM (port);
  int fd;
  FILE *fpout;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (sec_unbox (conn)->fptrace == NULL, conn, SCM_ARG1, FUNC_NAME);
  port = SCM_COERCE_OUTPORT (port);
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPOUTFPORTP (port),port,SCM_ARG2,FUNC_NAME);

  SCM_SYSCALL (fd = dup (fp->fdes));
  if (fd == -1)
    scm_syserror (FUNC_NAME);
  SCM_SYSCALL (fpout = fdopen (fd, "w"));
  if (fpout == NULL)
    scm_syserror (FUNC_NAME);

  SCM_DEFER_INTS;
  PQtrace (sec_unbox (conn)->dbconn, fpout);
  sec_unbox (conn)->fptrace = fpout;
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
}
#undef FUNC_NAME

PG_DEFINE (pg_untrace, "pg-untrace", 1, 0, 0,
           (SCM conn),
           "Stop tracing on connection @var{conn}.\n"
           "The return value is unspecified.")
#define FUNC_NAME s_pg_untrace
{
  int ret;
  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, FUNC_NAME);

  SCM_DEFER_INTS;
  PQuntrace (sec_unbox (conn)->dbconn);
  SCM_SYSCALL (ret = fclose (sec_unbox (conn)->fptrace));
  sec_unbox (conn)->fptrace = (FILE *) NULL;
  SCM_ALLOW_INTS;
  if (ret)
    scm_syserror (FUNC_NAME);

  return SCM_UNSPECIFIED;
}
#undef FUNC_NAME


/*
 * printing -- this is arguably more trouble than it's worth
 */

static long sepo_type_tag;

static int
sepo_p (SCM obj)
{
  return (SCM_NIMP (obj) && SCM_CAR (obj) == sepo_type_tag);
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
  return ((PQprintOpt *) SCM_CDR (obj));
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
    if (p) {                                    \
      size += 1 + strlen (p);                   \
      free (p);                                 \
    }                                           \
  } while (0)

  _FREE_STRING (po->fieldSep);
  _FREE_STRING (po->tableOpt);
  _FREE_STRING (po->caption);

  if (po->fieldName) {
    int i = 0;
    while (po->fieldName[i]) {
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
           "@item expand: Expand tables.\n"
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
#define FUNC_NAME s_pg_make_print_options
{
  PQprintOpt *po;
  int count = 0;                        /* of substnames */
  SCM check, substnames = SCM_BOOL_F, flags = SCM_EOL, keys = SCM_EOL;

  SCM_ASSERT (SCM_NULLP (spec) || SCM_CONSP (spec),
              spec, SCM_ARG1, FUNC_NAME);

  /* hairy validation/collection: symbols in `flags', pairs in `keys' */
  check = spec;
  while (SCM_NNULLP (check)) {
    SCM head = SCM_CAR (check);
    if (SCM_SYMBOLP (head)) {
      SCM_ASSERT (SCM_NFALSEP (scm_memq (head, valid_print_option_flags)),
                  head, SCM_ARG1, FUNC_NAME);
      flags = scm_cons (head, flags);
    } else if (SCM_CONSP (head)) {
      SCM key = SCM_CAR (head);
      SCM val = SCM_CDR (head);
      SCM_ASSERT (SCM_NFALSEP (scm_memq (key, valid_print_option_keys)),
                  key, SCM_ARG1, FUNC_NAME);
      if (key == pg_sym_field_names) {
        SCM_ASSERT (SCM_NNULLP (val), head, SCM_ARG1, FUNC_NAME);
        while (SCM_NNULLP (val)) {
          SCM_ASSERT (SCM_STRINGP (SCM_CAR (val)), head, SCM_ARG1, FUNC_NAME);
          count++;
          val = SCM_CDR (val);
        }
        substnames = SCM_CDR (head);    /* i.e., `val' */
      } else {
        SCM_ASSERT (SCM_STRINGP (val), val, SCM_ARG1, FUNC_NAME);
        keys = scm_cons (head, keys);
      }
    }
    check = SCM_CDR (check);
  }

  po = scm_must_malloc (sizeof (PQprintOpt), "PG-PRINT-OPTION");

#define _FLAG_CHECK(m)                                  \
  (SCM_NFALSEP (scm_memq (pg_sym_no_ ## m, flags))      \
   ? 0 : (SCM_NFALSEP (scm_memq (pg_sym_ ## m, flags))  \
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
    po->m = (SCM_NFALSEP (stemp)                        \
             ? strdup (SCM_ROCHARS (stemp))             \
             : (default_print_options.m                 \
                ? strdup (default_print_options.m)      \
                : NULL));                               \
  } while (0)

  _STRING_CHECK_SETX (field_sep, fieldSep);
  _STRING_CHECK_SETX (table_opt, tableOpt);
  _STRING_CHECK_SETX (caption, caption);
#undef _STRING_CHECK_SETX

  if (SCM_FALSEP (substnames)) {
    po->fieldName = NULL;
  } else {
    int i;
    po->fieldName = (char **) scm_must_malloc ((1 + count) * sizeof (char *),
                                               "PG-PRINT-OPTION fieldname");
    po->fieldName[count] = NULL;
    for (i = 0; i < count; i++) {
      po->fieldName[i] = strdup (SCM_ROCHARS (SCM_CAR (substnames)));
      substnames = SCM_CDR (substnames);
    }
  }

  return sepo_box (po);
}
#undef FUNC_NAME

PG_DEFINE (pg_print, "pg-print", 1, 1, 0,
           (SCM result, SCM options),
           "Display @var{result} to current output port.\n"
           "Optional second arg @var{options} is a pg-print options\n"
           "object returned by @code{pg-make-print-options}, q.v.")
#define FUNC_NAME s_pg_print
{
  SCM_ASSERT (ser_p (result), result, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (sepo_p (options), options, SCM_ARG2, FUNC_NAME);

  PQprint (stdout, ser_unbox (result)->result, sepo_unbox (options));

  return SCM_UNSPECIFIED;
}
#undef FUNC_NAME


/*
 * init
 */

void
init_postgres (void)
{
#ifdef USE_OLD_SMOB_INTERFACE
  static scm_smobfuns type_rec;
#endif
  extern void init_libpostgres_lo (void);

  INIT_PRINT (fprintf (stderr, "entered init_postgres function.\n"));

#ifdef USE_OLD_SMOB_INTERFACE

  /* add new scheme type for connections */
  type_rec.mark = sec_mark;
  type_rec.free = sec_free;
  type_rec.print = sec_display;
  type_rec.equalp = 0;
  pg_conn_tag.type_tag = scm_newsmob (&type_rec);

  /* add new scheme type for results */
  type_rec.mark = ser_mark;
  type_rec.free = ser_free;
  type_rec.print = ser_display;
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
  scm_set_smob_mark (pg_conn_tag.type_tag, sec_mark);
  scm_set_smob_free (pg_conn_tag.type_tag, sec_free);
  scm_set_smob_print (pg_conn_tag.type_tag, sec_display);

  pg_result_tag.type_tag = scm_make_smob_type ("PG-RESULT", 0);
  scm_set_smob_mark (pg_result_tag.type_tag, ser_mark);
  scm_set_smob_free (pg_result_tag.type_tag, ser_free);
  scm_set_smob_print (pg_result_tag.type_tag, ser_display);

  sepo_type_tag = scm_make_smob_type ("PG-PRINT-OPTION", 0);
  scm_set_smob_mark (sepo_type_tag, sepo_mark);
  scm_set_smob_free (sepo_type_tag, sepo_free);
  scm_set_smob_print (sepo_type_tag, sepo_display);

#endif /* !USE_OLD_SMOB_INTERFACE */

#include <libpostgres.x>

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
      pgrs[i].sym = scm_protect_object (SCM_CAR (scm_sysintern0 (pgrs[i].s)));
  }

  init_libpostgres_lo ();

  return;
}

void
scm_init_database_interface_postgres_module (void)
{
  INIT_PRINT (fprintf (stderr, "calling scm_register_module_xxx.\n"));
  scm_register_module_xxx ("database interface postgres",
                           (void *) init_postgres);
}

/* libpostgres.c ends here */
