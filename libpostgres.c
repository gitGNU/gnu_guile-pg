/*  Guile-pg - A Guile interface to PostgreSQL
    Copyright (C) 1999, 2002 Free Software Foundation, Inc.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

    Guile-pg was written by Ian Grant <Ian.Grant@cl.cam.ac.uk>
*/

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

static char *strncpy0 (char *dest, const char *source, int maxlen);
static char *strip_newlines (char *str);
void init_postgres (void);
void scm_init_database_interface_postgres_module (void);

typedef struct _smob_tag {
    long  type_tag; /* type tag */
    int   count;
} smob_tag;

static smob_tag pg_conn_tag, pg_result_tag;

#define MAX_SSI 8

static char *ser_status_str[] = {
   "PGRES_EMPTY_QUERY",
   "PGRES_COMMAND_OK",
   "PGRES_TUPLES_OK",
   "PGRES_COPY_OUT",
   "PGRES_COPY_IN",
   "PGRES_BAD_RESPONSE",
   "PGRES_NONFATAL_ERROR",
   "PGRES_FATAL_ERROR",
   "UNKNOWN RESULT STATUS"              /* see "- 1" immed below */
};

static int ser_status_str_count = ((sizeof (ser_status_str) /
                                    sizeof (char *))
                                   - 1);

static int ser_status[] = {
   PGRES_EMPTY_QUERY,
   PGRES_COMMAND_OK,
   PGRES_TUPLES_OK,
   PGRES_COPY_OUT,
   PGRES_COPY_IN,
   PGRES_BAD_RESPONSE,
   PGRES_NONFATAL_ERROR,
   PGRES_FATAL_ERROR
};

/*
 * boxing, unboxing, gc functions
 */
int
sec_p (SCM obj)
{
  return (SCM_NIMP (obj) && SCM_CAR (obj) == pg_conn_tag.type_tag);
}

scm_extended_dbconn *
sec_unbox (SCM obj)
{
  return ((scm_extended_dbconn *) SCM_CDR (obj));
}

SCM
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

int
ser_p (SCM obj)
{
  return (SCM_NIMP (obj) && SCM_CAR (obj) == pg_result_tag.type_tag);
}

scm_extended_result *
ser_unbox (SCM obj)
{
  return ((scm_extended_result*)SCM_CDR (obj));
}

SCM
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
  int ser_status_index;

  SCM_DEFER_INTS;
  status = PQresultStatus (ser->result);
  if (status == PGRES_TUPLES_OK) {
    ntuples = PQntuples (ser->result);
    nfields = PQnfields (ser->result);
  }
  SCM_ALLOW_INTS;

  for (ser_status_index = 0; ser_status_index < MAX_SSI; ser_status_index++)
    if (status == ser_status[ser_status_index])
      break;

  scm_puts ("#<PG-RESULT:", port);
  scm_intprint (ser->count, 10, port); scm_putc (':', port);
  scm_puts (ser_status_str[ser_status_index], port); scm_putc (':', port);
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

SCM_PROC (s_pg_guile_pg_loaded, "pg-guile-pg-loaded", 0, 0, 0, pg_guile_pg_loaded);

static SCM
pg_guile_pg_loaded (void)
{
  return SCM_BOOL_T;
}

SCM_PROC (s_pg_connectdb, "pg-connectdb", 1, 0, 0, pg_connectdb);

static SCM
pg_connectdb (SCM constr)
{
  scm_extended_dbconn *sec;
  SCM z;
  PGconn *dbconn;
  ConnStatusType connstat;
  char pgconstr[BUF_LEN];
  char pgerrormsg[BUF_LEN];

  SCM_ASSERT (SCM_NIMP (constr) && SCM_ROSTRINGP (constr), constr,
              SCM_ARG1, s_pg_connectdb);
  strncpy0 (pgconstr, SCM_CHARS (constr), BUF_LEN);

  SCM_DEFER_INTS;;
  dbconn = PQconnectdb (pgconstr);
  strncpy0 (pgerrormsg, PQerrorMessage (dbconn), BUF_LEN);
  if ((connstat = PQstatus (dbconn)) == CONNECTION_BAD)
    PQfinish (dbconn);
  SCM_ALLOW_INTS;

  if (connstat == CONNECTION_BAD)
    scm_misc_error (s_pg_connectdb, strip_newlines (pgerrormsg), SCM_EOL);

  z = sec_box ((scm_extended_dbconn*)
               scm_must_malloc (sizeof (scm_extended_dbconn), "PG-CONN"));
  sec = sec_unbox (z);

  sec->dbconn = dbconn;
  sec->count = ++pg_conn_tag.count;
  sec->client = SCM_BOOL_F;
  sec->fptrace = (FILE *) NULL;
  return z;
}

SCM_PROC (s_pg_reset, "pg-reset", 1, 0, 0, pg_reset);

static SCM
pg_reset (SCM obj)
{
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_reset);
  SCM_DEFER_INTS;
  PQreset (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
}

SCM_PROC (s_pg_get_client_data,"pg-get-client-data",1,0, 0, pg_get_client_data);

static SCM
pg_get_client_data (SCM obj)
{
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_client_data);
  return (sec_unbox (obj)->client);
}

SCM_PROC (s_pg_set_client_data,"pg-set-client-data!",2,0,0, pg_set_client_data);

static SCM
pg_set_client_data (SCM obj, SCM client)
{
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_set_client_data);
  SCM_DEFER_INTS;
  sec_unbox (obj)->client = client;
  SCM_ALLOW_INTS;
  return (client);
}

SCM_PROC (s_pg_exec,"pg-exec",2,0,0, pg_exec);

static SCM
pg_exec (SCM obj, SCM cmd)
{
  scm_extended_result *ser;
  SCM z;
  PGconn *dbconn;
  PGresult *result;
  char pgquery[QUERY_BUF_LEN];

  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_exec);
  SCM_ASSERT (SCM_NIMP (cmd) && SCM_ROSTRINGP (cmd), cmd, SCM_ARG2, s_pg_exec);

  SCM_DEFER_INTS;;

  strncpy0 (pgquery, SCM_CHARS (cmd), QUERY_BUF_LEN);

  dbconn = sec_unbox (obj)->dbconn;

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
    ser->conn = obj;
  } else {
    z = SCM_BOOL_F;
  }
  SCM_ALLOW_INTS;
  return z;
}

SCM_PROC (s_pg_error_message,"pg-error-message",1,0,0, pg_error_message);

static SCM
pg_error_message (SCM obj)
{
  char pgerrormsg[BUF_LEN];

#ifdef HAVE_PQRESULTERRORMESSAGE
  SCM_ASSERT ((sec_p (obj) || ser_p (obj)), obj, SCM_ARG1, s_pg_error_message);
#else
  SCM_ASSERT ((sec_p (obj)), obj, SCM_ARG1, s_pg_error_message);
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

SCM_PROC (s_pg_get_db,"pg-get-db",1,0,0, pg_get_db);

static SCM
pg_get_db (SCM obj)
{
  const char *rv;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_db);

  SCM_DEFER_INTS;
  rv = PQdb (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}

SCM_PROC (s_pg_get_user,"pg-get-user",1,0,0, pg_get_user);

static SCM
pg_get_user (SCM obj)
{
  const char *rv;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_user);

  SCM_DEFER_INTS;
  rv = PQuser (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}

#ifdef HAVE_PQPASS
SCM_PROC (s_pg_get_pass,"pg-get-pass",1,0,0, pg_get_pass);

static SCM
pg_get_pass (SCM obj)
{
  const char *rv;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_pass);

  SCM_DEFER_INTS;
  rv = PQpass (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}
#endif

SCM_PROC (s_pg_get_host,"pg-get-host",1,0,0, pg_get_host);

static SCM
pg_get_host (SCM obj)
{
  const char *rv;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_host);

  SCM_DEFER_INTS;
  rv = PQhost (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}

SCM_PROC (s_pg_get_port,"pg-get-port",1,0,0, pg_get_port);

static SCM
pg_get_port (SCM obj)
{
  const char *rv;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_port);

  SCM_DEFER_INTS;
  rv = PQport (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}

SCM_PROC (s_pg_get_tty,"pg-get-tty",1,0,0, pg_get_tty);

static SCM
pg_get_tty (SCM obj)
{
  const char *rv;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_tty);

  SCM_DEFER_INTS;
  rv = PQtty (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}

SCM_PROC (s_pg_get_options,"pg-get-options",1,0,0, pg_get_options);

static SCM
pg_get_options (SCM obj)
{
  const char *rv;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_get_options);

  SCM_DEFER_INTS;
  rv = PQoptions (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (rv);
}

SCM_PROC (s_pg_get_connection, "pg-get-connection",1,0,0, pg_get_connection);

static SCM
pg_get_connection (SCM obj)
{
  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_get_connection);
  return (ser_unbox (obj)->conn);
}

#ifdef HAVE_PQBACKENDPID
SCM_PROC (s_pg_backend_pid, "pg-backend-pid",1,0,0, pg_backend_pid);

static SCM
pg_backend_pid (SCM obj)
{
  int pid;

  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_backend_pid);

  SCM_DEFER_INTS;
  pid = PQbackendPID (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  return SCM_MAKINUM (pid);
}
#endif

SCM_PROC (s_pg_result_status, "pg-result-status",1,0,0, pg_result_status);

/* fixme: move to init */
#undef str2symbol
#define str2symbol(s) (SCM_CAR (scm_intern (s, strlen (s))))

static SCM
pg_result_status (SCM obj)
{
  int result_status;
  char **names = ser_status_str;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_result_status);

  SCM_DEFER_INTS;
  result_status = PQresultStatus (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  return ((0 <= result_status && result_status < ser_status_str_count)
          ? str2symbol (names[result_status])
          : SCM_MAKINUM (result_status));
}

SCM_PROC (s_pg_ntuples, "pg-ntuples",1,0,0, pg_ntuples);

static SCM
pg_ntuples (SCM obj)
{
  int ntuples;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_ntuples);

  SCM_DEFER_INTS;
  ntuples = PQntuples (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  return SCM_MAKINUM (ntuples);
}

SCM_PROC (s_pg_nfields, "pg-nfields",1,0,0, pg_nfields);

static SCM
pg_nfields (SCM obj)
{
  SCM scm_inum;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_nfields);

  SCM_DEFER_INTS;
  scm_inum = SCM_MAKINUM (PQnfields (ser_unbox (obj)->result));
  SCM_ALLOW_INTS;

  return scm_inum;
}

SCM_PROC (s_pg_cmdtuples, "pg-cmdtuples",1,0,0, pg_cmdtuples);

static SCM
pg_cmdtuples (SCM obj)
{
  const char *cmdtuples;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_cmdtuples);

  SCM_DEFER_INTS;
  cmdtuples = PQcmdTuples (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (cmdtuples);
}

SCM_PROC (s_pg_oid_status, "pg-oid-status",1,0,0, pg_oid_status);

static SCM
pg_oid_status (SCM obj)
{
  const char *oid_status;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_oid_status);

  SCM_DEFER_INTS;
  oid_status = PQoidStatus (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  return scm_makfrom0str (oid_status);
}

#ifdef HAVE_PQOIDVALUE
SCM_PROC (s_pg_oid_value, "pg-oid-value",1,0,0, pg_oid_value);

static SCM
pg_oid_value (SCM obj)
{
  Oid oid_value;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_oid_value);

  SCM_DEFER_INTS;
  oid_value = PQoidValue (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  if (oid_value == InvalidOid)
    return SCM_BOOL_F;

  return SCM_MAKINUM (oid_value);
}
#endif

SCM_PROC (s_pg_fname, "pg-fname",2,0,0, pg_fname);

static SCM
pg_fname (SCM obj, SCM num)
{
  int field;
  const char *fname;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_fname);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, s_pg_fname);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (obj)->result) && field >= 0) {
    fname = PQfname (ser_unbox (obj)->result, field);
    SCM_ALLOW_INTS;
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (s_pg_fname, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  return scm_makfrom0str (fname);
}

SCM_PROC (s_pg_fnumber, "pg-fnumber",2,0,0, pg_fnumber);

static SCM
pg_fnumber (SCM obj, SCM str)
{
  int fnum;
  char name[BUF_LEN];

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_fnumber);
  SCM_ASSERT (SCM_NIMP (str)&&SCM_ROSTRINGP (str), str, SCM_ARG2, s_pg_fnumber);

  strncpy0 (name, SCM_CHARS (str), BUF_LEN);

  SCM_DEFER_INTS;
  fnum = PQfnumber (ser_unbox (obj)->result, name);
  SCM_ALLOW_INTS;

  return SCM_MAKINUM (fnum);
}

SCM_PROC (s_pg_ftype, "pg-ftype",2,0,0, pg_ftype);

static SCM
pg_ftype (SCM obj, SCM num)
{
  int field;
  int ftype;
  SCM scm_inum;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_ftype);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, s_pg_ftype);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (obj)->result) && field >= 0) {
    ftype = PQftype (ser_unbox (obj)->result, field);
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (s_pg_ftype, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (ftype);
  return scm_inum;
}

SCM_PROC (s_pg_fsize, "pg-fsize",2,0,0, pg_fsize);

static SCM
pg_fsize (SCM obj, SCM num)
{
  int field;
  int fsize;
  SCM scm_inum;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_fsize);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, s_pg_fsize);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (obj)->result) && field >= 0) {
    fsize = PQfsize (ser_unbox (obj)->result, field);
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (s_pg_fsize, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (fsize);
  return scm_inum;
}

SCM_PROC (s_pg_getvalue, "pg-getvalue",3,0,0, pg_getvalue);

static SCM
pg_getvalue (SCM obj, SCM stuple, SCM sfield)
{
  int maxtuple, tuple;
  int maxfield, field;
  const char *val;
#ifdef HAVE_PQBINARYTUPLES
  int isbinary, veclen = 0;
#endif
  SCM srv;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_getvalue);
  SCM_ASSERT (SCM_IMP (stuple) && SCM_INUMP (stuple), stuple, SCM_ARG2,
              s_pg_getvalue);
  SCM_ASSERT (SCM_IMP (sfield) && SCM_INUMP (sfield), sfield, SCM_ARG3,
              s_pg_getvalue);
  tuple = SCM_INUM (stuple);
  field = SCM_INUM (sfield);

  SCM_DEFER_INTS;
  maxtuple = PQntuples (ser_unbox (obj)->result);
  maxfield = PQnfields (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  SCM_ASSERT (tuple < maxtuple && tuple >= 0, stuple, SCM_OUTOFRANGE,
              s_pg_getvalue);
  SCM_ASSERT (field < maxfield && field >= 0, sfield, SCM_OUTOFRANGE,
              s_pg_getvalue);
  SCM_DEFER_INTS;
  val = PQgetvalue (ser_unbox (obj)->result, tuple, field);
#ifdef HAVE_PQBINARYTUPLES
  if ((isbinary = PQbinaryTuples (ser_unbox (obj)->result)) != 0)
    veclen = PQgetlength (ser_unbox (obj)->result, tuple, field);
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

SCM_PROC (s_pg_getlength, "pg-getlength",3,0,0, pg_getlength);

static SCM
pg_getlength (SCM obj, SCM stuple, SCM sfield)
{
  int maxtuple, tuple;
  int maxfield, field;
  int len;
  SCM ret;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_getlength);
  SCM_ASSERT (SCM_IMP (stuple) && SCM_INUMP (stuple), stuple, SCM_ARG2,
              s_pg_getlength);
  SCM_ASSERT (SCM_IMP (sfield) && SCM_INUMP (sfield), sfield, SCM_ARG3,
              s_pg_getlength);
  tuple = SCM_INUM (stuple);
  field = SCM_INUM (sfield);

  SCM_DEFER_INTS;
  maxtuple = PQntuples (ser_unbox (obj)->result);
  maxfield = PQnfields (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  SCM_ASSERT (tuple < maxtuple && tuple >= 0, stuple, SCM_OUTOFRANGE,
              s_pg_getlength);
  SCM_ASSERT (field < maxfield && field >= 0, sfield, SCM_OUTOFRANGE,
              s_pg_getlength);
  SCM_DEFER_INTS;
  len = PQgetlength (ser_unbox (obj)->result, tuple, field);
  SCM_ALLOW_INTS;

  ret = SCM_MAKINUM (len);
  return ret;
}

SCM_PROC (s_pg_getisnull, "pg-getisnull",3,0,0, pg_getisnull);

static SCM
pg_getisnull (SCM obj, SCM stuple, SCM sfield)
{
  int maxtuple, tuple;
  int maxfield, field;
  SCM scm_bool;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_getisnull);
  SCM_ASSERT (SCM_IMP (stuple) && SCM_INUMP (stuple), stuple, SCM_ARG2,
              s_pg_getisnull);
  SCM_ASSERT (SCM_IMP (sfield) && SCM_INUMP (sfield), sfield, SCM_ARG3,
              s_pg_getisnull);
  tuple = SCM_INUM (stuple);
  field = SCM_INUM (sfield);

  SCM_DEFER_INTS;
  maxtuple = PQntuples (ser_unbox (obj)->result);
  maxfield = PQnfields (ser_unbox (obj)->result);
  SCM_ALLOW_INTS;

  SCM_ASSERT (tuple < maxtuple && tuple >= 0, stuple, SCM_OUTOFRANGE,
              s_pg_getisnull);
  SCM_ASSERT (field < maxfield && field >= 0, sfield, SCM_OUTOFRANGE,
              s_pg_getisnull);
  SCM_DEFER_INTS;
  if (PQgetisnull (ser_unbox (obj)->result, tuple, field))
    scm_bool = SCM_BOOL_T;
  else
    scm_bool = SCM_BOOL_F;
  SCM_ALLOW_INTS;

  return scm_bool;
}

#ifdef HAVE_PQBINARYTUPLES
SCM_PROC (s_pg_binary_tuples,"pg-binary-tuples?",1,0,0,pg_binary_tuples);

static SCM
pg_binary_tuples (SCM obj)
{
  SCM rv;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_binary_tuples);

  SCM_DEFER_INTS;
  if (PQbinaryTuples (ser_unbox (obj)->result))
    rv = SCM_BOOL_T;
  else
    rv = SCM_BOOL_F;
  SCM_ALLOW_INTS;

  return rv;
}
#endif

#ifdef HAVE_PQFMOD
SCM_PROC (s_pg_fmod, "pg-fmod",2,0,0, pg_fmod);

static SCM
pg_fmod (SCM obj, SCM num)
{
  int field;
  int fmod;
  SCM scm_inum;

  SCM_ASSERT (ser_p (obj), obj, SCM_ARG1, s_pg_fsize);
  SCM_ASSERT (SCM_IMP (num) && SCM_INUMP (num), num, SCM_ARG2, s_pg_fsize);

  field = SCM_INUM (num);

  SCM_DEFER_INTS;
  if (field < PQnfields (ser_unbox (obj)->result) && field >= 0) {
    fmod = PQfmod (ser_unbox (obj)->result, field);
  } else {
    SCM_ALLOW_INTS;
    scm_misc_error (s_pg_fmod, "Invalid field number %s",
                    scm_listify (num, SCM_UNDEFINED));
  }
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (fmod);
  return scm_inum;
}
#endif

SCM_PROC (s_pg_guile_pg_version,"pg-guile-pg-version",0,0,0,pg_guile_pg_version);

static SCM
pg_guile_pg_version (void)
{
  return scm_makfrom0str (VERSION);
}

SCM_PROC (s_pg_getline, "pg-getline", 1, 0, 0, pg_getline);

static SCM
pg_getline (SCM obj)
{
  char buf[BUF_LEN];
  int ret = 1;
  SCM str = SCM_UNDEFINED;

  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_getline);
  while (ret != 0 && ret != EOF) {
    SCM_DEFER_INTS;
    ret = PQgetline (sec_unbox (obj)->dbconn, buf, BUF_LEN);
    SCM_ALLOW_INTS;
    if (str == SCM_UNDEFINED)
      str = scm_makfrom0str (buf);
    else
      str = scm_string_append (SCM_LIST2 (str, scm_makfrom0str (buf)));
  }
  return str;
}

SCM_PROC (s_pg_putline, "pg-putline", 2, 0, 0, pg_putline);

static SCM
pg_putline (SCM obj, SCM str)
{
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_putline);
  SCM_ASSERT (SCM_NIMP (str)&&SCM_ROSTRINGP (str), str, SCM_ARG2, s_pg_putline);
  SCM_DEFER_INTS;
#ifdef HAVE_PQPUTNBYTES
  PQputnbytes (sec_unbox (obj)->dbconn, SCM_CHARS (str), SCM_ROLENGTH (str));
#else
  PQputline (sec_unbox (obj)->dbconn, SCM_CHARS (str));
#endif
  SCM_ALLOW_INTS;
  return SCM_UNSPECIFIED;
}

SCM_PROC (s_pg_endcopy, "pg-endcopy", 1, 0, 0, pg_endcopy);

static SCM
pg_endcopy (SCM obj)
{
  int ret;
  SCM scm_inum;

  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_endcopy);
  SCM_DEFER_INTS;
  ret = PQendcopy (sec_unbox (obj)->dbconn);
  SCM_ALLOW_INTS;

  scm_inum = SCM_MAKINUM (ret);
  return scm_inum;
}

SCM_PROC (s_pg_trace, "pg-trace", 2, 0, 0, pg_trace);

static SCM
pg_trace (SCM obj, SCM port)
{
  struct scm_fport *fp = SCM_FSTREAM (port);
  int fd;
  FILE *fpout;

  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_trace);
  SCM_ASSERT (sec_unbox (obj)->fptrace == NULL, obj, SCM_ARG1, s_pg_trace);
  port = SCM_COERCE_OUTPORT (port);
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPOUTFPORTP (port),port,SCM_ARG2,s_pg_trace);

  SCM_SYSCALL (fd = dup (fp->fdes));
  if (fd == -1)
    scm_syserror (s_pg_trace);
  SCM_SYSCALL (fpout = fdopen (fd, "w"));
  if (fpout == NULL)
    scm_syserror (s_pg_trace);

  SCM_DEFER_INTS;
  PQtrace (sec_unbox (obj)->dbconn, fpout);
  sec_unbox (obj)->fptrace = fpout;
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
}

SCM_PROC (s_pg_untrace, "pg-untrace", 1, 0, 0, pg_untrace);

static SCM
pg_untrace (SCM obj)
{
  int ret;
  SCM_ASSERT (sec_p (obj), obj, SCM_ARG1, s_pg_untrace);

  SCM_DEFER_INTS;
  PQuntrace (sec_unbox (obj)->dbconn);
  SCM_SYSCALL (ret = fclose (sec_unbox (obj)->fptrace));
  sec_unbox (obj)->fptrace = (FILE *) NULL;
  SCM_ALLOW_INTS;
  if (ret)
    scm_syserror (s_pg_untrace);

  return SCM_UNSPECIFIED;
}

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
#else
  pg_conn_tag.type_tag = scm_make_smob_type ("PG-CONN", 0);
  scm_set_smob_mark (pg_conn_tag.type_tag, sec_mark);
  scm_set_smob_free (pg_conn_tag.type_tag, sec_free);
  scm_set_smob_print (pg_conn_tag.type_tag, sec_display);

  pg_result_tag.type_tag = scm_make_smob_type ("PG-RESULT", 0);
  scm_set_smob_mark (pg_result_tag.type_tag, ser_mark);
  scm_set_smob_free (pg_result_tag.type_tag, ser_free);
  scm_set_smob_print (pg_result_tag.type_tag, ser_display);
#endif

#include <libpostgres.x>

  scm_sysintern ("PGRES_TUPLES_OK",      SCM_MAKINUM (PGRES_TUPLES_OK));
  scm_sysintern ("PGRES_COMMAND_OK",     SCM_MAKINUM (PGRES_COMMAND_OK));
  scm_sysintern ("PGRES_EMPTY_QUERY",    SCM_MAKINUM (PGRES_EMPTY_QUERY));
  scm_sysintern ("PGRES_COPY_OUT",       SCM_MAKINUM (PGRES_COPY_OUT));
  scm_sysintern ("PGRES_COPY_IN",        SCM_MAKINUM (PGRES_COPY_IN));
  scm_sysintern ("PGRES_BAD_RESPONSE",   SCM_MAKINUM (PGRES_BAD_RESPONSE));
  scm_sysintern ("PGRES_NONFATAL_ERROR", SCM_MAKINUM (PGRES_NONFATAL_ERROR));
  scm_sysintern ("PGRES_FATAL_ERROR",    SCM_MAKINUM (PGRES_FATAL_ERROR));

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
