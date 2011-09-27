/* libpq.c

   Copyright (C) 2003, 2004, 2005, 2006, 2007, 2008, 2009,
     2010, 2011 Thien-Thi Nguyen
   Portions Copyright (C) 1999, 2000 Ian Grant

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

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_POSTGRESQL_LIBPQ_FE_H
#include <postgresql/libpq-fe.h>
#else
#include <libpq-fe.h>
#endif
#ifdef HAVE_POSTGRESQL_LIBPQ_LIBPQ_FS_H
#include <postgresql/libpq/libpq-fs.h>
#else
#include <libpq/libpq-fs.h>
#endif

#include "gi.h"                         /* Guile interface */

#ifdef __GNUC__
#define UNUSED __attribute__ ((unused))
#else
#define UNUSED
#endif

#define PROB(x)  (0 > (x))


/* Abstractions for Scheme objects to C string conversion.  */

typedef struct {
  char *s;
  size_t len;
} range_t;

#define RS(svar)    c ## svar .s
#define RLEN(svar)  c ## svar .len

#if GI_LEVEL_NOT_YET_1_8

#define FINANGLABLE_SCHEME_STRING_FROM_SYMBOL(sym)      \
  scm_string_copy (scm_symbol_to_string (sym))

#define _FINANGLE(svar,p1)  do                  \
    {                                           \
      if (p1)                                   \
        ROZT_X (svar);                          \
      RS (svar) = SCM_ROCHARS (svar);           \
      RLEN (svar) = SCM_ROLENGTH (svar);        \
    }                                           \
  while (0)

#define UNFINANGLE(svar)

#else  /* !GI_LEVEL_NOT_YET_1_8 */

#define FINANGLABLE_SCHEME_STRING_FROM_SYMBOL  scm_symbol_to_string

#define REND(svar)          RS (svar) [RLEN (svar)]
#define NUL_AT_END_X(svar)  REND (svar) = '\0'

#define _FINANGLE(svar,p1)  do                                  \
    {                                                           \
      RS (svar) = scm_to_locale_stringn (svar, &RLEN (svar));   \
      if (RS (svar))                                            \
        {                                                       \
          if (p1 && REND (svar))                                \
            {                                                   \
              RS (svar) = realloc (RS (svar), 1 + RLEN (svar)); \
              NUL_AT_END_X (svar);                              \
            }                                                   \
        }                                                       \
      else                                                      \
        RS (svar) = strdup ("");                                \
    }                                                           \
  while (0)

#define UNFINANGLE(svar)  free (RS (svar))

#endif  /* !GI_LEVEL_NOT_YET_1_8 */

/* Use ‘FINANGLE_RAW’ when the consumer of the C string takes full range
   (start address plus length) info.  Otherwise, ‘FINANGLE’.  */

#define FINANGLE_RAW(svar)  _FINANGLE (svar, 0)
#define FINANGLE(svar)      _FINANGLE (svar, 1)


/*
 * smob: connection
 */

static unsigned long int pg_conn_tag;

typedef struct
{
  SCM          notice;        /* port to send notices to */
  PGconn      *dbconn;        /* Postgres data structure */
  FILE        *fptrace;       /* The current trace stream */
} xc_t;

static inline int
xc_p (SCM obj)
{
  return SCM_SMOB_PREDICATE (pg_conn_tag, obj);
}

static inline xc_t *
xc_unbox (SCM obj)
{
  return SMOBDATA (obj);
}

#define ASSERT_CONNECTION(n,arg)                \
  ASSERT (arg, xc_p (arg), SCM_ARG ## n)

#define CONN_CONN(conn)  (xc_unbox (conn)->dbconn)

static char xc_name[] = "PG-CONN";

static int
xc_display (SCM exp, SCM port, UNUSED scm_print_state *pstate)
{
  char buf[256];
  size_t len;
  PGconn *c = CONN_CONN (exp);

  if (! c)
    len = sprintf (buf, "#<%s:->", xc_name);
  else
    {
      char *host = PQhost (c);

      len = snprintf
        (buf, 256, "#<%s:%s:%s:%s:%s>",
         xc_name,
         PQdb (c),
         host ? host : "",
         /* A port without a host is misleading.  Also, a host starting
            with '/' is taken as the directory where the unix-domain
            socket is found.  In such case, port makes no sense.  */
         (host && '/' != host[0]) ? PQport (c) : "",
         PQoptions (c));
    }

  WBPORT (port, buf, len);
  return 1;
}

static SCM
xc_mark (SCM obj)
{
  xc_t *xc = xc_unbox (obj);

  return xc->notice;
}

static size_t
xc_free (SCM obj)
{
  xc_t *xc = xc_unbox (obj);

  if (xc->dbconn)
    PQfinish (xc->dbconn);
  free (xc);
  SCM_SET_SMOB_DATA (obj, NULL);

  return sizeof (xc_t);
}

#define CONN_NOTICE(conn)   (xc_unbox (conn)->notice)
#define CONN_FPTRACE(conn)  (xc_unbox (conn)->fptrace)

#define VALIDATE_CONNECTION_UNBOX_DBCONN(n,arg,cvar)    \
  do {                                                  \
    ASSERT (arg, xc_p (arg), SCM_ARG ## n);             \
    cvar = CONN_CONN (arg);                             \
  } while (0)


/*
 * large objects
 */

static unsigned long int lobp_tag;

#define LOB_BUFLEN 512

#define MAX_LOB_WRITE 7000

typedef struct
{
  SCM          conn;              /* connection on which the LOB fd is open */
  Oid          oid;               /* Oid of the LOB */
  int          alod;              /* A Large-Object Descriptor */
} lob_stream;

#define LOB_CONN(x)  (CONN_CONN ((x)->conn))

#define LOBPORTP(x)  (SCM_NIMP (x) && lobp_tag == SCM_TYP16 (x))

#define LOBPORT_WITH_FLAGS_P(x,flags)                   \
  (LOBPORTP (x) && (SCM_CELL_WORD_0 (x) & (flags)))

#define OPLOBPORTP(x)    (LOBPORT_WITH_FLAGS_P (x, SCM_OPN))
#define OPINLOBPORTP(x)  (LOBPORT_WITH_FLAGS_P (x, SCM_OPN | SCM_RDNG))

static long
extract_mode_bits (SCM modes, int *appendp)
{
  range_t cmodes;
  long rv;

  FINANGLE (modes);
  rv = scm_mode_bits (RS (modes));
  if (appendp)
    *appendp = strchr (RS (modes), 'a') ? 1 : 0;
  UNFINANGLE (modes);
  return rv;
}

#define ASSERT_MODES_COPY(pos,svar,cvar,ap)  do \
    {                                           \
      ASSERT_STRING (pos, svar);                \
      cvar = extract_mode_bits (svar, ap);      \
    }                                           \
  while (0)

static SCM
lob_mklobport (SCM conn, Oid oid, int alod, long modes, const char *FUNC_NAME)
{
  SCM port;
  lob_stream *lobp;
  scm_port *pt;

  lobp = (lob_stream *) scm_must_malloc (sizeof (lob_stream), "PG-LO-PORT");

  NEWCELL_X (port);

  NOINTS ();
  lobp->conn = conn;
  lobp->oid = oid;
  lobp->alod = alod;
  pt = scm_add_to_port_table (port);
  SCM_SETPTAB_ENTRY (port, pt);
  SCM_SET_CELL_WORD_0 (port, lobp_tag | modes);
  SCM_SETSTREAM (port, (SCM) lobp);

  pt->rw_random = 1;
  if (SCM_INPUT_PORT_P (port))
    {
      if (! (pt->read_buf = malloc (LOB_BUFLEN)))
        MEMORY_ERROR ();
      pt->read_pos = pt->read_end = pt->read_buf;
      pt->read_buf_size = LOB_BUFLEN;
    }
  else
    {
      pt->read_pos
        = pt->read_buf
        = pt->read_end
        = &pt->shortbuf;
      pt->read_buf_size = 1;
    }
  if (SCM_OUTPUT_PORT_P (port))
    {
      if (! (pt->write_buf = malloc (LOB_BUFLEN)))
        MEMORY_ERROR ();
      pt->write_pos = pt->write_buf;
      pt->write_buf_size = LOB_BUFLEN;
    }
  else
    {
      pt->write_buf = pt->write_pos = &pt->shortbuf;
      pt->write_buf_size = 1;
    }
  pt->write_end = pt->write_buf + pt->write_buf_size;

  SCM_SET_CELL_WORD_0 (port, SCM_CELL_WORD_0 (port) & ~SCM_BUF0);

  INTSOK ();

  return port;
}

PRIMPROC
(pg_lo_creat, "pg-lo-creat", 2, 0, 0,
 (SCM conn, SCM modes),
 doc: /***********
Create a new large object and open a port over it for reading
and/or writing.  @var{modes} is a string describing the mode in
which the port is to be opened.  The mode string must include
one of @code{r} for reading, @code{w} for writing or @code{a}
for append (but since the object is empty to start with this is
the same as @code{w}).  Return a large object port
which can be used to read or write data to/from the object, or
@code{#f} on failure in which case @code{pg-error-message} from
the connection should give some idea of what happened.

In addition to returning @code{#f} on failure this procedure
throws a @code{misc-error} if the @code{modes} string is invalid.  */)
{
#define FUNC_NAME s_pg_lo_creat
  long mode_bits;
  PGconn *dbconn;
  int alod = 0;
  Oid oid;
  int pg_modes = 0;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_MODES_COPY (2, modes, mode_bits, NULL);

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (! pg_modes)
    ERROR ("Invalid mode specification: ~S", modes);

  NOINTS ();
  oid = lo_creat (dbconn, INV_READ | INV_WRITE);
  INTSOK ();
  if (InvalidOid == oid)
    RETURN_FALSE ();

  NOINTS ();
  alod = lo_open (dbconn, oid, pg_modes);
  INTSOK ();
  if (PROB (alod))
    {
      NOINTS ();
      (void) lo_unlink (dbconn, oid);
      INTSOK ();
      RETURN_FALSE ();
    }

  return lob_mklobport (conn, oid, alod, mode_bits, FUNC_NAME);
#undef FUNC_NAME
}

PRIMPROC
(pg_lo_open, "pg-lo-open", 3, 0, 0,
 (SCM conn, SCM oid, SCM modes),
 doc: /***********
Open a port over an existing large object.  The port can be
used to read or write data from/to the object.  @var{oid}
should be an integer identifier representing the large object.
@var{modes} must be a string describing the mode in which the
port is to be opened.  The mode string must include one of
@code{r} for reading, @code{w} for writing, @code{a} for
appending or @code{+} with any of the above indicating both
reading and writing/appending.  Using @code{a} is equivalent to
opening the port for writing and immediately doing a
@code{(pg-lo-seek)} to the end.  Return either
an open large object port or @code{#f} on failure, in which
case @code{pg-error-message} from the connection should give
some idea of what happened.

Throw @code{misc-error} if the @code{modes} is invalid.  */)
{
#define FUNC_NAME s_pg_lo_open
  long mode_bits; int appendp;
  PGconn *dbconn;
  int alod;
  Oid pg_oid;
  int pg_modes = 0;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_ULONG_COPY (2, oid, pg_oid);
  ASSERT_MODES_COPY (3, modes, mode_bits, &appendp);

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (! pg_modes)
    ERROR ("Invalid mode specification: ~S", modes);
  NOINTS ();
  alod = lo_open (dbconn, pg_oid, pg_modes);
  INTSOK ();

  if (PROB (alod))
    RETURN_FALSE ();

  if (appendp)
    {
      NOINTS ();
      if (PROB (lo_lseek (dbconn, alod, 0, SEEK_END)))
        {
          (void) lo_close (dbconn, alod);
          INTSOK ();
          RETURN_FALSE ();
        }
      INTSOK ();
    }
  return lob_mklobport (conn, pg_oid, alod, mode_bits, FUNC_NAME);
#undef FUNC_NAME
}

PRIMPROC
(pg_lo_unlink, "pg-lo-unlink", 2, 0, 0,
 (SCM conn, SCM oid),
 doc: /***********
Delete the large object identified by @var{oid}.
Return @code{#t} if the object was successfully deleted,
@code{#f} otherwise, in which case @code{pg-error-message}
applied to @code{conn} should give an idea of what went wrong.  */)
{
#define FUNC_NAME s_pg_lo_unlink
  int ret, pg_oid;
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_ULONG_COPY (2, oid, pg_oid);

  NOINTS ();
  ret = lo_unlink (dbconn, pg_oid);
  INTSOK ();
  return BOOLEAN (! PROB (ret));
#undef FUNC_NAME
}

#define ASSERT_PORT(n,arg,precisely)            \
  ASSERT (arg, precisely (arg), SCM_ARG ## n)

#define LOB_STREAM(port)  ((lob_stream *) SCM_STREAM (port))

PRIMPROC
(pg_lo_get_oid, "pg-lo-get-oid", 1, 0, 0,
 (SCM port),
 doc: /***********
Return the integer identifier of the object to which a given
port applies.  @var{port} must be a large object port returned
from @code{pg-lo-creat} or @code{pg-lo-open}.  */)
{
#define FUNC_NAME s_pg_lo_get_oid
  ASSERT_PORT (1, port, LOBPORTP);
  return NUM_INT (LOB_STREAM (port)->oid);
#undef FUNC_NAME
}

PRIMPROC
(pg_lo_tell, "pg-lo-tell", 1, 0, 0,
 (SCM port),
 doc: /***********
Return the position of the file pointer for the given large
object port.  @var{port} must be a large object port returned
from @code{pg-lo-creat} or @code{pg-lo-open}.  Return
either an integer greater than or equal to zero, or
@code{#f} if an error occurred.  In the latter case
@code{pg-error-message} applied to @code{conn} should
explain what went wrong.  */)
{
#define FUNC_NAME s_pg_lo_tell
  ASSERT_PORT (1, port, OPLOBPORTP);

  return scm_ftell (port);
#undef FUNC_NAME
}

/* During lob_flush error, we decide whether to use SYSTEM_ERROR ("normal"
   error mechanism) or to write directly to stderr, depending on libguile's
   variable: scm_terminating.  If it's not available in some form (see
   guile-pg.m4 comments), we arrange to unconditionally write to stderr
   instead of risking further muck-up.  */
#if !HAVE_DECL_SCM_TERMINATING
# ifdef HAVE_LIBGUILE_TERMINATING
extern int terminating;
#   define scm_terminating terminating
# endif /* HAVE_LIBGUILE_TERMINATING */
#endif /* !HAVE_DECL_SCM_TERMINATING */

static void
lob_flush (SCM port)
{
#define FUNC_NAME __func__
  scm_port *pt = SCM_PTAB_ENTRY (port);
  lob_stream *lobp = LOB_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  unsigned char *ptr = pt->write_buf;
  int init_size = pt->write_pos - pt->write_buf;
  int remaining = init_size;

  while (remaining > 0)
    {
      int count;
      NOINTS ();
      count = lo_write (conn, lobp->alod, (char *) ptr, remaining);
      INTSOK ();
      if (count < remaining)
        {
          /* Error.  Assume nothing was written this call, but
             fix up the buffer for any previous successful writes.  */
          int done = init_size - remaining;

          if (done > 0)
            {
              int i;

              for (i = 0; i < remaining; i++)
                {
                  * (pt->write_buf + i) = * (pt->write_buf + done + i);
                }
              pt->write_pos = pt->write_buf + remaining;
            }
#if HAVE_DECL_SCM_TERMINATING || defined (HAVE_LIBGUILE_TERMINATING)
          if (! scm_terminating)
            SYSTEM_ERROR ();
          else
#endif /* HAVE_DECL_SCM_TERMINATING || defined (HAVE_LIBGUILE_TERMINATING) */
            {
              const char *msg = "Error: could not"
                " flush large object file descriptor ";
              char buf[11];

              write (2, msg, strlen (msg));
              sprintf (buf, "%d\n", lobp->alod);
              write (2, buf, strlen (buf));

              count = remaining;
            }
        }
      ptr += count;
      remaining -= count;
    }
  pt->write_pos = pt->write_buf;
#undef FUNC_NAME
}

static void
lob_end_input (SCM port, int offset)
{
#define FUNC_NAME __func__
  scm_port *pt = SCM_PTAB_ENTRY (port);
  lob_stream *lobp = LOB_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  int ret;

  offset += pt->read_end - pt->read_pos;

  if (offset > 0)
    {
      pt->read_pos = pt->read_end;
      NOINTS ();
      ret = lo_lseek (conn, lobp->alod, -offset, SEEK_CUR);
      INTSOK ();
      if (PROB (ret))
        ERROR ("Error seeking on lo port ~S", port);
    }
  pt->rw_active = SCM_PORT_NEITHER;
#undef FUNC_NAME
}

static off_t
lob_seek (SCM port, off_t offset, int whence)
{
#define FUNC_NAME __func__
  lob_stream *lobp = LOB_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  off_t ret;

  NOINTS ();
  ret = lo_lseek (conn, lobp->alod, offset, whence);
  INTSOK ();
  if (PROB (ret))
    ERROR ("Error (~S) seeking on lo port ~S", NUM_INT (ret), port);

  /* Adjust return value to account for guile port buffering.  */
  if (SEEK_CUR == whence)
    {
      scm_port *pt = SCM_PTAB_ENTRY (port);
      ret -= (pt->read_end - pt->read_pos);
    }

  return ret;
#undef FUNC_NAME
}

PRIMPROC
(pg_lo_seek, "pg-lo-seek", 3, 0, 0,
 (SCM port, SCM where, SCM whence),
 doc: /***********
Set the position of the next read or write to/from the given
large object port.  @var{port} must be a large object port
returned from @code{pg-lo-creat} or @code{pg-lo-open}.
@var{where} is the position to set the pointer.  @var{whence}
must be one of

@table @code
@item SEEK_SET
Relative to the beginning of the file.
@item SEEK_CUR
Relative to the current position.
@item SEEK_END
Relative to the end of the file.
@end table

Return an integer which is the new position
relative to the beginning of the object, or a number less than
zero if an error occurred.  */)
{
#define FUNC_NAME s_pg_lo_seek
  ASSERT_PORT (1, port, OPLOBPORTP);
  ASSERT_EXACT (2, where);
  ASSERT_EXACT (3, whence);

  lob_flush (port);

  return NUM_INT (lob_seek (port, C_INT (where), C_INT (whence)));
#undef FUNC_NAME
}

/* Fill a port's read-buffer with a single read.  Return the first char and
   move the ‘read_pos’ pointer past it, or return EOF if end of file.  */
static int
lob_fill_input (SCM port)
{
#define FUNC_NAME __func__
  scm_port *pt = SCM_PTAB_ENTRY (port);
  lob_stream *lobp = LOB_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  int ret;

  if (pt->write_pos > pt->write_buf)
    lob_flush (port);

  NOINTS ();
  ret = lo_read (conn, lobp->alod, (char *) pt->read_buf, pt->read_buf_size);
  INTSOK ();
  if (PROB (ret))
    ERROR ("Error (~S) reading from lo port ~S", NUM_INT (ret), port);
  if (pt->read_buf_size && !ret)
    return EOF;
  pt->read_pos = pt->read_buf;
  pt->read_end = pt->read_buf + ret;

  return * (pt->read_buf);
#undef FUNC_NAME
}

static void
lob_write (SCM port, const void *data, size_t size)
{
#define FUNC_NAME __func__
  scm_port *pt = SCM_PTAB_ENTRY (port);

  if (pt->write_buf == &pt->shortbuf)
    {
      /* This is an "unbuffered" port.  */
      int fdes = SCM_FPORT_FDES (port);

      if (PROB (write (fdes, data, size)))
        SYSTEM_ERROR ();
    }
  else
    {
      const char *input = data;
      size_t remaining = size;

      while (remaining > 0)
        {
          size_t space = pt->write_end - pt->write_pos;
          size_t write_len = (remaining > space) ? space : remaining;

          memcpy (pt->write_pos, input, write_len);
          pt->write_pos += write_len;
          remaining -= write_len;
          input += write_len;
          if (write_len == space)
            lob_flush (port);
        }
      /* handle line buffering.  */
      if ((SCM_CELL_WORD_0 (port) & SCM_BUFLINE)
          && memchr (data, '\n', size))
        lob_flush (port);
    }
#undef FUNC_NAME
}

/* Check whether a port can supply input.  */
static int
lob_input_waiting_p (UNUSED SCM port)
{
  return 1;
}

PRIMPROC
(pg_lo_read, "pg-lo-read", 3, 0, 0,
 (SCM siz, SCM num, SCM port),
 doc: /***********
Read @var{num} objects each of length @var{siz} from @var{port}.
Return a string containing the data read from the port or
@code{#f} if an error occurred.  */)
{
#define FUNC_NAME s_pg_lo_read
  char *stage, *wp;
  int csiz, cnum, len, c;

  ASSERT_EXACT_NON_NEGATIVE_COPY (1, siz, csiz);
  ASSERT_EXACT_NON_NEGATIVE_COPY (2, num, cnum);
  ASSERT_PORT (3, port, OPINLOBPORTP);

  if (0 > (len = csiz * cnum)
      || (MAX_NEWSTRING_LENGTH < len)
      || (! (wp = stage = (char *) malloc (1 + len))))
    RETURN_FALSE ();
  while (len-- && (EOF != (c = scm_getc (port))))
    *wp++ = c;
  *wp = '\0';
  return scm_take_str (stage, wp - stage);
#undef FUNC_NAME
}

static int
lob_close (SCM port)
{
  scm_port *pt = SCM_PTAB_ENTRY (port);
  lob_stream *lobp = LOB_STREAM (port);
  PGconn *dbconn = LOB_CONN (lobp);
  int ret;

  lob_flush (port);
  NOINTS ();
  ret = lo_close (dbconn, lobp->alod);
  INTSOK ();

  if (pt->read_buf != &pt->shortbuf)
    free (pt->read_buf);
  if (pt->write_buf != &pt->shortbuf)
    free (pt->write_buf);

  return ret ? EOF : 0;
}

static SCM
lob_mark (SCM port)
{
  return DEFAULT_FALSE (SCM_OPENP (port),
                        LOB_STREAM (port)->conn);
}

static size_t
lob_free (SCM port)
{
  lob_stream *lobp = LOB_STREAM (port);
  int ret;

  if (SCM_OPENP (port))
    ret = lob_close (port);
  free (lobp);
  return 0;
}

PRIMPROC
(pg_lo_import, "pg-lo-import", 2, 0, 0,
 (SCM conn, SCM filename),
 doc: /***********
Create a new large object and loads it with the contents of
the specified file.  @var{filename} must be a string containing
the name of the file to be loaded into the new object.  Return
the integer identifier (@acronym{OID}) of the newly created large object,
or @code{#f} if an error occurred, in which case
@code{pg-error-message} should be consulted to determine
the failure.  */)
{
#define FUNC_NAME s_pg_lo_import
  PGconn *dbconn;
  range_t cfilename;
  Oid ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, filename);

  NOINTS ();
  FINANGLE (filename);
  ret = lo_import (dbconn, RS (filename));
  UNFINANGLE (filename);
  INTSOK ();

  return DEFAULT_FALSE (InvalidOid != ret,
                        NUM_INT (ret));
#undef FUNC_NAME
}

PRIMPROC
(pg_lo_export, "pg-lo-export", 3, 0, 0,
 (SCM conn, SCM oid, SCM filename),
 doc: /***********
Write the contents of a given large object to a file.
@var{oid} is the integer identifying the large object to be
exported and @var{filename} the name of the file to contain the
object data.  Return @code{#t} on success, @code{#f} otherwise,
in which case @code{pg-error-message} may offer an explanation
of the failure.  */)
{
#define FUNC_NAME s_pg_lo_export
  PGconn *dbconn;
  Oid pg_oid;
  range_t cfilename;
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_ULONG_COPY (2, oid, pg_oid);
  ASSERT_STRING (3, filename);

  NOINTS ();
  FINANGLE (filename);
  ret = lo_export (dbconn, pg_oid, RS (filename));
  UNFINANGLE (filename);
  INTSOK ();

  return BOOLEAN (! PROB (ret));
#undef FUNC_NAME
}

static int
lob_printpt (SCM exp, SCM port, scm_print_state *pstate)
{
  static const char head[] = "#<PG-LO-PORT:";
  static const char tail[] = ">";

  WBPORT (port, head, sizeof (head) - 1);
  scm_print_port_mode (exp, port);
  if (SCM_OPENP (exp))
    {
      char buf[32];
      size_t len;
      lob_stream *lobp = LOB_STREAM (exp);

      len = snprintf (buf, 32, "%d:%d:", lobp->alod, lobp->oid);
      WBPORT (port, buf, len);
      xc_display (lobp->conn, port, pstate);
    }
  WBPORT (port, tail, sizeof (tail) - 1);
  return 1;
}


/*
 * smob: result
 */

static unsigned long int pg_result_tag;

static inline int
res_p (SCM obj)
{
  return SCM_SMOB_PREDICATE (pg_result_tag, obj);
}

static inline PGresult *
res_unbox (SCM obj)
{
  return SMOBDATA (obj);
}

#define VALIDATE_RESULT_UNBOX(pos,arg,cvar)     \
  do {                                          \
    ASSERT (arg, res_p (arg), SCM_ARG ## pos);  \
    cvar = res_unbox (arg);                     \
  } while (0)

static SCM
res_box (PGresult *res)
{
  if (res)
    SCM_RETURN_NEWSMOB (pg_result_tag, res);
  else
    RETURN_FALSE ();
}

static char res_name[] = "PG-RESULT";

static int
res_display (SCM exp, SCM port, UNUSED scm_print_state *pstate)
{
  char buf[64];
  size_t len = 0;
  PGresult *res = res_unbox (exp);
  ExecStatusType status;

  if (PGRES_FATAL_ERROR < (status = PQresultStatus (res)))
    status = PGRES_FATAL_ERROR;

  len = sprintf (buf, "#<%s:%s", res_name,
                 /* Omit common "PGRES_" (sizeof 7) prefix.  */
                 6 + PQresStatus (status));
  if (PGRES_TUPLES_OK == status)
    len += sprintf (buf + len, ":%d:%d",
                    PQntuples (res),
                    PQnfields (res));
  len += sprintf (buf + len, ">");
  WBPORT (port, buf, len);

  return 1;
}

static size_t
res_free (SCM obj)
{
  PGresult *res = res_unbox (obj);

  if (res)
    PQclear (res);
  SCM_SET_SMOB_DATA (obj, NULL);

  return 0;
}


/*
 * string munging
 */

#define BUF_LEN 256

static SCM pgrs[1 + PGRES_FATAL_ERROR];

static SCM
strip_newlines (char *str)
{
  char *lc = str + strlen (str) - 1;    /* last char */

  while (str <= lc && *lc == '\n')
    lc--;

  return BSTRING (str, lc + 1 - str);
}


/*
 * common parameter handling
 */

struct paramspecs
{
  int          len;
  Oid         *types;
  const char **values;
  int         *lengths;
  int         *formats;
};

#define VALIDATE_PARAM_RELATED_ARGS(what)               \
  do {                                                  \
    VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn); \
    SCM_VALIDATE_STRING (2, what);                      \
    SCM_VALIDATE_VECTOR (3, parms);                     \
    FINANGLE (what);                                    \
  } while (0)

static void
prep_paramspecs (const char *FUNC_NAME, struct paramspecs *ps, SCM v)
{
  int i, len;
  SCM elem;

  /* To avoid hairy Oid specification/lookup, for now we support vector of
     strings only.  This lameness is not good long-term, but the Scheme
     procedure programming interface is upward-compatible by design, so we
     can ease into full specification later (by relaxing/extending the
     vector element validation).  */
  ps->len = len = VECTOR_LEN (v);
  for (i = 0; i < len; i++)
    {
      elem = VREF (v, i);
      if (! STRINGP (elem))
        ERROR ("bad parameter-vector element: ~S", elem);
    }
  ps->types = NULL;
  ps->values = (const char **) malloc (len * sizeof (char *));
  if (! ps->values)
    MEMORY_ERROR ();
  for (i = 0; i < len; i++)
    {
      range_t celem;

      elem = VREF (v, i);
      FINANGLE (elem);
      ps->values[i] = strdup (RS (elem));
      UNFINANGLE (elem);
    }
  ps->lengths = NULL;
  ps->formats = NULL;
}

static void
drop_paramspecs (struct paramspecs *ps)
{
  free (ps->types);
  free (ps->values);
  free (ps->lengths);
  free (ps->formats);
}


/*
 * other abstractions
 */

#define VALIDATE_FIELD_NUMBER_COPY(pos,num,res,cvar)                    \
  VALIDATE_EXACT_0_UP_TO_N_COPY (pos, num, PQnfields (res), cvar)


/*
 * meta and connection
 */

PRIMPROC
(pg_guile_pg_loaded, "pg-guile-pg-loaded", 0, 0, 0,
 (void),
 doc: /***********
Return a list of symbols describing the Guile-PG
installation.  These are basically derived from C preprocessor
macros determined at build time by the configure script.
Presence of this procedure is also a good indicator that
the compiled module @code{(database postgres)} is
available.  You can test this like so:

@lisp
(false-if-exception (pg-guile-pg-loaded))
@end lisp  */)
{
  SCM rv = SCM_EOL;
  /* $ ttn-do generate-C-symbol-set     \
     PQPROTOCOLVERSION                  \
     PQRESULTERRORMESSAGE               \
     PQPASS                             \
     PQBACKENDPID                       \
     PQOIDVALUE                         \
     PQBINARYTUPLES                     \
     PQFMOD                             \
     PQSETNONBLOCKING                   \
     PQISNONBLOCKING                    \
     -b char  */
  static const char symbolpool[126] =
    {
      9 /* count */,
      17,'P','Q','P','R','O','T','O','C','O','L','V','E','R','S','I','O','N',
      20,'P','Q','R','E','S','U','L','T','E','R','R','O','R','M','E','S','S','A','G','E',
      6,'P','Q','P','A','S','S',
      12,'P','Q','B','A','C','K','E','N','D','P','I','D',
      10,'P','Q','O','I','D','V','A','L','U','E',
      14,'P','Q','B','I','N','A','R','Y','T','U','P','L','E','S',
      6,'P','Q','F','M','O','D',
      16,'P','Q','S','E','T','N','O','N','B','L','O','C','K','I','N','G',
      15,'P','Q','I','S','N','O','N','B','L','O','C','K','I','N','G'
    };
  const char *p = symbolpool;
  int count = *p++;

  while (count--)
    {
      int len = *p++;
      SCM sym = scm_string_to_symbol (BSTRING (p, len));

      rv = CONS (sym, rv);
      p += len;
    }
  return rv;
}

PRIMPROC
(pg_protocol_version, "pg-protocol-version", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return the client protocol version for @var{conn}.
This (integer) will be 2 prior to PostgreSQL 7.4.
If @var{conn} is not a connection object, return @code{#f}.  */)
{
#define FUNC_NAME s_pg_protocol_version
  PGconn *dbconn;
  int v = 2;

  if (! xc_p (conn))
    RETURN_FALSE ();

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  v = PQprotocolVersion (dbconn);

  return DEFAULT_FALSE (v, NUM_INT (v));
#undef FUNC_NAME
}

#define SIMPLE_KEYWORD(name)                    \
  SCM_KEYWORD (kwd_ ## name, # name)

SIMPLE_KEYWORD (envvar);
SIMPLE_KEYWORD (compiled);
SIMPLE_KEYWORD (val);
SIMPLE_KEYWORD (label);
SIMPLE_KEYWORD (dispchar);
SIMPLE_KEYWORD (dispsize);

#define KWD(name)  (kwd_ ## name)

PRIMPROC
(pg_conndefaults, "pg-conndefaults", 0, 0, 0,
 (void),
 doc: /***********
Return an alist associating options with their connection
defaults.  The option name is a keyword.
Each associated value is in turn a sub-alist, with
the following keys:

@itemize
@item @code{#:envvar}
@item @code{#:compiled}
@item @code{#:val}
@item @code{#:label}
@item @code{#:dispchar} (character: @code{#\*} or @code{#\D};
or @code{#f})
@item @code{#:dispsize} (integer)
@end itemize

Values are strings or @code{#f}, unless noted otherwise.
A @code{dispchar} of @code{#\*} means the option should
be treated like a password: user dialogs should hide
the value; while @code{#\D} means the option is for
debugging purposes: probably a good idea to entirely avoid
presenting this option in the first place.  */)
{
  PQconninfoOption *opt, *head;
  SCM rv = SCM_EOL;

#define PAIRM(field,exp) /* maybe */                            \
    CONS (KWD (field),                                          \
          DEFAULT_FALSE (opt->field && opt->field[0],           \
                         (exp)))
#define PAIRX(field,exp) /* unconditional */    \
    CONS (KWD (field), (exp))

  for (head = opt = PQconndefaults (); opt && opt->keyword; opt++)
    rv = CONS
      (PCHAIN (scm_c_make_keyword (opt->keyword),
               PAIRX (envvar,   STRING (opt->envvar)),
               PAIRM (compiled, STRING (opt->compiled)),
               PAIRM (val,      STRING (opt->val)),
               PAIRM (label,    STRING (opt->label)),
               PAIRM (dispchar, CHARACTER (opt->dispchar[0])),
               PAIRX (dispsize, NUM_INT (opt->dispsize))),
       rv);

#undef PAIRX
#undef PAIRM

  if (head)
    PQconninfoFree (head);

  return rv;
}

static void
notice_processor (void *xc, const char *message)
{
  SCM out = ((xc_t *) xc)->notice;

  if (BOOLEANP (out))
    {
      if (NOT_FALSEP (out))
        out = scm_current_error_port ();
      else
        return;
    }

  if (SCM_OUTPUT_PORT_P (out))
    WBPORT (out, message, strlen (message));
  else if (PROCEDUREP (out))
    APPLY (out, PCHAIN (STRING (message)));
  else
    abort ();
}

PRIMPROC
(pg_connectdb, "pg-connectdb", 1, 0, 0,
 (SCM constr),
 doc: /***********
Open and return a connection to the database specified
and configured by @var{constr}, a possibly empty string
consisting of space-separated @code{name=value} pairs.
The @var{name} can be any of:

@table @code
@item host
The host-name or dotted-decimal @acronym{IP} address of the host
on which the postmaster is running.  If no @code{host=}
sub-string is given then consult in order: the
environment variable @code{PGHOST}, otherwise the name
of the local host.

@item port
The @acronym{TCP} or Unix socket on which the backend is
listening.  If this is not specified then consult in
order: the environment variable @code{PGPORT},
otherwise the default port (5432).

@item options
A string containing the options to the backend server.
The options given here are in addition to the options
given by the environment variable @code{PGOPTIONS}.
The options string should be a set of command line
switches as would be passed to the backend.  See the
postgres(1) man page for more details.

@item tty
A string defining the file or device on which error
messages from the backend are to be displayed.  If this
is empty (@code{""}), then consult the environment
variable @code{PGTTY}.  If the specified tty is a file
then the file will be readable only by the user the
postmaster runs as (usually @samp{postgres}).
Similarly, if the specified tty is a device then it
must have permissions allowing the postmaster user to
write to it.

@item dbname
The name of the database.  If no @code{dbname=}
sub-string is given then consult in order: the
environment variable @code{PGDATABASE}, the environment
variable @code{USER}, otherwise the @code{user} value.

@item user
The login name of the user to authenticate.  If none is
given then consult in order: the environment variable
@code{PGUSER}, otherwise the login name of the user
owning the process.

@item password
The password.  Whether or not this is used depends upon
the contents of the @file{pg_hba.conf} file.  See the
pg_hba.conf(5) man page for details.

@item authtype
This must be set to @code{password} if password
authentication is in use, otherwise it must not be
specified.
@end table

If @var{value} contains spaces it must be enclosed in
single quotes, and any single quotes appearing in
@var{value} must be escaped using backslashes.
Backslashes appearing in @var{value} must similarly be
escaped.  Note that if the @var{constr} is a Guile
string literal then all the backslashes will themselves
need to be escaped a second time.  */)
{
#define FUNC_NAME s_pg_connectdb
  xc_t *xc;
  PGconn *dbconn;
  range_t cconstr;

  ASSERT_STRING (1, constr);

  NOINTS ();
  FINANGLE (constr);
  dbconn = PQconnectdb (RS (constr));
  UNFINANGLE (constr);

  if (CONNECTION_BAD == PQstatus (dbconn))
    {
      /* Get error message before PQfinish, which zonks dbconn storage.  */
      SCM pgerrormsg = strip_newlines (PQerrorMessage (dbconn));

      PQfinish (dbconn);
      INTSOK ();
      ERROR ("~A", pgerrormsg);
    }
  INTSOK ();

  xc = ((xc_t *) scm_must_malloc (sizeof (xc_t), xc_name));

  xc->dbconn = dbconn;
  xc->notice = SCM_BOOL_T;
  xc->fptrace = NULL;

  /* Whatever the default was before, we don't care.  */
  PQsetNoticeProcessor (dbconn, &notice_processor, xc);

  SCM_RETURN_NEWSMOB (pg_conn_tag, xc);
#undef FUNC_NAME
}

PRIMPROC
(pg_connection_p, "pg-connection?", 1, 0, 0,
 (SCM obj),
 doc: /***********
Return @code{#t} iff @var{obj} is a connection object
returned by @code{pg-connectdb}.  */)
{
  return BOOLEAN (xc_p (obj));
}

PRIMPROC
(pg_finish, "pg-finish", 1, 0, 0,
 (SCM conn),
 doc: /***********
Close the connection @var{conn} with the backend.  */)
{
#define FUNC_NAME s_pg_finish
  xc_t *xc;

  ASSERT_CONNECTION (1, conn);

  xc = xc_unbox (conn);
  if (xc->dbconn)
    {
      PQfinish (xc->dbconn);
      xc->dbconn = NULL;
    }

  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}

PRIMPROC
(pg_reset, "pg-reset", 1, 0, 0,
 (SCM conn),
 doc: /***********
Reset the connection @var{conn} with the backend.
Equivalent to closing the connection and re-opening it again
with the same connect options as given to @code{pg-connectdb}.
@var{conn} must be a valid @code{PG_CONN} object returned by
@code{pg-connectdb}.  */)
{
#define FUNC_NAME s_pg_reset
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  PQreset (dbconn);
  INTSOK ();

  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}

PRIMPROC
(pg_server_version, "pg-server-version", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return an integer representation of the server version at @var{conn}.
This is basically

@example
(+ (* 10000 @var{major}) (* 100 @var{minor}) @var{micro})
@end example

@noindent
which yields 40725 for PostgreSQL 4.7.25, for example.
Return @code{#f} if @var{conn} is closed.  */)
{
#define FUNC_NAME s_pg_server_version
  PGconn *dbconn;
  unsigned int combined = 0;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
#ifndef HAVE_PQSERVERVERSION
  {
    const char *sv;
    int major, minor, micro;

    sv = PQparameterStatus (dbconn, "server_version");
    sscanf (sv, "%d.%d.%d", &major, &minor, &micro);
    combined = 10000 * major + 100 * minor + micro;
  }
#else /* HAVE_PQSERVERVERSION */
  combined = PQserverVersion (dbconn);
#endif /* HAVE_PQSERVERVERSION */

  return DEFAULT_FALSE (combined, NUM_INT (combined));
#undef FUNC_NAME
}


/*
 * string and bytea escaping
 */

PRIMPROC
(pg_escape_string_conn, "pg-escape-string-conn", 2, 0, 0,
 (SCM conn, SCM string),
 doc: /***********
Return a new string made from doubling every single-quote
char in @var{string}.
The escaping is consistent with the encoding for @var{conn}.
The returned string does not have surrounding single-quote chars.
If there is an error, return @code{#f}.  */)
{
#define FUNC_NAME s_pg_escape_string_conn
  PGconn *dbconn;
  range_t cstring;
  char *answer;
  size_t olen;
  int errcode;
  SCM rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, string);

  FINANGLE_RAW (string);
  if (! (answer = malloc (1 + 2 * RLEN (string))))
    {
      UNFINANGLE (string);
      SYSTEM_ERROR ();
    }

  olen = PQescapeStringConn (dbconn, answer, RS (string), RLEN (string), &errcode);
  rv = DEFAULT_FALSE (! errcode,
                      STRING (answer));
  free (answer);
  UNFINANGLE (string);
  return rv;
#undef FUNC_NAME
}

PRIMPROC
(pg_escape_bytea_conn, "pg-escape-bytea-conn", 2, 0, 0,
 (SCM conn, SCM bytea),
 doc: /***********
Return a new string made from doing the ``minimal'' replacement
of byte values with its @code{\\ABC} (octal) representation
in @var{bytea} (a string).
The escaping is consistent with the encoding for @var{conn}.
The returned bytea does not have surrounding single-quote chars.
If there is an error, return @code{#f}.  */)
{
#define FUNC_NAME s_pg_escape_bytea_conn
  void *ucbytea; range_t cbytea;
  PGconn *dbconn;
  unsigned char *answer;
  size_t olen;
  SCM rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, bytea);

  FINANGLE_RAW (bytea);
  ucbytea = RS (bytea);
  rv = DEFAULT_FALSE
    (answer = PQescapeByteaConn (dbconn, ucbytea, RLEN (bytea), &olen),
     BSTRING ((char *) answer, olen ? olen - 1 : 0));
  UNFINANGLE (bytea);
  PQfreemem (answer);
  return rv;
#undef FUNC_NAME
}

PRIMPROC
(pg_unescape_bytea, "pg-unescape-bytea", 1, 0, 0,
 (SCM bytea),
 doc: /***********
Return a new bytea made from unescaping @var{bytea}.
If there is an error, return @code{#f}.  */)
{
#define FUNC_NAME s_pg_unescape_bytea
  unsigned char *answer;
  void *ucbytea; range_t cbytea;
  size_t olen;
  SCM rv;

  ASSERT_STRING (1, bytea);
  FINANGLE_RAW (bytea);
  ucbytea = RS (bytea);
  rv = DEFAULT_FALSE
    (answer = PQunescapeBytea (ucbytea, &olen),
     BSTRING ((char *) answer, olen));
  PQfreemem (answer);
  UNFINANGLE (bytea);
  return rv;
#undef FUNC_NAME
}


/*
 * exec and results
 */

/* At this time, we use ‘RESFMT_TEXT’ exclusively.  TODO: Figure out what
   higher layers need to do/know to be able to handle both text and binary,
   then make it dynamic.  */
#define RESFMT_TEXT    0
#define RESFMT_BINARY  1

PRIMPROC
(pg_exec, "pg-exec", 2, 0, 0,
 (SCM conn, SCM statement),
 doc: /***********
Execute the SQL string @var{statement} on a given connection
@var{conn} returning either a @code{PG_RESULT} object containing
a @code{pg-result-status} or @code{#f} if an error occurred,
in which case the error message can be obtained using
@code{pg-error-message}, passing it the @code{PG_CONN} object
on which the statement was attempted.  Note that the error
message is available only until the next call to @code{pg-exec}
on this connection.  */)
{
#define FUNC_NAME s_pg_exec
  range_t cstatement;
  SCM z;
  PGconn *dbconn;
  PGresult *result;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, statement);

  NOINTS ();
  FINANGLE (statement);
  result = PQexec (dbconn, RS (statement));
  UNFINANGLE (statement);

  z = res_box (result);
  INTSOK ();
  return z;
#undef FUNC_NAME
}

PRIMPROC
(pg_exec_params, "pg-exec-params", 3, 0, 0,
 (SCM conn, SCM statement, SCM parms),
 doc: /***********
Like @code{pg-exec}, except that @var{statement} is a
parameterized string, and @var{parms} is a parameter-vector.  */)
{
#define FUNC_NAME s_pg_exec_params
  SCM z;
  PGconn *dbconn;
  range_t cstatement;
  PGresult *result;
  struct paramspecs ps;

  VALIDATE_PARAM_RELATED_ARGS (statement);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQexecParams (dbconn, RS (statement), ps.len,
                         ps.types, ps.values, ps.lengths, ps.formats,
                         RESFMT_TEXT);
  UNFINANGLE (statement);
  z = res_box (result);
  INTSOK ();
  drop_paramspecs (&ps);
  return z;
#undef FUNC_NAME
}

PRIMPROC
(pg_exec_prepared, "pg-exec-prepared", 3, 0, 0,
 (SCM conn, SCM stname, SCM parms),
 doc: /***********
Execute the statement named by @var{stname} (a string)
on a given connection @var{conn} returning either a result
object or @code{#f}.  @var{stname} must match the
name specified in some prior SQL @code{PREPARE} statement.
@var{parms} is a parameter-vector.  */)
{
#define FUNC_NAME s_pg_exec_prepared
  SCM z;
  PGconn *dbconn;
  range_t cstname;
  PGresult *result;
  struct paramspecs ps;

  VALIDATE_PARAM_RELATED_ARGS (stname);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQexecPrepared (dbconn, RS (stname), ps.len,
                           ps.values, ps.lengths, ps.formats,
                           RESFMT_TEXT);
  UNFINANGLE (stname);
  z = res_box (result);
  INTSOK ();
  drop_paramspecs (&ps);
  return z;
#undef FUNC_NAME
}

PRIMPROC
(pg_result_p, "pg-result?", 1, 0, 0,
 (SCM obj),
 doc: /***********
Return @code{#t} iff @var{obj} is a result object
returned by @code{pg-exec}.  */)
{
  return BOOLEAN (res_p (obj));
}

SIMPLE_KEYWORD (severity);
SIMPLE_KEYWORD (sqlstate);
SCM_KEYWORD (kwd_messageprimary, "message-primary");
SCM_KEYWORD (kwd_messagedetail, "message-detail");
SCM_KEYWORD (kwd_messagehint, "message-hint");
SCM_KEYWORD (kwd_statementposition, "statement-position");
SIMPLE_KEYWORD (context);
SCM_KEYWORD (kwd_sourcefile, "source-file");
SCM_KEYWORD (kwd_sourceline, "source-line");
SCM_KEYWORD (kwd_sourcefunction, "source-function");

PRIMPROC
(pg_result_error_field, "pg-result-error-field", 2, 0, 0,
 (SCM result, SCM fieldcode),
 doc: /***********
Return information associated with @var{result}, on
@var{fieldcode}, a keyword, or @code{#f} if either
@var{fieldcode} is unrecognized or the field value is
not available for some reason.  The type of the
return value depends on @var{fieldcode}:

@table @code
@item #:severity
@itemx #:sqlstate
@itemx #:message-primary
@itemx #:message-detail
@itemx #:message-hint
@itemx #:context
@itemx #:source-file
A string.  The value for @code{#:message-primary} is
typically one line, whereas for @code{#:message-detail},
@code{#:message-hint} and @code{#:context}, it may run
to multiple lines.  For @code{#:sqlstate}, it is always
five characters long.
@item #:statement-position
@itemx #:source-line
An integer.  Statement position counts characters
(not bytes), starting from 1.
@item #:source-function
A symbol.
@end table  */)
{
#define FUNC_NAME s_pg_result_error_field
  PGresult *res;
  int fc;
  char *s;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_KEYWORD (2, fieldcode);

#define CHKFC(x,y)                              \
  if (EQ (fieldcode, (x)))                 \
    do { fc = (y); goto gotfc; } while (0)

  CHKFC (KWD (severity),          PG_DIAG_SEVERITY);
  CHKFC (KWD (sqlstate),          PG_DIAG_SQLSTATE);
  CHKFC (KWD (messageprimary),    PG_DIAG_MESSAGE_PRIMARY);
  CHKFC (KWD (messagedetail),     PG_DIAG_MESSAGE_DETAIL);
  CHKFC (KWD (messagehint),       PG_DIAG_MESSAGE_HINT);
  CHKFC (KWD (statementposition), PG_DIAG_STATEMENT_POSITION);
  CHKFC (KWD (context),           PG_DIAG_CONTEXT);
  CHKFC (KWD (sourcefile),        PG_DIAG_SOURCE_FILE);
  CHKFC (KWD (sourceline),        PG_DIAG_SOURCE_LINE);
  CHKFC (KWD (sourcefunction),    PG_DIAG_SOURCE_FUNCTION);
  RETURN_FALSE ();
#undef CHKFC

 gotfc:
  if ((s = PQresultErrorField (res, fc)))
    {
      SCM rv;

      switch (fc)
        {
        case PG_DIAG_STATEMENT_POSITION:
        case PG_DIAG_SOURCE_LINE:
          rv = EVAL_STRING (s);
          break;
        case PG_DIAG_SOURCE_FUNCTION:
          rv = SYMBOL (s);
          break;
        default:
          rv = STRING (s);
        }
    }

  RETURN_FALSE ();
#undef FUNC_NAME
}

PRIMPROC
(pg_result_error_message, "pg-result-error-message", 1, 0, 0,
 (SCM result),
 doc: /***********
Return the error message associated with @var{result},
or the empty string if there was no error.
If the installation does not support
@code{PQRESULTERRORMESSAGE}, return the empty string.
The returned string has no trailing newlines.  */)
{
#define FUNC_NAME s_pg_result_error_message
  PGresult *res;
  char *msg;

  VALIDATE_RESULT_UNBOX (1, result, res);
  msg = PQresultErrorMessage (res);
  return strip_newlines (msg);
#undef FUNC_NAME
}

PRIMPROC
(pg_error_message, "pg-error-message", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return the most-recent error message that occurred on the
connection @var{conn}, or an empty string.
For backward compatability, if @var{conn} is actually
a result object returned from calling @code{pg-exec},
delegate the call to @code{pg-result-error-message}
transparently (new code should call that procedure
directly).  */)
{
#define FUNC_NAME s_pg_error_message
  if (res_p (conn))
    return pg_result_error_message (conn);

  {
    PGconn *dbconn;
    SCM rv;
    char *msg;

    VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
    NOINTS ();
    msg = PQerrorMessage (dbconn);
    rv = strip_newlines (msg);
    INTSOK ();

    return rv;
  }
#undef FUNC_NAME
}

PRIMPROC
(pg_get_db, "pg-get-db", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a string containing the name of the database
to which @var{conn} represents a connection.  */)
{
#define FUNC_NAME s_pg_get_db
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQdb (dbconn);
  INTSOK ();

  return STRING (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_user, "pg-get-user", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a string containing the user name used to
authenticate the connection @var{conn}.  */)
{
#define FUNC_NAME s_pg_get_user
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQuser (dbconn);
  INTSOK ();

  return STRING (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_pass, "pg-get-pass", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a string containing the password used to
authenticate the connection @var{conn}.  */)
{
#define FUNC_NAME s_pg_get_pass
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQpass (dbconn);
  INTSOK ();

  return STRING (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_host, "pg-get-host", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a string containing the name of the host to which
@var{conn} represents a connection.  */)
{
#define FUNC_NAME s_pg_get_host
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQhost (dbconn);
  INTSOK ();

  return STRING (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_port, "pg-get-port", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a string containing the port number to which
@var{conn} represents a connection.  */)
{
#define FUNC_NAME s_pg_get_port
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQport (dbconn);
  INTSOK ();

  return STRING (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_tty, "pg-get-tty", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a string containing the the name of the
diagnostic tty for @var{conn}.  */)
{
#define FUNC_NAME s_pg_get_tty
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQtty (dbconn);
  INTSOK ();

  return STRING (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_options, "pg-get-options", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a string containing the options string for @var{conn}.  */)
{
#define FUNC_NAME s_pg_get_options
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQoptions (dbconn);
  INTSOK ();

  return STRING (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_backend_pid, "pg-backend-pid", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return an integer which is the the @acronym{PID} of the backend
process for @var{conn}.  */)
{
#define FUNC_NAME s_pg_backend_pid
  PGconn *dbconn;
  int pid;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  pid = PQbackendPID (dbconn);
  INTSOK ();

  return NUM_INT (pid);
#undef FUNC_NAME
}

SIMPLE_KEYWORD (idle);
SIMPLE_KEYWORD (active);
SIMPLE_KEYWORD (intrans);
SIMPLE_KEYWORD (inerror);
SIMPLE_KEYWORD (unknown);

PRIMPROC
(pg_transaction_status, "pg-transaction-status", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a keyword describing the current transaction status,
one of: @code{#:idle} (connection idle),
@code{#:active} (command in progress),
@code{#:intrans} (idle, within transaction block),
@code{#:inerror} (idle, within failed transaction),
@code{#:unknown} (cannot determine status).  */)
{
#define FUNC_NAME s_pg_transaction_status
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  switch (PQtransactionStatus (dbconn))
    {
    case PQTRANS_IDLE:    return KWD (idle);
    case PQTRANS_ACTIVE:  return KWD (active);
    case PQTRANS_INTRANS: return KWD (intrans);
    case PQTRANS_INERROR: return KWD (inerror);
    case PQTRANS_UNKNOWN:
    default:              return KWD (unknown);
    }
#undef FUNC_NAME
}

PRIMPROC
(pg_parameter_status, "pg-parameter-status", 2, 0, 0,
 (SCM conn, SCM parm),
 doc: /***********
Return the status (a string) of @var{parm} for @var{conn},
or @code{#f} if there is no such parameter.
@var{parm} is a symbol, such as @code{client_encoding}.  */)
{
#define FUNC_NAME s_pg_parameter_status
  PGconn *dbconn;
  range_t cparm;
  const char *cstatus = NULL;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_SYMBOL (2, parm);
  parm = FINANGLABLE_SCHEME_STRING_FROM_SYMBOL (parm);

  FINANGLE (parm);
  cstatus = PQparameterStatus (dbconn, RS (parm));
  UNFINANGLE (parm);
  return DEFAULT_FALSE (cstatus, STRING (cstatus));
#undef FUNC_NAME
}

PRIMPROC
(pg_result_status, "pg-result-status", 1, 0, 0,
 (SCM result),
 doc: /***********
Return the symbolic status of the @var{result}
returned by @code{pg-exec}.  */)
{
#define FUNC_NAME s_pg_result_status
  PGresult *res;
  int result_status;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  result_status = PQresultStatus (res);
  INTSOK ();

  if (PGRES_FATAL_ERROR < result_status)
    result_status = PGRES_FATAL_ERROR;

  return pgrs[result_status];
#undef FUNC_NAME
}

PRIMPROC
(pg_ntuples, "pg-ntuples", 1, 0, 0,
 (SCM result),
 doc: /***********
Return the number of tuples in @var{result}.  */)
{
#define FUNC_NAME s_pg_ntuples
  PGresult *res;
  int ntuples;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  ntuples = PQntuples (res);
  INTSOK ();

  return NUM_INT (ntuples);
#undef FUNC_NAME
}

PRIMPROC
(pg_nfields, "pg-nfields", 1, 0, 0,
 (SCM result),
 doc: /***********
Return the number of fields in @var{result}.  */)
{
#define FUNC_NAME s_pg_nfields
  PGresult *res;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  rv = NUM_INT (PQnfields (res));
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

PRIMPROC
(pg_cmdtuples, "pg-cmdtuples", 1, 0, 0,
 (SCM result),
 doc: /***********
Return the number of tuples in @var{result} affected by a
command.  This is a string which is empty in the case of
commands like @code{CREATE TABLE}, @code{GRANT}, @code{REVOKE}
etc., which don't affect tuples.  */)
{
#define FUNC_NAME s_pg_cmdtuples
  PGresult *res;
  const char *cmdtuples;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  cmdtuples = PQcmdTuples (res);
  INTSOK ();

  return STRING (cmdtuples);
#undef FUNC_NAME
}

PRIMPROC
(pg_oid_value, "pg-oid-value", 1, 0, 0,
 (SCM result),
 doc: /***********
If the @var{result} is that of an SQL @code{INSERT} command,
return the integer @acronym{OID} of the inserted tuple, otherwise
@code{#f}.  */)
{
#define FUNC_NAME s_pg_oid_value
  PGresult *res;
  Oid oid_value;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  oid_value = PQoidValue (res);
  INTSOK ();

  return DEFAULT_FALSE (InvalidOid != oid_value,
                        NUM_INT (oid_value));
#undef FUNC_NAME
}

PRIMPROC
(pg_fname, "pg-fname", 2, 0, 0,
 (SCM result, SCM num),
 doc: /***********
Return a string containing the canonical lower-case name
of the field number @var{num} in @var{result}.  SQL variables
and field names are not case-sensitive.  */)
{
#define FUNC_NAME s_pg_fname
  PGresult *res;
  int field;
  const char *fname;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fname = PQfname (res, field);
  return STRING (fname);
#undef FUNC_NAME
}

PRIMPROC
(pg_fnumber, "pg-fnumber", 2, 0, 0,
 (SCM result, SCM fname),
 doc: /***********
Return the integer field-number corresponding to field
@var{fname} (a string) if this exists in @var{result}, or @code{-1}
otherwise.  */)
{
#define FUNC_NAME s_pg_fnumber
  PGresult *res;
  range_t cfname;
  int fnum;

  VALIDATE_RESULT_UNBOX (1, result, res);
  ASSERT_STRING (2, fname);

  NOINTS ();
  FINANGLE (fname);
  fnum = PQfnumber (res, RS (fname));
  UNFINANGLE (fname);
  INTSOK ();

  return NUM_INT (fnum);
#undef FUNC_NAME
}

PRIMPROC
(pg_ftable, "pg-ftable", 2, 0, 0,
 (SCM result, SCM num),
 doc: /***********
Return the @acronym{OID} of the table from which the field @var{num}
was fetched in @var{result}.  */)
{
#define FUNC_NAME s_pg_ftable
  PGresult *res;
  int field;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);

  return NUM_ULONG (PQftable (res, field));
#undef FUNC_NAME
}

PRIMPROC
(pg_ftablecol, "pg-ftablecol", 2, 0, 0,
 (SCM result, SCM num),
 doc: /***********
Return the column number (within its table) of the column
making up field @var{num} of @var{result}.  Column numbers
start at zero.  */)
{
#define FUNC_NAME s_pg_ftablecol
  PGresult *res;
  int field;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);

  return NUM_ULONG (PQftablecol (res, field));
#undef FUNC_NAME
}

PRIMPROC
(pg_fformat, "pg-fformat", 2, 0, 0,
 (SCM result, SCM num),
 doc: /***********
Return an integer indicating the format of field
@var{num} of @var{result}.  Zero (0) indicates textual
data representation; while one (1) indicates binary.  */)
{
#define FUNC_NAME s_pg_fformat
  PGresult *res;
  int field;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);

  return NUM_ULONG (PQfformat (res, field));
#undef FUNC_NAME
}

PRIMPROC
(pg_ftype, "pg-ftype", 2, 0, 0,
 (SCM result, SCM num),
 doc: /***********
Return the PostgreSQL internal integer representation of
the type of the given attribute.  The integer is actually an
@acronym{OID} (object ID) which can be used as the primary key to
reference a tuple from the system table @code{pg_type}.
Throw @code{misc-error} if the field @var{num} is
not valid for the given @code{result}.  */)
{
#define FUNC_NAME s_pg_ftype
  PGresult *res;
  int field;
  int ftype;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  ftype = PQftype (res, field);

  return NUM_INT (ftype);
#undef FUNC_NAME
}

PRIMPROC
(pg_fsize, "pg-fsize", 2, 0, 0,
 (SCM result, SCM num),
 doc: /***********
Return the size of a @var{result} field @var{num} in bytes,
or @code{-1} if the field is variable-length.  */)
{
#define FUNC_NAME s_pg_fsize
  PGresult *res;
  int field;
  int fsize;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fsize = PQfsize (res, field);
  return NUM_INT (fsize);
#undef FUNC_NAME
}

/* This is for pg-getvalue, pg-getlength, pg-getisnull.  */
#define CHECK_TUPLE_COORDS() do                                         \
    {                                                                   \
      ASSERT (stuple, ctuple < PQntuples (res), SCM_OUTOFRANGE);        \
      ASSERT (sfield, cfield < PQnfields (res), SCM_OUTOFRANGE);        \
    }                                                                   \
  while (0)

PRIMPROC
(pg_getvalue, "pg-getvalue", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 doc: /***********
Return a string containing the value of the attribute
@var{sfield}, tuple @var{stuple} of @var{result}.  It is
up to the caller to convert this to the required type.  */)
{
#define FUNC_NAME s_pg_getvalue
  PGresult *res;
  int ctuple, cfield;
  const char *val;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);
  ASSERT_EXACT_NON_NEGATIVE_COPY (2, stuple, ctuple);
  ASSERT_EXACT_NON_NEGATIVE_COPY (3, sfield, cfield);
  CHECK_TUPLE_COORDS ();

  NOINTS ();
  val = PQgetvalue (res, ctuple, cfield);
  rv = PQbinaryTuples (res)
    ? BSTRING (val, PQgetlength (res, ctuple, cfield))
    : STRING (val);
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

PRIMPROC
(pg_getlength, "pg-getlength", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 doc: /***********
Return the size in bytes of the value of the attribute
@var{sfield}, tuple @var{stuple} of @var{result}.  */)
{
#define FUNC_NAME s_pg_getlength
  PGresult *res;
  int ctuple, cfield, len;
  SCM ret;

  VALIDATE_RESULT_UNBOX (1, result, res);
  ASSERT_EXACT_NON_NEGATIVE_COPY (2, stuple, ctuple);
  ASSERT_EXACT_NON_NEGATIVE_COPY (3, sfield, cfield);
  CHECK_TUPLE_COORDS ();

  NOINTS ();
  len = PQgetlength (res, ctuple, cfield);
  INTSOK ();

  ret = NUM_INT (len);
  return ret;
#undef FUNC_NAME
}

PRIMPROC
(pg_getisnull, "pg-getisnull", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 doc: /***********
Return @code{#t} if the value of the attribute @var{sfield}, tuple
@var{stuple} of @var{result}, is @code{NULL}, @code{#f} otherwise.  */)
{
#define FUNC_NAME s_pg_getisnull
  PGresult *res;
  int ctuple, cfield;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);
  ASSERT_EXACT_NON_NEGATIVE_COPY (2, stuple, ctuple);
  ASSERT_EXACT_NON_NEGATIVE_COPY (3, sfield, cfield);
  CHECK_TUPLE_COORDS ();

  NOINTS ();
  rv = BOOLEAN (PQgetisnull (res, ctuple, cfield));
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

PRIMPROC
(pg_binary_tuples, "pg-binary-tuples?", 1, 0, 0,
 (SCM result),
 doc: /***********
Return @code{#t} if @var{result} contains binary tuple
data, @code{#f} otherwise.  */)
{
#define FUNC_NAME s_pg_binary_tuples
  PGresult *res;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  rv = BOOLEAN (PQbinaryTuples (res));
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

PRIMPROC
(pg_fmod, "pg-fmod", 2, 0, 0,
 (SCM result, SCM num),
 doc: /***********
Return the integer type-specific modification data for
the given field (field number @var{num}) of @var{result}.  */)
{
#define FUNC_NAME s_pg_fmod
  PGresult *res;
  int field;
  int fmod;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fmod = PQfmod (res, field);
  return NUM_INT (fmod);
#undef FUNC_NAME
}

PRIMPROC
(pg_put_copy_data, "pg-put-copy-data", 2, 0, 0,
 (SCM conn, SCM data),
 doc: /***********
Send on @var{conn} the @var{data} (a string).
Return one if ok; zero if the server is in nonblocking mode
and the attempt would block; and @code{-1} if an error occurred.  */)
{
#define FUNC_NAME s_pg_put_copy_data
  PGconn *dbconn;
  range_t cdata;
  int rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_STRING (2, data);

  FINANGLE_RAW (data);
  rv = PQputCopyData (dbconn, RS (data), RLEN (data));
  UNFINANGLE (data);
  return NUM_INT (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_put_copy_end, "pg-put-copy-end", 1, 1, 0,
 (SCM conn, SCM errmsg),
 doc: /***********
Send an end-of-data indication over @var{conn}.
Optional arg @var{errmsg} is a string, which if present,
forces the @code{COPY} operation to fail with @var{errmsg} as the
error message.
Return one if ok; zero if the server is in nonblocking mode
and the attempt would block; and @code{-1} if an error occurred.  */)
{
#define FUNC_NAME s_pg_put_copy_end
  PGconn *dbconn;
  range_t cerrmsg;
  int rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  RS (errmsg) = NULL;
  if (GIVENP (errmsg))
    {
      SCM_VALIDATE_STRING (2, errmsg);
      FINANGLE (errmsg);
    }

  rv = PQputCopyEnd (dbconn, RS (errmsg));
  UNFINANGLE (errmsg);
  return NUM_INT (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_copy_data, "pg-get-copy-data", 2, 1, 0,
 (SCM conn, SCM port, SCM asyncp),
 doc: /***********
Get a line of @code{COPY} data from @var{conn}, writing it to
output @var{port}.  If @var{port} is a pair, construct
a new string and set its @sc{car} to the new string.
Return the number of data bytes in the row (always greater
than zero); zero to mean the @code{COPY} is still in progress
and no data is yet available; @code{-1} to mean the @code{COPY} is
done; or @code{-2} to mean an error occurred.  */)
{
#define FUNC_NAME s_pg_get_copy_data
  PGconn *dbconn;
  int pwritep = 0, swritep = 0, rv;
  char *newbuf;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  if (SCM_OUTPUT_PORT_P (port))
    pwritep = 1;
  else
    {
      ASSERT (port, PAIRP (port), 2);
      swritep = 1;
    }

  NOINTS ();
  rv = PQgetCopyData (dbconn, &newbuf, NOT_FALSEP (asyncp));
  if (0 < rv)
    {
      if (pwritep)
        WBPORT (port, newbuf, rv);
      if (swritep)
        SETCAR (port, BSTRING (newbuf, rv));
    }
  INTSOK ();
  PQfreemem (newbuf);

  return NUM_INT (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_getline, "pg-getline", 1, 0, 0,
 (SCM conn),
 doc: /***********
Read a line from @var{conn} on which a @code{COPY <table> TO
STDOUT} has been issued.  Return a string from the connection.
A returned string consisting of a backslash followed by a full
stop signifies an end-of-copy marker.  */)
{
#define FUNC_NAME s_pg_getline
  PGconn *dbconn;
  char buf[BUF_LEN];
  int ret = 1;
  SCM box = PCHAIN (SCM_UNSPECIFIED);
  SCM tp = box;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  while (ret != 0 && ret != EOF)
    {
      NOINTS ();
      ret = PQgetline (dbconn, buf, BUF_LEN);
      INTSOK ();
      SETCDR (tp, CONS (STRING (buf), SCM_EOL));
      tp = CDR (tp);
    }
  return scm_string_append (CDR (box));
#undef FUNC_NAME
}

PRIMPROC
(pg_getlineasync, "pg-getlineasync", 2, 1, 0,
 (SCM conn, SCM buf, SCM tickle),
 doc: /***********
Read a line from @var{conn} on which a @code{COPY <table> TO
STDOUT} has been issued, into @var{buf} (a string).
Return @code{-1} to mean end-of-copy marker recognized, or a number
(possibly zero) indicating how many bytes of data were read.
The returned data may contain at most one newline (in the last
byte position).
Optional arg @var{tickle} non-@code{#f} means to do a
``consume input'' operation prior to the read.  */)
{
#define FUNC_NAME s_pg_getlineasync
  PGconn *dbconn;
  int rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, buf);

  if (GIVENP (tickle) && NOT_FALSEP (tickle))
    /* We don't care if there was an error consuming input; caller can use
       ‘pg_error_message’ to find out afterwards, or simply avoid tickling in
       the first place.  */
    PQconsumeInput (dbconn);

#if GI_LEVEL_NOT_YET_1_8
  rv = PQgetlineAsync (dbconn, SCM_ROCHARS (buf), SCM_ROLENGTH (buf));
#else  /* !GI_LEVEL_NOT_YET_1_8 */
  /* Fill a temporary C buffer then copy into the Scheme string.
     Totally gross!  */
  {
    size_t sz = scm_c_string_length (buf);
    char *cbuf = malloc (sz);

    if (! cbuf)
      SYSTEM_ERROR ();
    rv = PQgetlineAsync (dbconn, cbuf, sz);
    while (sz--)
      scm_c_string_set_x (buf, sz, CHARACTER (cbuf[sz]));
    free (cbuf);
  }
#endif /* !GI_LEVEL_NOT_YET_1_8 */

  return NUM_INT (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_putline, "pg-putline", 2, 0, 0,
 (SCM conn, SCM str),
 doc: /***********
Write a line to the connection on which a @code{COPY <table>
FROM STDIN} has been issued.  The lines written should include
the final newline characters.  The last line should be a
backslash, followed by a @samp{.} (full-stop).  After this, the
@code{pg-endcopy} procedure should be called for this
connection before any further @code{pg-exec} call is made.
Return @code{#t} if successful.  */)
{
#define FUNC_NAME s_pg_putline
  PGconn *dbconn;
  int status;
  range_t cstr;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, str);
  NOINTS ();
  FINANGLE_RAW (str);
  status = PQputnbytes (dbconn, RS (str), RLEN (str));
  UNFINANGLE (str);
  INTSOK ();
  return BOOLEAN (! status);
#undef FUNC_NAME
}

PRIMPROC
(pg_endcopy, "pg-endcopy", 1, 0, 0,
 (SCM conn),
 doc: /***********
Resynchronize with the backend process on @var{conn}.  This procedure
must be called after the last line of a table has been
transferred using @code{pg-getline}, @code{pg-getlineasync}
or @code{pg-putline}.  Return @code{#t} if successful.  */)
{
#define FUNC_NAME s_pg_endcopy
  PGconn *dbconn;
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  ret = PQendcopy (dbconn);
  INTSOK ();

  return BOOLEAN (! ret);
#undef FUNC_NAME
}

SIMPLE_KEYWORD (terse);
SIMPLE_KEYWORD (default);
SIMPLE_KEYWORD (verbose);

PRIMPROC
(pg_set_error_verbosity, "pg-set-error-verbosity", 2, 0, 0,
 (SCM conn, SCM verbosity),
 doc: /***********
Set the error verbosity for @var{conn} to @var{verbosity}.
@var{verbosity} is a keyword, one of: @code{#:terse},
@code{#:default} or @code{#:verbose}.  Return the previous
verbosity.  */)
{
#define FUNC_NAME s_pg_set_error_verbosity
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  VALIDATE_KEYWORD (2, verbosity);

  {
    PGVerbosity now = PQERRORS_DEFAULT;

    if (EQ (verbosity, KWD (terse)))
      now = PQERRORS_TERSE;
    else if (EQ (verbosity, KWD (default)))
      now = PQERRORS_DEFAULT;
    else if (EQ (verbosity, KWD (verbose)))
      now = PQERRORS_VERBOSE;
    else
      ERROR ("Invalid verbosity: ~A", verbosity);

    switch (PQsetErrorVerbosity (dbconn, now))
      {
      case PQERRORS_TERSE:   return KWD (terse);
      case PQERRORS_DEFAULT: return KWD (default);
      case PQERRORS_VERBOSE: return KWD (verbose);
      default:               RETURN_FALSE (); /* TODO: abort.  */
      }
  }
#undef FUNC_NAME
}

PRIMPROC
(pg_trace, "pg-trace", 2, 0, 0,
 (SCM conn, SCM port),
 doc: /***********
Start outputting low-level trace information on the
connection @var{conn} to @var{port}, which must have been
opened for writing.  Some consider this information more useful
for debugging PostgreSQL than for debugging its clients,
but as usual, @acronym{YMMV}.  */)
{
#define FUNC_NAME s_pg_trace
  PGconn *dbconn;
  int fd;
  FILE *fpout;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  if (CONN_FPTRACE (conn))
    ERROR ("Tracing already in progress for connection: ~S", conn);
  port = SCM_COERCE_OUTPORT (port);
  ASSERT_PORT (2, port, SCM_OPOUTFPORTP);

  if (PROB (fd = dup (SCM_FPORT_FDES (port))))
    SYSTEM_ERROR ();
  if (! (fpout = fdopen (fd, "w")))
    SYSTEM_ERROR ();

  NOINTS ();
  PQtrace (dbconn, fpout);
  CONN_FPTRACE (conn) = fpout;
  INTSOK ();

  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}

PRIMPROC
(pg_untrace, "pg-untrace", 1, 0, 0,
 (SCM conn),
 doc: /***********
Stop tracing on connection @var{conn}.  */)
{
#define FUNC_NAME s_pg_untrace
  PGconn *dbconn;
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  if (! CONN_FPTRACE (conn))
    /* We could throw an error here but that's not cool.  An error would be
       warranted in the future, however, if tracing state were to become
       nestable (with fluids, say) -- a relatively unlikely scenario.  */
    RETURN_UNSPECIFIED ();

  NOINTS ();
  PQuntrace (dbconn);
  ret = fclose (CONN_FPTRACE (conn));
  CONN_FPTRACE (conn) = NULL;
  INTSOK ();
  if (ret)
    SYSTEM_ERROR ();

  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}


/*
 * printing -- this is arguably more trouble than its worth
 */

static unsigned long int sepo_type_tag;

static inline int
sepo_p (SCM obj)
{
  return SCM_SMOB_PREDICATE (sepo_type_tag, obj);
}

static inline PQprintOpt *
sepo_unbox (SCM obj)
{
  return SMOBDATA (obj);
}

static size_t
sepo_free (SCM obj)
{
  PQprintOpt *po = sepo_unbox (obj);
  size_t size = 0;

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
  SCM_SET_SMOB_DATA (obj, NULL);

  return size;
}

static char sepo_name[] = "PG-PRINT-OPTION";

static int
sepo_display (UNUSED SCM sepo, SCM port, UNUSED scm_print_state *pstate)
{
  static char buf[20];
  static size_t len = 0;

  if (! len)
    len = snprintf (buf, 20, "#<%s>", sepo_name);

  WBPORT (port, buf, len);
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

static char *
option_as_string (SCM alist, SCM key, const char *def)
{
  SCM maybe = scm_assq_ref (alist, key);

  if (NOT_FALSEP (maybe))
    {
      range_t cmaybe;
      char *rv;

      FINANGLE (maybe);
      rv = strdup (RS (maybe));
      UNFINANGLE (maybe);
      return rv;
    }

  if (def)
    return strdup (def);

  return NULL;
}

PRIMPROC
(pg_make_print_options, "pg-make-print-options", 1, 0, 0,
 (SCM spec),
 doc: /***********
Return a @dfn{print options object} created from @var{spec},
suitable for use with @code{pg-print}.  @var{spec} is a list
of elements, each either a flag (symbol) or a key-value pair
(with the key being a symbol).  Recognized flags:

@itemize
@item header: Print output field headings and row count.
@item align: Fill align the fields.
@item standard: Old brain-dead format.
@item html3: Output HTML tables.
@item expanded: Expand tables.
@end itemize

To specify a disabled flag, use @dfn{no-FLAG}, e.g.,
@code{no-header}.  Recognized keys:

@itemize
@item field-sep
String specifying field separator.

@item table-opt
String specifying HTML table attributes.

@item caption
String specifying caption to use in HTML table.

@item field-names
List of replacement field names, each a string.
@end itemize  */)
{
#define FUNC_NAME s_pg_make_print_options
  PQprintOpt *po;
  int count = 0;                        /* of substnames */
  SCM check, substnames = SCM_EOL, flags = SCM_EOL, keys = SCM_EOL;

  ASSERT (spec, NULLP (spec) || PAIRP (spec), 1);

  /* Hairy validation/collection: symbols in ‘flags’, pairs in ‘keys’.  */
  check = spec;
  while (! NULLP (check))
    {
      SCM head = CAR (check);

#define CHECK_HEAD(expr)                        \
      ASSERT (head, (expr), 1)

      if (SYMBOLP (head))
        {
          CHECK_HEAD (MEMQ (head, valid_print_option_flags));
          flags = CONS (head, flags);
        }
      else if (PAIRP (head))
        {
          SCM key = CAR (head);
          SCM val = CDR (head);

          ASSERT (key, MEMQ (key, valid_print_option_keys), 1);
          if (key == pg_sym_field_names)
            {
              CHECK_HEAD (! NULLP (val));
              while (! NULLP (val))
                {
                  CHECK_HEAD (STRINGP (CAR (val)));
                  count++;
                  val = CDR (val);
                }
              substnames = CDR (head);    /* i.e., ‘val’ */
            }
          else
            {
              ASSERT_STRING (1, val);
              keys = CONS (head, keys);
            }
        }
      check = CDR (check);

#undef CHECK_HEAD
    }

  po = scm_must_malloc (sizeof (PQprintOpt), sepo_name);

#define _FLAG_CHECK(m)                                  \
  (MEMQ (pg_sym_no_ ## m, flags)                        \
   ? 0 : (MEMQ (pg_sym_ ## m, flags)                    \
          ? 1 : default_print_options.m))

  po->header   = _FLAG_CHECK (header);
  po->align    = _FLAG_CHECK (align);
  po->standard = _FLAG_CHECK (standard);
  po->html3    = _FLAG_CHECK (html3);
  po->expanded = _FLAG_CHECK (expanded);
  po->pager    = 0;                     /* never */
#undef _FLAG_CHECK

#define _STRING_CHECK_SETX(k,m)                         \
  po->m = option_as_string                              \
    (keys, pg_sym_ ## k, default_print_options.m)

  _STRING_CHECK_SETX (field_sep, fieldSep);
  _STRING_CHECK_SETX (table_opt, tableOpt);
  _STRING_CHECK_SETX (caption, caption);
#undef _STRING_CHECK_SETX

  if (NULLP (substnames))
    po->fieldName = NULL;
  else
    {
      int i;
      po->fieldName = (char **) scm_must_malloc ((1 + count) * sizeof (char *),
                                                 sepo_name);
      po->fieldName[count] = NULL;
      for (i = 0; i < count; i++)
        {
          SCM name = CAR (substnames);
          range_t cname;

          FINANGLE (name);
          po->fieldName[i] = strdup (RS (name));
          UNFINANGLE (name);
          substnames = CDR (substnames);
        }
    }

  SCM_RETURN_NEWSMOB (sepo_type_tag, po);
#undef FUNC_NAME
}

PRIMPROC
(pg_print, "pg-print", 1, 1, 0,
 (SCM result, SCM options),
 doc: /***********
Display @var{result} on the current output port.
Optional second arg @var{options} is an
object returned by @code{pg-make-print-options} that
specifies various parameters of the output format.  */)
{
#define FUNC_NAME s_pg_print
  PGresult *res;
  FILE *fout;
  int fd;
  SCM curout;

  VALIDATE_RESULT_UNBOX (1, result, res);
  options = (GIVENP (options)
             ? options
             : pg_make_print_options (SCM_EOL));
  ASSERT (options, sepo_p (options), 2);

  curout = scm_current_output_port ();
  fd = (SCM_OPFPORTP (curout)
        ? SCM_FPORT_FDES (curout)
        : -1);

  if (PROB (fd))
    fout = tmpfile ();
  else
    {
      scm_force_output (curout);
      if (fileno (stdout) == fd)
        fout = stdout;
      else
        {
          if (PROB (fd = dup (fd)))
            SYSTEM_ERROR ();
          fout = fdopen (fd, "w");
        }
    }

  if (! fout)
    SYSTEM_ERROR ();
  PQprint (fout, res, sepo_unbox (options));

  if (PROB (fd))
    {
      char buf[BUF_LEN];
      int howmuch = 0;

      buf[BUF_LEN - 1] = '\0';          /* elephant */
      fseek (fout, 0, SEEK_SET);

      while (BUF_LEN - 1 == (howmuch = fread (buf, 1, BUF_LEN - 1, fout)))
        WBPORT (curout, buf, howmuch);
      if (feof (fout))
        {
          buf[howmuch] = '\0';
          WBPORT (curout, buf, howmuch);
        }
    }

  if (stdout != fout)
    fclose (fout);

  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}


/* Modify notice processing.

   Note that this is not a simple wrap of ‘PQsetNoticeProcessor’.  Instead,
   we simply modify ‘xc->notice’.  Also, the value can either be a port or
   procedure that takes a string.  For these reasons, we name the procedure
   ‘pg-set-notice-out!’ to help avoid confusion.  */

PRIMPROC
(pg_set_notice_out_x, "pg-set-notice-out!", 2, 0, 0,
 (SCM conn, SCM out),
 doc: /***********
Set notice output handler of @var{conn} to @var{out}.
@var{out} can be @code{#f}, which means discard notices;
@code{#t}, which means send them to the current error port;
an output port to send the notice to; or a procedure that
takes one argument, the notice string.  It's usually a good
idea to call @code{pg-set-notice-out!} soon after establishing
the connection.  */)
{
#define FUNC_NAME s_pg_set_notice_out_x
  ASSERT_CONNECTION (1, conn);
  ASSERT (out, (BOOLEANP (out)
                || SCM_OUTPUT_PORT_P (out)
                || PROCEDUREP (out)),
          2);

  CONN_NOTICE (conn) = out;
  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}


/* Fetch asynchronous notifications.  */

PRIMPROC
(pg_notifies, "pg-notifies", 1, 1, 0,
 (SCM conn, SCM tickle),
 doc: /***********
Return the next as-yet-unhandled notification
from @var{conn}, or @code{#f} if there are none available.
The notification is a pair with @sc{car} @var{relname},
a string naming the relation containing data; and
@sc{cdr} @var{pid}, the integer @acronym{PID}
of the backend delivering the notification.
Optional arg @var{tickle} non-@code{#f} means to do a
``consume input'' operation prior to the query.  */)
{
#define FUNC_NAME s_pg_notifies
  PGconn *dbconn;
  PGnotify *n;
  SCM rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  if (GIVENP (tickle) && NOT_FALSEP (tickle))
    /* We don't care if there was an error consuming input; caller can use
       ‘pg_error_message’ to find out afterwards, or simply avoid tickling in
       the first place.  */
    PQconsumeInput (dbconn);
  n = PQnotifies (dbconn);
  if (n)
    {
      rv = CONS (STRING (n->relname),
                 NUM_INT (n->be_pid));
      PQfreemem (n);
    }
  return DEFAULT_FALSE (n, rv);
#undef FUNC_NAME
}


/* Client encoding.  */

/* Hmmm, ‘pg_encoding_to_char’ is mentioned in the PostgreSQL documentation,
   Chapter "Localization", Section "Character Set Support", but is not in
   libpq-fe.h (at least, as of PostgreSQL 7.4.21).

   We add a ‘const’ since apparently sometime between PostgreSQL 7.4.21 and
   8.2.9 the actual (installed-header) prototype changed to include ‘const’
   even though the suggested (documented) one doesn't.  Sigh.

   The whole thing is wrapped in a preprocessor conditional to handle the
   possibility that some future PostgreSQL release will include a proper
   prototype in their installed headers, and detectable at configure-time.
   (Sigh^2!)  */
#if !HAVE_DECL_PG_ENCODING_TO_CHAR
extern const char * pg_encoding_to_char (int encoding);
#endif

/* Not mentioned (even in the docs), but the same reasoning applies.
   (Sigh^3!)  */
#if !HAVE_DECL_PG_CHAR_TO_ENCODING
extern int pg_char_to_encoding (const char *name);
#endif

static SCM encoding_alist;

PRIMPROC
(pg_mblen, "pg-mblen", 2, 1, 0,
 (SCM encoding, SCM string, SCM start),
 doc: /***********
Return the number of bytes of the first character in unibyte
@var{string}, which is encoded in @var{encoding} (string or symbol).
Optional third arg @var{start} specifies the byte offset
into @var{string} to use instead of the default (zero).

Signal error if @var{encoding} is unknown or if @var{start}
is out of range.  If @var{start} is exactly the length of
@var{string}, return zero.  */)
{
#define FUNC_NAME s_pg_mblen
  SCM as_string, cell;
  range_t cstring;
  int cenc, rv;
  size_t cstart = 0;

  if (STRINGP (encoding))
    encoding = scm_string_to_symbol (encoding);
  ASSERT (encoding, SYMBOLP (encoding), 1);
  as_string = FINANGLABLE_SCHEME_STRING_FROM_SYMBOL (encoding);
  cell = scm_assq (encoding, CDR (encoding_alist));
  if (NOT_FALSEP (cell))
    cenc = C_INT (CDR (cell));
  else
    {
      range_t cas_string;

      FINANGLE (as_string);
      cenc = pg_char_to_encoding (RS (as_string));
      UNFINANGLE (as_string);
      if (PROB (cenc))
        ERROR ("No such encoding: ~A", encoding);
      cell = CONS (encoding, NUM_INT (cenc));
      SETCDR (encoding_alist, CONS (cell, CDR (encoding_alist)));
    }

  ASSERT_STRING (2, string);
  FINANGLE (string);
  if (GIVENP (start))
    {
      ASSERT_EXACT (3, start);
      cstart = C_INT (start);
      if (RLEN (string) < cstart)
        ERROR ("String start index out of range: ~A", start);
    }
  rv = (RLEN (string) == cstart
        ? 0
        /* Somewhere around PostgreSQL 8.1 the first arg type
           changed from ‘const unsigned char *’ to ‘const char *’.
           This ‘void *’ papers over that wart.  */
        : PQmblen ((void *) (RS (string) + cstart), cenc));
  UNFINANGLE (string);
  return NUM_INT (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_client_encoding, "pg-client-encoding", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return the current client encoding for @var{conn} as a string.  */)
{
#define FUNC_NAME s_pg_client_encoding
  PGconn *dbconn;
  SCM enc;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  enc = STRING (pg_encoding_to_char (PQclientEncoding (dbconn)));
  return enc;
#undef FUNC_NAME
}

PRIMPROC
(pg_set_client_encoding_x, "pg-set-client-encoding!", 2, 0, 0,
 (SCM conn, SCM encoding),
 doc: /***********
Set the client encoding for @var{conn} to @var{encoding} (a string).
Return @code{#t} if successful, @code{#f} otherwise.  */)
{
#define FUNC_NAME s_pg_set_client_encoding_x
  PGconn *dbconn;
  range_t cencoding;
  int rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, encoding);

  FINANGLE (encoding);
  rv = PQsetClientEncoding (dbconn, RS (encoding));
  UNFINANGLE (encoding);
  return BOOLEAN (! rv);
#undef FUNC_NAME
}


/*
 * non-blocking connection mode
 */

PRIMPROC
(pg_set_nonblocking_x, "pg-set-nonblocking!", 2, 0, 0,
 (SCM conn, SCM mode),
 doc: /***********
Set the nonblocking status of @var{conn} to @var{mode}.
If @var{mode} is non-@code{#f}, set it to nonblocking, otherwise
set it to blocking.  Return @code{#t} if successful.  */)
{
#define FUNC_NAME s_pg_set_nonblocking_x
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  return BOOLEAN (! PQsetnonblocking (dbconn, NOT_FALSEP (mode)));
#undef FUNC_NAME
}

PRIMPROC
(pg_is_nonblocking_p, "pg-is-nonblocking?", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return @code{#t} if @var{conn} is in nonblocking mode.  */)
{
#define FUNC_NAME s_pg_is_nonblocking_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  return BOOLEAN (PQisnonblocking (dbconn));
#undef FUNC_NAME
}


/*
 * non-blocking query operations
 */

PRIMPROC
(pg_send_query, "pg-send-query", 2, 0, 0,
 (SCM conn, SCM query),
 doc: /***********
Send @var{conn} a non-blocking @var{query} (string).
Return @code{#t} iff successful.  If not successful, error
message is retrievable with @code{pg-error-message}.  */)
{
#define FUNC_NAME s_pg_send_query
  PGconn *dbconn;
  range_t cquery;
  int rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, query);

  FINANGLE (query);
  rv = PQsendQuery (dbconn, RS (query));
  UNFINANGLE (query);
  return BOOLEAN (rv);
#undef FUNC_NAME
}

PRIMPROC
(pg_send_query_params, "pg-send-query-params", 3, 0, 0,
 (SCM conn, SCM query, SCM parms),
 doc: /***********
Like @code{pg-send-query}, except that @var{query} is a
parameterized string, and @var{parms} is a parameter-vector.  */)
{
#define FUNC_NAME s_pg_send_query_params
  PGconn *dbconn;
  range_t cquery;
  struct paramspecs ps;
  int result;

  VALIDATE_PARAM_RELATED_ARGS (query);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQsendQueryParams (dbconn, RS (query), ps.len,
                              ps.types, ps.values, ps.lengths, ps.formats,
                              RESFMT_TEXT);
  UNFINANGLE (query);
  INTSOK ();
  drop_paramspecs (&ps);
  return BOOLEAN (result);
#undef FUNC_NAME
}

PRIMPROC
(pg_send_query_prepared, "pg-send-query-prepared", 3, 0, 0,
 (SCM conn, SCM stname, SCM parms),
 doc: /***********
Like @code{pg-exec-prepared}, except asynchronous.
Also, return @code{#t} if successful.  */)
{
#define FUNC_NAME s_pg_send_query_prepared
  PGconn *dbconn;
  range_t cstname;
  struct paramspecs ps;
  int result;

  VALIDATE_PARAM_RELATED_ARGS (stname);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQsendQueryPrepared (dbconn, RS (stname), ps.len,
                                ps.values, ps.lengths, ps.formats,
                                RESFMT_TEXT);
  UNFINANGLE (stname);
  INTSOK ();
  drop_paramspecs (&ps);
  return BOOLEAN (result);
#undef FUNC_NAME
}

PRIMPROC
(pg_get_result, "pg-get-result", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return a result from @var{conn}, or @code{#f}.  */)
{
#define FUNC_NAME s_pg_get_result
  PGconn *dbconn;
  PGresult *result;
  SCM z;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  NOINTS ();
  result = PQgetResult (dbconn);
  z = res_box (result);
  INTSOK ();
  return z;
#undef FUNC_NAME
}

PRIMPROC
(pg_consume_input, "pg-consume-input", 1, 0, 0,
 (SCM conn),
 doc: /***********
Consume input from @var{conn}.  Return @code{#t} iff successful.  */)
{
#define FUNC_NAME s_pg_consume_input
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return BOOLEAN (PQconsumeInput (dbconn));
#undef FUNC_NAME
}

PRIMPROC
(pg_is_busy_p, "pg-is-busy?", 1, 0, 0,
 (SCM conn),
 doc: /***********
Return @code{#t} if there is data waiting for
@code{pg-consume-input}, otherwise @code{#f}.  */)
{
#define FUNC_NAME s_pg_is_busy_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return BOOLEAN (PQisBusy (dbconn));
#undef FUNC_NAME
}

PRIMPROC
(pg_request_cancel, "pg-request-cancel", 1, 0, 0,
 (SCM conn),
 doc: /***********
Request a cancellation on @var{conn}.
Return @code{#t} iff the cancel request was successfully
dispatched.  If not, @code{pg-error-message}
tells why not.  Successful dispatch is no guarantee
that the request will have any effect, however.
Regardless of the return value,
the client must continue with the normal
result-reading sequence using @code{pg-get-result}.
If the cancellation is effective, the current query
will terminate early and return an error result.
If the cancellation fails (say, because the backend
was already done processing the query), then there
will be no visible result at all.

Note that if the current query is part of a transaction,
cancellation will abort the whole transaction.  */)
{
#define FUNC_NAME s_pg_request_cancel
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return BOOLEAN (PQrequestCancel (dbconn));
#undef FUNC_NAME
}

PRIMPROC
(pg_flush, "pg-flush", 1, 0, 0,
 (SCM conn),
 doc: /***********
Flush output for connection @var{conn}.
Return zero if successful (or if the send queue is empty);
@code{-1} if flushing failed for some reason; or one if not all
data was sent (only possible for a non-blocking connection).  */)
{
#define FUNC_NAME s_pg_flush
  ASSERT_CONNECTION (1, conn);

  return NUM_INT (PQflush (CONN_CONN (conn)));
#undef FUNC_NAME
}


/*
 * init
 */

static
void
init_module (void)
{
  DEFSMOB (pg_conn_tag, xc_name,
           xc_mark,
           xc_free,
           xc_display);

  DEFSMOB (pg_result_tag, res_name,
           NULL,
           res_free,
           res_display);

  DEFSMOB (sepo_type_tag, sepo_name,
           NULL,
           sepo_free,
           sepo_display);

#include "libpq.x"

#define KLIST(...)  PERMANENT (PCHAIN (__VA_ARGS__))

  valid_print_option_keys
    = KLIST (pg_sym_field_sep,
             pg_sym_table_opt,
             pg_sym_caption,
             pg_sym_field_names);

  valid_print_option_flags
    = KLIST (pg_sym_header,
             pg_sym_align,
             pg_sym_standard,
             pg_sym_html3,
             pg_sym_expanded,
             pg_sym_no_header,
             pg_sym_no_align,
             pg_sym_no_standard,
             pg_sym_no_html3,
             pg_sym_no_expanded);

  encoding_alist = KLIST (SCM_BOOL_F);

  {
    unsigned int i;
    for (i = 0; i < sizeof (pgrs) / sizeof (SCM); i++)
      pgrs[i] = PERMANENT (SYMBOL (PQresStatus (i)));
  }

  lobp_tag = scm_make_port_type ("pg-lo-port", lob_fill_input, lob_write);
  scm_set_port_free          (lobp_tag, lob_free);
  scm_set_port_mark          (lobp_tag, lob_mark);
  scm_set_port_print         (lobp_tag, lob_printpt);
  scm_set_port_flush         (lobp_tag, lob_flush);
  scm_set_port_end_input     (lobp_tag, lob_end_input);
  scm_set_port_close         (lobp_tag, lob_close);
  scm_set_port_seek          (lobp_tag, lob_seek);
  scm_set_port_truncate      (lobp_tag, NULL);
  scm_set_port_input_waiting (lobp_tag, lob_input_waiting_p);

#undef KLIST
}

#ifdef USE_CMOD
MOD_INIT_LINK_THUNK ("database postgres"
                     ,database_postgres
                     ,init_module)
#else
void
guile_pg_init_database_postgres_module (void)
{
  init_module ();
}
#endif

/* libpq.c ends here */
