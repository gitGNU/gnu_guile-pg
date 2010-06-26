/* libpq.c

   Copyright (C) 2003, 2004, 2005, 2006, 2007, 2008, 2009,
     2010 Thien-Thi Nguyen
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
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

#include "gi.h"                         /* Guile interface */

#if __STDC_VERSION__ < 199901L
# if __GNUC__ >= 2
#  define __func__ __FUNCTION__
# else
#  define __func__ "<unknown>"
# endif
#endif

#ifdef __GNUC__
#define UNUSED __attribute__ ((unused))
#else
#define UNUSED
#endif

#define PROB(x)  (0 > (x))

/*
 * guile abstractions
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

static SCM
lob_mklobport (SCM conn, Oid oid, int alod, long modes, const char *FUNC_NAME)
{
  SCM port;
  lob_stream *lobp;
  scm_port *pt;

  lobp = (lob_stream *) scm_must_malloc (sizeof (lob_stream), "PG-LO-PORT");

  SCM_NEWCELL (port);

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

GH_DEFPROC
(lob_lo_creat, "pg-lo-creat", 2, 0, 0,
 (SCM conn, SCM modes),
 "Create a new large object and open a port over it for reading\n"
 "and/or writing.  @var{modes} is a string describing the mode in\n"
 "which the port is to be opened.  The mode string must include\n"
 "one of @code{r} for reading, @code{w} for writing or @code{a}\n"
 "for append (but since the object is empty to start with this is\n"
 "the same as @code{w}.)  The return value is a large object port\n"
 "which can be used to read or write data to/from the object, or\n"
 "@code{#f} on failure in which case @code{pg-error-message} from\n"
 "the connection should give some idea of what happened.\n\n"
 "In addition to returning @code{#f} on failure this procedure\n"
 "throws a @code{misc-error} if the @code{modes} string is invalid.")
{
#define FUNC_NAME s_lob_lo_creat
  long mode_bits;
  PGconn *dbconn;
  int alod = 0;
  Oid oid;
  int pg_modes = 0;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, modes);
  ROZT_X (modes);

  mode_bits = scm_mode_bits (ROZT (modes));

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

GH_DEFPROC
(lob_lo_open, "pg-lo-open", 3, 0, 0,
 (SCM conn, SCM oid, SCM modes),
 "Open a port over an existing large object.  The port can be\n"
 "used to read or write data from/to the object.  @var{oid}\n"
 "should be an integer identifier representing the large object.\n"
 "@var{modes} must be a string describing the mode in which the\n"
 "port is to be opened.  The mode string must include one of\n"
 "@code{r} for reading, @code{w} for writing, @code{a} for\n"
 "append or @code{+} with any of the above indicating both\n"
 "reading and writing/appending.  @code{A} is equivalent to\n"
 "opening the port for writing and immediately doing a\n"
 "@code{(pg-lo-seek)} to the end.  The return value is either\n"
 "an open large object port or @code{#f} on failure in which\n"
 "case @code{pg-error-message} from the connection should give\n"
 "some idea of what happened.\n\n"
 "Throw @code{misc-error} if the @code{modes} is invalid.")
{
#define FUNC_NAME s_lob_lo_open
  long mode_bits;
  PGconn *dbconn;
  int alod;
  Oid pg_oid;
  int pg_modes = 0;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_ULONG_COPY (2, oid, pg_oid);
  ASSERT_STRING (3, modes);
  ROZT_X (modes);

  mode_bits = scm_mode_bits (ROZT (modes));

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

  if (strchr (ROZT (modes), 'a'))
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

GH_DEFPROC
(lob_lo_unlink, "pg-lo-unlink", 2, 0, 0,
 (SCM conn, SCM oid),
 "Delete the large object identified by @var{oid}.\n"
 "Return @code{#t} if the object was successfully deleted,\n"
 "@code{#f} otherwise, in which case @code{pg-error-message}\n"
 "applied to @code{conn} should give an idea of what went wrong.")
{
#define FUNC_NAME s_lob_lo_unlink
  int ret, pg_oid;
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_ULONG_COPY (2, oid, pg_oid);

  NOINTS ();
  ret = lo_unlink (dbconn, pg_oid);
  INTSOK ();
  return gh_bool2scm (! PROB (ret));
#undef FUNC_NAME
}

#define ASSERT_PORT(n,arg,precisely)            \
  ASSERT (arg, precisely (arg), SCM_ARG ## n)

#define LOB_STREAM(port)  ((lob_stream *) SCM_STREAM (port))

GH_DEFPROC
(lob_lo_get_oid, "pg-lo-get-oid", 1, 0, 0,
 (SCM port),
 "Return the integer identifier of the object to which a given\n"
 "port applies.  @var{port} must be a large object port returned\n"
 "from @code{pg-lo-creat} or @code{pg-lo-open}.")
{
#define FUNC_NAME s_lob_lo_get_oid
  ASSERT_PORT (1, port, LOBPORTP);
  return gh_int2scm (LOB_STREAM (port)->oid);
#undef FUNC_NAME
}

GH_DEFPROC
(lob_lo_tell, "pg-lo-tell", 1, 0, 0,
 (SCM port),
 "Return the position of the file pointer for the given large\n"
 "object port.  @var{port} must be a large object port returned\n"
 "from @code{pg-lo-creat} or @code{pg-lo-open}.  The return\n"
 "value is either an integer greater than or equal to zero or\n"
 "@code{#f} if an error occurred.  In the latter case\n"
 "@code{pg-error-message} applied to @code{conn} should\n"
 "explain what went wrong.")
{
#define FUNC_NAME s_lob_lo_tell
  ASSERT_PORT (1, port, OPLOBPORTP);

  return scm_ftell (port);
#undef FUNC_NAME
}

/* During lob_flush error, we decide whether to use SYSTEM_ERROR ("normal"
   error mechanism) or to write directly to stderr, depending on libguile's
   variable: scm_terminating.  If it's not available in some form (see
   guile-pg.m4 comments), we arrange to unconditionally write to stderr
   instead of risking further muck-up.  */
#ifndef HAVE_SCM_TERMINATING
# ifdef HAVE_LIBGUILE_TERMINATING
extern int terminating;
#   define scm_terminating terminating
# endif /* HAVE_LIBGUILE_TERMINATING */
#endif /* !HAVE_SCM_TERMINATING */

static void
lob_flush (SCM port)
{
  const char *FUNC_NAME = "lob_flush";
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
#if defined (HAVE_SCM_TERMINATING) || defined (HAVE_LIBGUILE_TERMINATING)
          if (! scm_terminating)
            SYSTEM_ERROR ();
          else
#endif /* defined (HAVE_SCM_TERMINATING) ||
          defined (HAVE_LIBGUILE_TERMINATING) */
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
}

static void
lob_end_input (SCM port, int offset)
{
  const char *FUNC_NAME = "lob_end_input";
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
}

static off_t
lob_seek (SCM port, off_t offset, int whence)
{
  const char *FUNC_NAME = "lob_seek";
  lob_stream *lobp = LOB_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  off_t ret;

  NOINTS ();
  ret = lo_lseek (conn, lobp->alod, offset, whence);
  INTSOK ();
  if (PROB (ret))
    ERROR ("Error (~S) seeking on lo port ~S", gh_int2scm (ret), port);

  /* Adjust return value to account for guile port buffering.  */
  if (SEEK_CUR == whence)
    {
      scm_port *pt = SCM_PTAB_ENTRY (port);
      ret -= (pt->read_end - pt->read_pos);
    }

  return ret;
}

GH_DEFPROC
(lob_lo_seek, "pg-lo-seek", 3, 0, 0,
 (SCM port, SCM where, SCM whence),
 "Set the position of the next read or write to/from the given\n"
 "large object port.  @var{port} must be a large object port\n"
 "returned from @code{pg-lo-creat} or @code{pg-lo-open}.\n"
 "@var{where} is the position to set the pointer.  @var{whence}\n"
 "must be one of\n\n"
 "@table @code\n\n"
 "@item SEEK_SET\n"
 "Relative to the beginning of the file.\n\n"
 "@item SEEK_CUR\n"
 "Relative to the current position.\n\n"
 "@item SEEK_END\n"
 "Relative to the end of the file.\n\n"
 "@end table\n"
 "The return value is an integer which is the new position\n"
 "relative to the beginning of the object, or a number less than\n"
 "zero if an error occurred.")
{
#define FUNC_NAME s_lob_lo_seek
  int cwhere, cwhence;
  ASSERT_PORT (1, port, OPLOBPORTP);
  SCM_VALIDATE_INUM_COPY (2, where, cwhere);
  SCM_VALIDATE_INUM_COPY (3, whence, cwhence);

  lob_flush (port);

  return gh_int2scm (lob_seek (port, cwhere, cwhence));
#undef FUNC_NAME
}

/* Fill a port's read-buffer with a single read.  Return the first char and
   move the `read_pos' pointer past it, or return EOF if end of file.  */
static int
lob_fill_input (SCM port)
{
  const char *FUNC_NAME = "lob_fill_input";
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
    ERROR ("Error (~S) reading from lo port ~S", gh_int2scm (ret), port);
  if (pt->read_buf_size && !ret)
    return EOF;
  pt->read_pos = pt->read_buf;
  pt->read_end = pt->read_buf + ret;

  return * (pt->read_buf);
}

static void
lob_write (SCM port, const void *data, size_t size)
{
  const char *FUNC_NAME = "lob_write";
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
}

/* Check whether a port can supply input.  */
static int
lob_input_waiting_p (UNUSED SCM port)
{
  return 1;
}

GH_DEFPROC
(lob_lo_read, "pg-lo-read", 3, 0, 0,
 (SCM siz, SCM num, SCM port),
 "Read @var{num} objects each of length @var{siz} from @var{port}.\n"
 "Return a string containing the data read from the port or\n"
 "@code{#f} if an error occurred.")
{
#define FUNC_NAME s_lob_lo_read
  char *stage, *wp;
  int csiz, cnum, len, c;

  SCM_VALIDATE_INUM_MIN_COPY (1, siz, 0, csiz);
  SCM_VALIDATE_INUM_MIN_COPY (2, num, 0, cnum);
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

GH_DEFPROC
(lob_lo_import, "pg-lo-import", 2, 0, 0,
 (SCM conn, SCM filename),
 "Create a new large object and loads it with the contents of\n"
 "the specified file.  @var{filename} must be a string containing\n"
 "the name of the file to be loaded into the new object.  Return\n"
 "the integer identifier (OID) of the newly created large object,\n"
 "or @code{#f} if an error occurred, in which case\n"
 "@code{pg-error-message} should be consulted to determine\n"
 "the failure.")
{
#define FUNC_NAME s_lob_lo_import
  PGconn *dbconn;
  Oid ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, filename);
  ROZT_X (filename);

  NOINTS ();
  ret = lo_import (dbconn, ROZT (filename));
  INTSOK ();

  return DEFAULT_FALSE (InvalidOid != ret,
                        gh_int2scm (ret));
#undef FUNC_NAME
}

GH_DEFPROC
(lob_lo_export, "pg-lo-export", 3, 0, 0,
 (SCM conn, SCM oid, SCM filename),
 "Write the contents of a given large object to a file.\n"
 "@var{oid} is the integer identifying the large object to be\n"
 "exported and @var{filename} the name of the file to contain the\n"
 "object data.  Return @code{#t} on success, @code{#f} otherwise,\n"
 "in which case @code{pg-error-message} may offer an explanation\n"
 "of the failure.")
{
#define FUNC_NAME s_lob_lo_export
  PGconn *dbconn;
  Oid pg_oid;
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_ULONG_COPY (2, oid, pg_oid);
  ASSERT_STRING (3, filename);
  ROZT_X (filename);

  NOINTS ();
  ret = lo_export (dbconn, pg_oid, ROZT (filename));
  INTSOK ();

  return gh_bool2scm (! PROB (ret));
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

  return gh_str2scm (str, lc + 1 - str);
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
    ROZT_X (what);                                      \
    SCM_VALIDATE_VECTOR (3, parms);                     \
  } while (0)

#define VREF(v,i)  (SCM_VELTS (v)[i])

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
  ps->len = len = gh_vector_length (v);
  for (i = 0; i < len; i++)
    {
      elem = VREF (v, i);
      if (! gh_string_p (elem))
        ERROR ("bad parameter-vector element: ~S", elem);
    }
  ps->types = NULL;
  ps->values = (const char **) malloc (len * sizeof (char *));
  if (! ps->values)
    MEMORY_ERROR ();
  for (i = 0; i < len; i++)
    ps->values[i] = ROZT (VREF (v, i));
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
  SCM_VALIDATE_INUM_RANGE_COPY (pos, num, 0, PQnfields (res), cvar)


/*
 * meta and connection
 */

static SCM goodies;

GH_DEFPROC
(pg_guile_pg_loaded, "pg-guile-pg-loaded", 0, 0, 0,
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

GH_DEFPROC
(pg_protocol_version, "pg-protocol-version", 1, 0, 0,
 (SCM conn),
 "Return the client protocol version for @var{conn}.\n"
 "This (integer) will be 2 prior to PostgreSQL 7.4.\n"
 "If @var{conn} is not a connection object, return @code{#f}.")
{
#define FUNC_NAME s_pg_protocol_version
  PGconn *dbconn;
  int v = 2;

  if (! xc_p (conn))
    RETURN_FALSE ();

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  v = PQprotocolVersion (dbconn);

  return DEFAULT_FALSE (v, gh_int2scm (v));
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

GH_DEFPROC
(pg_conndefaults, "pg-conndefaults", 0, 0, 0,
 (void),
 "Return an alist associating options with their connection\n"
 "defaults.  The option name is a keyword.\n"
 "Each associated value is in turn a sub-alist, with\n"
 "the following keys:\n\n"
 "@itemize\n"
 "@item @code{#:envvar}\n\n"
 "@item @code{#:compiled}\n"
 "@item @code{#:val}\n"
 "@item @code{#:label}\n"
 "@item @code{#:dispchar} (character: @code{#\\*} or @code{#\\D};\n"
 "or @code{#f})\n"
 "@item @code{#:dispsize} (integer)\n"
 "@end itemize\n\n"
 "Values are strings or @code{#f}, unless noted otherwise.\n"
 "A @code{dispchar} of @code{#\\*} means the option should\n"
 "be treated like a password: user dialogs should hide\n"
 "the value; while @code{#\\D} means the option is for\n"
 "debugging purposes: probably a good idea to entirely avoid\n"
 "presenting this option in the first place.")
{
  PQconninfoOption *opt, *head;
  SCM rv = SCM_EOL;

#define PAIRM(field,exp) /* maybe */                            \
    gh_cons (KWD (field),                                       \
             DEFAULT_FALSE (opt->field && opt->field[0],        \
                            (exp)))
#define PAIRX(field,exp) /* unconditional */    \
    gh_cons (KWD (field), (exp))

  for (head = opt = PQconndefaults (); opt && opt->keyword; opt++)
    rv = gh_cons
      (PCHAIN (scm_c_make_keyword (opt->keyword),
               PAIRX (envvar,   gh_str02scm (opt->envvar)),
               PAIRM (compiled, gh_str02scm (opt->compiled)),
               PAIRM (val,      gh_str02scm (opt->val)),
               PAIRM (label,    gh_str02scm (opt->label)),
               PAIRM (dispchar, gh_char2scm (opt->dispchar[0])),
               PAIRX (dispsize, gh_int2scm (opt->dispsize))),
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

  if (gh_boolean_p (out))
    {
      if (NOT_FALSEP (out))
        out = scm_current_error_port ();
      else
        return;
    }

  if (SCM_OUTPUT_PORT_P (out))
    WBPORT (out, message, strlen (message));
  else if (gh_procedure_p (out))
    gh_apply (out, PCHAIN (gh_str02scm (message)));
  else
    abort ();
}

GH_DEFPROC
(pg_connectdb, "pg-connectdb", 1, 0, 0,
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
  PGconn *dbconn;

  ASSERT_STRING (1, constr);
  ROZT_X (constr);

  NOINTS ();
  dbconn = PQconnectdb (ROZT (constr));

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

GH_DEFPROC
(pg_connection_p, "pg-connection?", 1, 0, 0,
 (SCM obj),
 "Return @code{#t} iff @var{obj} is a connection object\n"
 "returned by @code{pg-connectdb}.")
{
  return gh_bool2scm (xc_p (obj));
}

GH_DEFPROC
(pg_finish, "pg-finish", 1, 0, 0,
 (SCM conn),
 "Close the connection @var{conn} with the backend.")
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

GH_DEFPROC
(pg_reset, "pg-reset", 1, 0, 0,
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
  NOINTS ();
  PQreset (dbconn);
  INTSOK ();

  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}

GH_DEFPROC
(pg_server_version, "pg-server-version", 1, 0, 0,
 (SCM conn),
 "Return an integer representation of the server version at @var{conn}.\n"
 "This is basically\n"
 "@example\n"
 "(+ (* 10000 @var{major}) (* 100 @var{minor}) @var{micro})\n"
 "@end example\n"
 "@noindent\n"
 "which yields 40725 for PostgreSQL 4.7.25, for example.\n"
 "Return @code{#f} if @var{conn} is closed.")
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

  return DEFAULT_FALSE (combined, gh_int2scm (combined));
#undef FUNC_NAME
}


/*
 * string and bytea escaping
 */

GH_DEFPROC
(pg_escape_string_conn, "pg-escape-string-conn", 2, 0, 0,
 (SCM conn, SCM string),
 "Return a new string made from doubling every single-quote\n"
 "char in @var{string}.\n"
 "The escaping is consistent with the encoding for @var{conn}.\n"
 "The returned string does not have surrounding single-quote chars.\n"
 "If there is an error, return @code{#f}.")
{
#define FUNC_NAME s_pg_escape_string_conn
  PGconn *dbconn;
  char *answer;
  size_t ilen, olen;
  int errcode;
  SCM rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, string);
  ROZT_X (string);

  ilen = SCM_ROLENGTH (string);
  if (! (answer = malloc (1 + 2 * ilen)))
    SYSTEM_ERROR ();

  olen = PQescapeStringConn (dbconn, answer, ROZT (string), ilen, &errcode);
  rv = DEFAULT_FALSE (! errcode,
                      gh_str02scm (answer));
  free (answer);
  return rv;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_escape_bytea_conn, "pg-escape-bytea-conn", 2, 0, 0,
 (SCM conn, SCM bytea),
 "Return a new string made from doing the ``minimal'' replacement\n"
 "of byte values with its @code{\\\\ABC} (octal) representation\n"
 "in @var{bytea} (a string).\n"
 "The escaping is consistent with the encoding for @var{conn}.\n"
 "The returned bytea does not have surrounding single-quote chars.\n"
 "If there is an error, return @code{#f}.")
{
#define FUNC_NAME s_pg_escape_bytea_conn
  PGconn *dbconn;
  unsigned char *answer;
  size_t ilen, olen;
  SCM rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, bytea);

  ilen = SCM_ROLENGTH (bytea);
  rv = DEFAULT_FALSE
    (answer = PQescapeByteaConn (dbconn, SCM_ROUCHARS (bytea), ilen, &olen),
     gh_str2scm ((char *) answer, olen ? olen - 1 : 0));
  PQfreemem (answer);
  return rv;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_unescape_bytea, "pg-unescape-bytea", 1, 0, 0,
 (SCM bytea),
 "Return a new bytea made from unescaping @var{bytea}.\n"
 "If there is an error, return @code{#f}.")
{
#define FUNC_NAME s_pg_unescape_bytea
  unsigned char *answer;
  size_t olen;
  SCM rv;

  ASSERT_STRING (1, bytea);

  rv = DEFAULT_FALSE
    (answer = PQunescapeBytea (SCM_ROUCHARS (bytea), &olen),
     gh_str2scm ((char *) answer, olen));
  PQfreemem (answer);
  return rv;
#undef FUNC_NAME
}


/*
 * exec and results
 */

/* At this time, we use `RESFMT_TEXT' exclusively.  TODO: Figure out what
   higher layers need to do/know to be able to handle both text and binary,
   then make it dynamic.  */
#define RESFMT_TEXT    0
#define RESFMT_BINARY  1

GH_DEFPROC
(pg_exec, "pg-exec", 2, 0, 0,
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
  ASSERT_STRING (2, statement);
  ROZT_X (statement);

  NOINTS ();
  result = PQexec (dbconn, ROZT (statement));

  z = res_box (result);
  INTSOK ();
  return z;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_exec_params, "pg-exec-params", 3, 0, 0,
 (SCM conn, SCM statement, SCM parms),
 "Like @code{pg-exec}, except that @var{statement} is a\n"
 "parameterized string, and @var{parms} is a parameter-vector.")
{
#define FUNC_NAME s_pg_exec_params
  SCM z;
  PGconn *dbconn;
  PGresult *result;
  struct paramspecs ps;

  VALIDATE_PARAM_RELATED_ARGS (statement);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQexecParams (dbconn, ROZT (statement), ps.len,
                         ps.types, ps.values, ps.lengths, ps.formats,
                         RESFMT_TEXT);
  z = res_box (result);
  INTSOK ();
  drop_paramspecs (&ps);
  return z;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_exec_prepared, "pg-exec-prepared", 3, 0, 0,
 (SCM conn, SCM stname, SCM parms),
 "Execute the statement named by @var{stname} (a string)\n"
 "on a given connection @var{conn} returning either a result\n"
 "object or @code{#f}.  @var{stname} must match the\n"
 "name specified in some prior SQL @code{PREPARE} statement.\n"
 "@var{parms} is a parameter-vector.")
{
#define FUNC_NAME s_pg_exec_prepared
  SCM z;
  PGconn *dbconn;
  PGresult *result;
  struct paramspecs ps;

  VALIDATE_PARAM_RELATED_ARGS (stname);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQexecPrepared (dbconn, ROZT (stname), ps.len,
                           ps.values, ps.lengths, ps.formats,
                           RESFMT_TEXT);
  z = res_box (result);
  INTSOK ();
  drop_paramspecs (&ps);
  return z;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_result_p, "pg-result?", 1, 0, 0,
 (SCM obj),
 "Return @code{#t} iff @var{obj} is a result object\n"
 "returned by @code{pg-exec}.")
{
  return gh_bool2scm (res_p (obj));
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

GH_DEFPROC
(pg_result_error_field, "pg-result-error-field", 2, 0, 0,
 (SCM result, SCM fieldcode),
 "Return information associated with @var{result}, on\n"
 "@var{fieldcode}, a keyword, or @code{#f} if either\n"
 "@var{fieldcode} is unrecognized or the field value is\n"
 "not available for some reason.  The type of the\n"
 "return value depends on @var{fieldcode}:\n\n"
 "@table @code\n"
 "@item #:severity\n"
 "@itemx #:sqlstate\n"
 "@itemx #:message-primary\n"
 "@itemx #:message-detail\n"
 "@itemx #:message-hint\n"
 "@itemx #:context\n"
 "@itemx #:source-file\n"
 "A string.  The value for @code{#:message-primary} is\n"
 "typically one line, whereas for @code{#:message-detail},\n"
 "@code{#:message-hint} and @code{#:context}, it may run\n"
 "to multiple lines.  For @code{#:sqlstate}, it is always\n"
 "five characters long.\n"
 "@item #:statement-position\n"
 "@itemx #:source-line\n"
 "An integer.  Statement position counts characters\n"
 "(not bytes), starting from 1.\n"
 "@item #:source-function\n"
 "A symbol.\n"
 "@end table")
{
#define FUNC_NAME s_pg_result_error_field
  PGresult *res;
  int fc;
  char *s;

  VALIDATE_RESULT_UNBOX (1, result, res);
  SCM_VALIDATE_KEYWORD (2, fieldcode);

#define CHKFC(x,y)                              \
  if (gh_eq_p (fieldcode, (x)))                 \
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
          rv = gh_eval_str (s);
          break;
        case PG_DIAG_SOURCE_FUNCTION:
          rv = gh_symbol2scm (s);
          break;
        default:
          rv = gh_str02scm (s);
        }
    }

  RETURN_FALSE ();
#undef FUNC_NAME
}

GH_DEFPROC
(pg_result_error_message, "pg-result-error-message", 1, 0, 0,
 (SCM result),
 "Return the error message associated with @var{result},\n"
 "or the empty string if there was no error.\n"
 "If the installation does not support\n"
 "@code{PQRESULTERRORMESSAGE}, return the empty string.\n"
 "The returned string has no trailing newlines.")
{
#define FUNC_NAME s_pg_result_error_message
  PGresult *res;
  char *msg;

  VALIDATE_RESULT_UNBOX (1, result, res);
  msg = PQresultErrorMessage (res);
  return strip_newlines (msg);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_error_message, "pg-error-message", 1, 0, 0,
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

GH_DEFPROC
(pg_get_db, "pg-get-db", 1, 0, 0,
 (SCM conn),
 "Return a string containing the name of the database\n"
 "to which @var{conn} represents a connection.")
{
#define FUNC_NAME s_pg_get_db
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQdb (dbconn);
  INTSOK ();

  return gh_str02scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_user, "pg-get-user", 1, 0, 0,
 (SCM conn),
 "Return a string containing the user name used to\n"
 "authenticate the connection @var{conn}.")
{
#define FUNC_NAME s_pg_get_user
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQuser (dbconn);
  INTSOK ();

  return gh_str02scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_pass, "pg-get-pass", 1, 0, 0,
 (SCM conn),
 "Return a string containing the password used to\n"
 "authenticate the connection @var{conn}.")
{
#define FUNC_NAME s_pg_get_pass
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQpass (dbconn);
  INTSOK ();

  return gh_str02scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_host, "pg-get-host", 1, 0, 0,
 (SCM conn),
 "Return a string containing the name of the host to which\n"
 "@var{conn} represents a connection.")
{
#define FUNC_NAME s_pg_get_host
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQhost (dbconn);
  INTSOK ();

  return gh_str02scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_port,"pg-get-port", 1, 0, 0,
 (SCM conn),
 "Return a string containing the port number to which\n"
 "@var{conn} represents a connection.")
{
#define FUNC_NAME s_pg_get_port
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQport (dbconn);
  INTSOK ();

  return gh_str02scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_tty, "pg-get-tty", 1, 0, 0,
 (SCM conn),
 "Return a string containing the the name of the\n"
 "diagnostic tty for @var{conn}.")
{
#define FUNC_NAME s_pg_get_tty
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQtty (dbconn);
  INTSOK ();

  return gh_str02scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_options, "pg-get-options", 1, 0, 0,
 (SCM conn),
 "Return a string containing the the options string for @var{conn}.")
{
#define FUNC_NAME s_pg_get_options
  PGconn *dbconn;
  const char *rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  rv = PQoptions (dbconn);
  INTSOK ();

  return gh_str02scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_backend_pid, "pg-backend-pid", 1, 0, 0,
 (SCM conn),
 "Return an integer which is the the PID of the backend\n"
 "process for @var{conn}.")
{
#define FUNC_NAME s_pg_backend_pid
  PGconn *dbconn;
  int pid;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  pid = PQbackendPID (dbconn);
  INTSOK ();

  return gh_int2scm (pid);
#undef FUNC_NAME
}

SIMPLE_KEYWORD (idle);
SIMPLE_KEYWORD (active);
SIMPLE_KEYWORD (intrans);
SIMPLE_KEYWORD (inerror);
SIMPLE_KEYWORD (unknown);

GH_DEFPROC
(pg_transaction_status, "pg-transaction-status", 1, 0, 0,
 (SCM conn),
 "Return a keyword describing the current transaction status,\n"
 "one of: @code{#:idle} (connection idle),\n"
 "@code{#:active} (command in progress),\n"
 "@code{#:intrans} (idle, within transaction block),\n"
 "@code{#:inerror} (idle, within failed transaction),\n"
 "@code{#:unknown} (cannot determine status).")
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

GH_DEFPROC
(pg_parameter_status, "pg-parameter-status", 2, 0, 0,
 (SCM conn, SCM parm),
 "Return the status (a string) of @var{parm} for @var{conn},\n"
 "or @code{#f} if there is no such parameter.\n"
 "@var{parm} is a symbol, such as @code{client_encoding}.")
{
#define FUNC_NAME s_pg_parameter_status
  PGconn *dbconn;
  const char *cstatus = NULL;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_SYMBOL (2, parm);

  cstatus = PQparameterStatus (dbconn, ROZT (parm));
  return DEFAULT_FALSE (cstatus, gh_str02scm (cstatus));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_result_status, "pg-result-status", 1, 0, 0,
 (SCM result),
 "Return the symbolic status of a @code{PG_RESULT} object\n"
 "returned by @code{pg-exec}.")
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

GH_DEFPROC
(pg_ntuples, "pg-ntuples", 1, 0, 0,
 (SCM result),
 "Return the number of tuples in @var{result}.")
{
#define FUNC_NAME s_pg_ntuples
  PGresult *res;
  int ntuples;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  ntuples = PQntuples (res);
  INTSOK ();

  return gh_int2scm (ntuples);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_nfields, "pg-nfields", 1, 0, 0,
 (SCM result),
 "Return the number of fields in @var{result}.")
{
#define FUNC_NAME s_pg_nfields
  PGresult *res;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  rv = gh_int2scm (PQnfields (res));
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_cmdtuples, "pg-cmdtuples", 1, 0, 0,
 (SCM result),
 "Return the number of tuples in @var{result} affected by a\n"
 "command.  This is a string which is empty in the case of\n"
 "commands like @code{CREATE TABLE}, @code{GRANT}, @code{REVOKE}\n"
 "etc. which don't affect tuples.")
{
#define FUNC_NAME s_pg_cmdtuples
  PGresult *res;
  const char *cmdtuples;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  cmdtuples = PQcmdTuples (res);
  INTSOK ();

  return gh_str02scm (cmdtuples);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_oid_value, "pg-oid-value", 1, 0, 0,
 (SCM result),
 "If the @var{result} is that of an SQL @code{INSERT} command,\n"
 "return the integer OID of the inserted tuple, otherwise return\n"
 "@code{#f}.")
{
#define FUNC_NAME s_pg_oid_value
  PGresult *res;
  Oid oid_value;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  oid_value = PQoidValue (res);
  INTSOK ();

  return DEFAULT_FALSE (InvalidOid != oid_value,
                        gh_int2scm (oid_value));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_fname, "pg-fname", 2, 0, 0,
 (SCM result, SCM num),
 "Return a string containing the canonical lower-case name\n"
 "of the field number @var{num} in @var{result}.  SQL variables\n"
 "and field names are not case-sensitive.")
{
#define FUNC_NAME s_pg_fname
  PGresult *res;
  int field;
  const char *fname;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fname = PQfname (res, field);
  return gh_str02scm (fname);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_fnumber, "pg-fnumber", 2, 0, 0,
 (SCM result, SCM fname),
 "Return the integer field-number corresponding to field\n"
 "@var{fname} if this exists in @var{result}, or @code{-1}\n"
 "otherwise.")
{
#define FUNC_NAME s_pg_fnumber
  PGresult *res;
  int fnum;

  VALIDATE_RESULT_UNBOX (1, result, res);
  ASSERT_STRING (2, fname);
  ROZT_X (fname);

  NOINTS ();
  fnum = PQfnumber (res, ROZT (fname));
  INTSOK ();

  return gh_int2scm (fnum);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_ftable, "pg-ftable", 2, 0, 0,
 (SCM result, SCM num),
 "Return the OID of the table from which the field @var{num}\n"
 "was fetched in @var{result}.")
{
#define FUNC_NAME s_pg_ftable
  PGresult *res;
  int field;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);

  return gh_ulong2scm (PQftable (res, field));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_ftablecol, "pg-ftablecol", 2, 0, 0,
 (SCM result, SCM num),
 "Return the column number (within its table) of the column\n"
 "making up field @var{num} of @var{result}.  Column numbers\n"
 "start at 0.")
{
#define FUNC_NAME s_pg_ftablecol
  PGresult *res;
  int field;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);

  return gh_ulong2scm (PQftablecol (res, field));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_fformat, "pg-fformat", 2, 0, 0,
 (SCM result, SCM num),
 "Return the format code indicating the format of field\n"
 "@var{num} of @var{result}.  Zero (0) indicates textual\n"
 "data representation; while one (1) indicates binary.")
{
#define FUNC_NAME s_pg_fformat
  PGresult *res;
  int field;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);

  return gh_ulong2scm (PQfformat (res, field));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_ftype, "pg-ftype", 2, 0, 0,
 (SCM result, SCM num),
 "Return the PostgreSQL internal integer representation of\n"
 "the type of the given attribute.  The integer is actually an\n"
 "OID (object ID) which can be used as the primary key to\n"
 "reference a tuple from the system table @code{pg_type}.  A\n"
 "@code{misc-error} is thrown if the @code{field-number} is\n"
 "not valid for the given @code{result}.")
{
#define FUNC_NAME s_pg_ftype
  PGresult *res;
  int field;
  int ftype;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  ftype = PQftype (res, field);

  return gh_int2scm (ftype);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_fsize, "pg-fsize", 2, 0, 0,
 (SCM result, SCM num),
 "Return the size of a @var{result} field @var{num} in bytes,\n"
 "or -1 if the field is variable-length.")
{
#define FUNC_NAME s_pg_fsize
  PGresult *res;
  int field;
  int fsize;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fsize = PQfsize (res, field);
  return gh_int2scm (fsize);
#undef FUNC_NAME
}

/* This is for pg-getvalue, pg-getlength, pg-getisnull.  */
#define CHECK_TUPLE_COORDS() do                                         \
    {                                                                   \
      ASSERT (stuple, ctuple < PQntuples (res), SCM_OUTOFRANGE);        \
      ASSERT (sfield, cfield < PQnfields (res), SCM_OUTOFRANGE);        \
    }                                                                   \
  while (0)

GH_DEFPROC
(pg_getvalue, "pg-getvalue", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 "Return a string containing the value of the attribute\n"
 "@var{sfield}, tuple @var{stuple} of @var{result}.  It is\n"
 "up to the caller to convert this to the required type.")
{
#define FUNC_NAME s_pg_getvalue
  PGresult *res;
  int ctuple, cfield;
  const char *val;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);
  SCM_VALIDATE_INUM_MIN_COPY (2, stuple, 0, ctuple);
  SCM_VALIDATE_INUM_MIN_COPY (3, sfield, 0, cfield);
  CHECK_TUPLE_COORDS ();

  NOINTS ();
  val = PQgetvalue (res, ctuple, cfield);
  rv = PQbinaryTuples (res)
    ? gh_str2scm (val, PQgetlength (res, ctuple, cfield))
    : gh_str02scm (val);
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_getlength, "pg-getlength", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 "The size of the datum in bytes.")
{
#define FUNC_NAME s_pg_getlength
  PGresult *res;
  int ctuple, cfield, len;
  SCM ret;

  VALIDATE_RESULT_UNBOX (1, result, res);
  SCM_VALIDATE_INUM_MIN_COPY (2, stuple, 0, ctuple);
  SCM_VALIDATE_INUM_MIN_COPY (3, sfield, 0, cfield);
  CHECK_TUPLE_COORDS ();

  NOINTS ();
  len = PQgetlength (res, ctuple, cfield);
  INTSOK ();

  ret = gh_int2scm (len);
  return ret;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_getisnull, "pg-getisnull", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 "Return @code{#t} if the attribute is @code{NULL},\n"
 "@code{#f} otherwise.")
{
#define FUNC_NAME s_pg_getisnull
  PGresult *res;
  int ctuple, cfield;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);
  SCM_VALIDATE_INUM_MIN_COPY (2, stuple, 0, ctuple);
  SCM_VALIDATE_INUM_MIN_COPY (3, sfield, 0, cfield);
  CHECK_TUPLE_COORDS ();

  NOINTS ();
  rv = gh_bool2scm (PQgetisnull (res, ctuple, cfield));
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_binary_tuples, "pg-binary-tuples?", 1, 0, 0,
 (SCM result),
 "Return @code{#t} if @var{result} contains binary tuple\n"
 "data, @code{#f} otherwise.")
{
#define FUNC_NAME s_pg_binary_tuples
  PGresult *res;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);

  NOINTS ();
  rv = gh_bool2scm (PQbinaryTuples (res));
  INTSOK ();

  return rv;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_fmod, "pg-fmod", 2, 0, 0,
 (SCM result, SCM num),
 "Return the integer type-specific modification data for\n"
 "the given field (field number @var{num}) of @var{result}.")
{
#define FUNC_NAME s_pg_fmod
  PGresult *res;
  int field;
  int fmod;

  VALIDATE_RESULT_UNBOX (1, result, res);
  VALIDATE_FIELD_NUMBER_COPY (2, num, res, field);
  fmod = PQfmod (res, field);
  return gh_int2scm (fmod);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_put_copy_data, "pg-put-copy-data", 2, 0, 0,
 (SCM conn, SCM data),
 "Send on @var{conn} the @var{data} (a string).\n"
 "Return 1 if ok; 0 if the server is in nonblocking mode\n"
 "and the attempt would block; and -1 if an error occurred.")
{
#define FUNC_NAME s_pg_put_copy_data
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_STRING (2, data);

  ROZT_X (data);
  return gh_int2scm (PQputCopyData (dbconn,
                                    ROZT (data),
                                    SCM_ROLENGTH (data)));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_put_copy_end, "pg-put-copy-end", 1, 1, 0,
 (SCM conn, SCM errmsg),
 "Send an end-of-data indication over @var{conn}.\n"
 "Optional arg @var{errmsg} is a string, which if present,\n"
 "forces the COPY operation to fail with @var{errmsg} as the\n"
 "error message.\n"
 "Return 1 if ok; 0 if the server is in nonblocking mode\n"
 "and the attempt would block; and -1 if an error occurred.")
{
#define FUNC_NAME s_pg_put_copy_end
  PGconn *dbconn;
  char *cerrmsg = NULL;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  if (GIVENP (errmsg))
    {
      SCM_VALIDATE_STRING (2, errmsg);
      ROZT_X (errmsg);
      cerrmsg = ROZT (errmsg);
    }

  return gh_int2scm (PQputCopyEnd (dbconn, cerrmsg));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_copy_data, "pg-get-copy-data", 2, 1, 0,
 (SCM conn, SCM port, SCM asyncp),
 "Get a line of COPY data from @var{conn}, writing it to\n"
 "output @var{port}.  If @var{port} is a pair, construct\n"
 "a new string and set its @sc{car} to the new string.\n"
 "Return the number of data bytes in the row (always greater\n"
 "than zero); or zero to mean the COPY is still in progress\n"
 "and no data is yet available; or -1 to mean the COPY is\n"
 "done; or -2 to mean an error occurred.")
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
      ASSERT (port, gh_pair_p (port), 2);
      swritep = 1;
    }

  NOINTS ();
  rv = PQgetCopyData (dbconn, &newbuf, NOT_FALSEP (asyncp));
  if (0 < rv)
    {
      if (pwritep)
        WBPORT (port, newbuf, rv);
      if (swritep)
        gh_set_car_x (port, gh_str2scm (newbuf, rv));
    }
  INTSOK ();
  PQfreemem (newbuf);

  return gh_int2scm (rv);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_getline, "pg-getline", 1, 0, 0,
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
  SCM box = PCHAIN (SCM_UNSPECIFIED);
  SCM tp = box;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  while (ret != 0 && ret != EOF)
    {
      NOINTS ();
      ret = PQgetline (dbconn, buf, BUF_LEN);
      INTSOK ();
      gh_set_cdr_x (tp, gh_cons (gh_str02scm (buf), SCM_EOL));
      tp = gh_cdr (tp);
    }
  return scm_string_append (gh_cdr (box));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_getlineasync, "pg-getlineasync", 2, 1, 0,
 (SCM conn, SCM buf, SCM tickle),
 "Read a line from @var{conn} on which a @code{COPY <table> TO\n"
 "STDOUT} has been issued, into @var{buf} (a string).\n"
 "Return -1 to mean end-of-copy marker recognized, or a number\n"
 "(possibly zero) indicating how many bytes of data were read.\n"
 "The returned data may contain at most one newline (in the last\n"
 "byte position).\n"
 "Optional arg @var{tickle} non-@code{#f} means to do a\n"
 "\"consume input\" operation prior to the read.")
{
#define FUNC_NAME s_pg_getlineasync
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, buf);

  if (GIVENP (tickle) && NOT_FALSEP (tickle))
    /* We don't care if there was an error consuming input; caller can use
       `pg_error_message' to find out afterwards, or simply avoid tickling in
       the first place.  */
    PQconsumeInput (dbconn);

  return gh_int2scm (PQgetlineAsync (dbconn,
                                     SCM_ROCHARS (buf),
                                     SCM_ROLENGTH (buf)));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_putline, "pg-putline", 2, 0, 0,
 (SCM conn, SCM str),
 "Write a line to the connection on which a @code{COPY <table>\n"
 "FROM STDIN} has been issued.  The lines written should include\n"
 "the final newline characters.  The last line should be a\n"
 "backslash, followed by a full-stop.  After this, the\n"
 "@code{pg-endcopy} procedure should be called for this\n"
 "connection before any further @code{pg-exec} call is made.\n"
 "Return @code{#t} if successful.")
{
#define FUNC_NAME s_pg_putline
  PGconn *dbconn;
  int status;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, str);
  NOINTS ();
  status = PQputnbytes (dbconn, SCM_ROCHARS (str), SCM_ROLENGTH (str));
  INTSOK ();
  return gh_bool2scm (! status);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_endcopy, "pg-endcopy", 1, 0, 0,
 (SCM conn),
 "Resynchronize with the backend process.  This procedure\n"
 "must be called after the last line of a table has been\n"
 "transferred using @code{pg-getline}, @code{pg-getlineasync}\n"
 "or @code{pg-putline}.  Return @code{#t} if successful.")
{
#define FUNC_NAME s_pg_endcopy
  PGconn *dbconn;
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  NOINTS ();
  ret = PQendcopy (dbconn);
  INTSOK ();

  return gh_bool2scm (! ret);
#undef FUNC_NAME
}

SIMPLE_KEYWORD (terse);
SIMPLE_KEYWORD (default);
SIMPLE_KEYWORD (verbose);

GH_DEFPROC
(pg_set_error_verbosity, "pg-set-error-verbosity", 2, 0, 0,
 (SCM conn, SCM verbosity),
 "Set the error verbosity for @var{conn} to @var{verbosity}.\n"
 "@var{verbosity} is a keyword, one of: @code{#:terse},\n"
 "@code{#:default} or @code{#:verbose}.  Return the previous\n"
 "verbosity.")
{
#define FUNC_NAME s_pg_set_error_verbosity
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_KEYWORD (2, verbosity);

  {
    PGVerbosity now = PQERRORS_DEFAULT;

    if (gh_eq_p (verbosity, KWD (terse)))
      now = PQERRORS_TERSE;
    else if (gh_eq_p (verbosity, KWD (default)))
      now = PQERRORS_DEFAULT;
    else if (gh_eq_p (verbosity, KWD (verbose)))
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

GH_DEFPROC
(pg_trace, "pg-trace", 2, 0, 0,
 (SCM conn, SCM port),
 "Start outputting low-level trace information on the\n"
 "connection @var{conn} to @var{port}, which must have been\n"
 "opened for writing.  This trace is more useful for debugging\n"
 "PostgreSQL than it is for debugging its clients.\n"
 "The return value is unspecified.")
{
#define FUNC_NAME s_pg_trace
  PGconn *dbconn;
  int fd;
  FILE *fpout;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT (conn, ! CONN_FPTRACE (conn),
          "tracing already in progress for connection: ~S");
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

GH_DEFPROC
(pg_untrace, "pg-untrace", 1, 0, 0,
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

static long sepo_type_tag;

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

GH_DEFPROC
(pg_make_print_options, "pg-make-print-options", 1, 0, 0,
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
  SCM check, substnames = SCM_EOL, flags = SCM_EOL, keys = SCM_EOL;

  ASSERT (spec, gh_null_p (spec) || gh_pair_p (spec), 1);

  /* Hairy validation/collection: symbols in `flags', pairs in `keys'.  */
  check = spec;
  while (! gh_null_p (check))
    {
      SCM head = gh_car (check);

#define CHECK_HEAD(expr)                        \
      ASSERT (head, (expr), 1)

      if (gh_symbol_p (head))
        {
          CHECK_HEAD (NOT_FALSEP (gh_memq (head, valid_print_option_flags)));
          flags = gh_cons (head, flags);
        }
      else if (gh_pair_p (head))
        {
          SCM key = gh_car (head);
          SCM val = gh_cdr (head);

          ASSERT (key, NOT_FALSEP (gh_memq (key, valid_print_option_keys)),
                  1);
          if (key == pg_sym_field_names)
            {
              CHECK_HEAD (! gh_null_p (val));
              while (! gh_null_p (val))
                {
                  CHECK_HEAD (gh_string_p (gh_car (val)));
                  count++;
                  val = gh_cdr (val);
                }
              substnames = gh_cdr (head);    /* i.e., `val' */
            }
          else
            {
              ASSERT_STRING (1, val);
              keys = gh_cons (head, keys);
            }
        }
      check = gh_cdr (check);

#undef CHECK_HEAD
    }

  po = scm_must_malloc (sizeof (PQprintOpt), sepo_name);

#define _FLAG_CHECK(m)                                  \
  (NOT_FALSEP (gh_memq (pg_sym_no_ ## m, flags))        \
   ? 0 : (NOT_FALSEP (gh_memq (pg_sym_ ## m, flags))    \
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

  if (gh_null_p (substnames))
    po->fieldName = NULL;
  else
    {
      int i;
      po->fieldName = (char **) scm_must_malloc ((1 + count) * sizeof (char *),
                                                 sepo_name);
      po->fieldName[count] = NULL;
      for (i = 0; i < count; i++)
        {
          po->fieldName[i] = strdup (SCM_ROCHARS (gh_car (substnames)));
          substnames = gh_cdr (substnames);
        }
    }

  SCM_RETURN_NEWSMOB (sepo_type_tag, po);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_print, "pg-print", 1, 1, 0,
 (SCM result, SCM options),
 "Display @var{result} on the current output port.\n"
 "Optional second arg @var{options} is an\n"
 "object returned by @code{pg-make-print-options} that\n"
 "specifies various parameters of the output format.")
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

  fd = (SCM_OPFPORTP (curout = scm_current_output_port ())
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

   Note that this is not a simple wrap of `PQsetNoticeProcessor'.  Instead,
   we simply modify `xc->notice'.  Also, the value can either be a port or
   procedure that takes a string.  For these reasons, we name the procedure
   `pg-set-notice-out!' to help avoid confusion.  */

GH_DEFPROC
(pg_set_notice_out_x, "pg-set-notice-out!", 2, 0, 0,
 (SCM conn, SCM out),
 "Set notice output handler of @var{conn} to @var{out}.\n"
 "@var{out} can be @code{#f}, which means discard notices;\n"
 "@code{#t}, which means send them to the current error port;\n"
 "an output port to send the notice to; or a procedure that\n"
 "takes one argument, the notice string.  It's usually a good\n"
 "idea to call @code{pg-set-notice-out!} soon after establishing\n"
 "the connection.")
{
#define FUNC_NAME s_pg_set_notice_out_x
  ASSERT_CONNECTION (1, conn);
  ASSERT (out, (gh_boolean_p (out)
                || SCM_OUTPUT_PORT_P (out)
                || gh_procedure_p (out)),
          2);

  CONN_NOTICE (conn) = out;
  RETURN_UNSPECIFIED ();
#undef FUNC_NAME
}


/* Fetch asynchronous notifications.  */

GH_DEFPROC
(pg_notifies, "pg-notifies", 1, 1, 0,
 (SCM conn, SCM tickle),
 "Return the next as-yet-unhandled notification\n"
 "from @var{conn}, or @code{#f} if there are none available.\n"
 "The notification is a pair with @sc{car} @var{relname},\n"
 "a string naming the relation containing data; and\n"
 "@sc{cdr} @var{pid}, the integer pid\n"
 "of the backend delivering the notification.\n"
 "Optional arg @var{tickle} non-@code{#f} means to do a\n"
 "\"consume input\" operation prior to the query.")
{
#define FUNC_NAME s_pg_notifies
  PGconn *dbconn;
  PGnotify *n;
  SCM rv;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  if (GIVENP (tickle) && NOT_FALSEP (tickle))
    /* We don't care if there was an error consuming input; caller can use
       `pg_error_message' to find out afterwards, or simply avoid tickling in
       the first place.  */
    PQconsumeInput (dbconn);
  n = PQnotifies (dbconn);
  if (n)
    {
      rv = gh_cons (gh_str02scm (n->relname),
                    gh_int2scm (n->be_pid));
      PQfreemem (n);
    }
  return DEFAULT_FALSE (n, rv);
#undef FUNC_NAME
}


/* Client encoding.  */

/* Hmmm, `pg_encoding_to_char' is mentioned in the PostgreSQL documentation,
   Chapter "Localization", Section "Character Set Support", but is not in
   libpq-fe.h (at least, as of PostgreSQL 7.4.21).

   We add a `const' since apparently sometime between PostgreSQL 7.4.21 and
   8.2.9 the actual (installed-header) prototype changed to include `const'
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

GH_DEFPROC
(pg_mblen, "pg-mblen", 2, 1, 0,
 (SCM encoding, SCM string, SCM start),
 "Return the number of bytes of the first character in unibyte\n"
 "@var{string}, which is encoded in @var{encoding} (string or symbol).\n"
 "Optional third arg @var{start} specifies the byte offset\n"
 "into @var{string} to use instead of the default (zero).\n\n"
 "Signal error if @var{encoding} is unknown or if @var{start}\n"
 "is out of range.  If @var{start} is exactly the length of\n"
 "@var{string}, return 0 (zero).")
{
#define FUNC_NAME s_pg_mblen
  SCM cell;
  int cenc;
  size_t cstart = 0, clen;

  if (gh_string_p (encoding))
    encoding = gh_symbol2scm (ROZT (encoding));
  ASSERT (encoding, gh_symbol_p (encoding), 1);
  cell = gh_assq (encoding, gh_cdr (encoding_alist));
  if (NOT_FALSEP (cell))
    cenc = gh_scm2int (gh_cdr (cell));
  else
    {
      if (PROB (cenc = pg_char_to_encoding (ROZT (encoding))))
        ERROR ("No such encoding: ~A", encoding);
      cell = gh_cons (encoding, gh_int2scm (cenc));
      gh_set_cdr_x (encoding_alist, gh_cons (cell, gh_cdr (encoding_alist)));
    }

  ASSERT_STRING (2, string);
  clen = SCM_ROLENGTH (string);
  if (GIVENP (start))
    {
      ASSERT (start, gh_exact_p (start), 3);
      cstart = gh_scm2int (start);
      if (clen < cstart)
        ERROR ("String start index out of range: ~A", start);
    }
  return gh_int2scm ((clen == cstart)
                     ? 0
                     : PQmblen ((unsigned char *) ROZT (string) + cstart, cenc));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_client_encoding, "pg-client-encoding", 1, 0, 0,
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

GH_DEFPROC
(pg_set_client_encoding_x, "pg-set-client-encoding!", 2, 0, 0,
 (SCM conn, SCM encoding),
 "Set the client encoding for @var{conn} to @var{encoding}.\n"
 "Return @code{#t} if successful, #f otherwise.")
{
#define FUNC_NAME s_pg_set_client_encoding_x
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, encoding);
  ROZT_X (encoding);

  return gh_bool2scm (! PQsetClientEncoding (dbconn, ROZT (encoding)));
#undef FUNC_NAME
}


/*
 * non-blocking connection mode
 */

GH_DEFPROC
(pg_set_nonblocking_x, "pg-set-nonblocking!", 2, 0, 0,
 (SCM conn, SCM mode),
 "Set the nonblocking status of @var{conn} to @var{mode}.\n"
 "If @var{mode} is non-@code{#f}, set it to nonblocking, otherwise\n"
 "set it to blocking.  Return @code{#t} if successful.")
{
#define FUNC_NAME s_pg_set_nonblocking_x
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  return gh_bool2scm (! PQsetnonblocking (dbconn, NOT_FALSEP (mode)));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_is_nonblocking_p, "pg-is-nonblocking?", 1, 0, 0,
 (SCM conn),
 "Return @code{#t} if @var{conn} is in nonblocking mode.")
{
#define FUNC_NAME s_pg_is_nonblocking_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  return gh_bool2scm (PQisnonblocking (dbconn));
#undef FUNC_NAME
}


/*
 * non-blocking query operations
 */

GH_DEFPROC
(pg_send_query, "pg-send-query", 2, 0, 0,
 (SCM conn, SCM query),
 "Send @var{conn} a non-blocking @var{query} (string).\n"
 "Return @code{#t} iff successful.  If not successful, error\n"
 "message is retrievable with @code{pg-error-message}.")
{
#define FUNC_NAME s_pg_send_query
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  ASSERT_STRING (2, query);
  ROZT_X (query);

  return gh_bool2scm (PQsendQuery (dbconn, ROZT (query)));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_send_query_params, "pg-send-query-params", 3, 0, 0,
 (SCM conn, SCM query, SCM parms),
 "Like @code{pg-send-query}, except that @var{query} is a\n"
 "parameterized string, and @var{parms} is a parameter-vector.")
{
#define FUNC_NAME s_pg_send_query_params
  PGconn *dbconn;
  struct paramspecs ps;
  int result;

  VALIDATE_PARAM_RELATED_ARGS (query);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQsendQueryParams (dbconn, ROZT (query), ps.len,
                              ps.types, ps.values, ps.lengths, ps.formats,
                              RESFMT_TEXT);
  INTSOK ();
  drop_paramspecs (&ps);
  return gh_bool2scm (result);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_send_query_prepared, "pg-send-query-prepared", 3, 0, 0,
 (SCM conn, SCM stname, SCM parms),
 "Like @code{pg-exec-prepared}, except asynchronous.\n"
 "Also, return @code{#t} if successful.")
{
#define FUNC_NAME s_pg_send_query_prepared
  PGconn *dbconn;
  struct paramspecs ps;
  int result;

  VALIDATE_PARAM_RELATED_ARGS (stname);

  prep_paramspecs (FUNC_NAME, &ps, parms);
  NOINTS ();
  result = PQsendQueryPrepared (dbconn, ROZT (stname), ps.len,
                                ps.values, ps.lengths, ps.formats,
                                RESFMT_TEXT);
  INTSOK ();
  drop_paramspecs (&ps);
  return gh_bool2scm (result);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_result, "pg-get-result", 1, 0, 0,
 (SCM conn),
 "Return a result from @var{conn}, or @code{#f}.")
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

GH_DEFPROC
(pg_consume_input, "pg-consume-input", 1, 0, 0,
 (SCM conn),
 "Consume input from @var{conn}.  Return @code{#t} iff successful.")
{
#define FUNC_NAME s_pg_consume_input
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return gh_bool2scm (PQconsumeInput (dbconn));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_is_busy_p, "pg-is-busy?", 1, 0, 0,
 (SCM conn),
 "Return @code{#t} if there is data waiting for\n"
 "@code{pg-consume-input}, otherwise @code{#f}.")
{
#define FUNC_NAME s_pg_is_busy_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return gh_bool2scm (PQisBusy (dbconn));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_request_cancel, "pg-request-cancel", 1, 0, 0,
 (SCM conn),
 "Request a cancellation on @var{conn}.\n"
 "Return @code{#t} iff the cancel request was successfully\n"
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
#define FUNC_NAME s_pg_request_cancel
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return gh_bool2scm (PQrequestCancel (dbconn));
#undef FUNC_NAME
}

GH_DEFPROC
(pg_flush, "pg-flush", 1, 0, 0,
 (SCM conn),
 "Flush output for connection @var{conn}.\n"
 "Return 0 (zero) if successful (or if the send queue is empty),\n"
 "-1 if flushing failed for some reason, or 1 if not all data was\n"
 "sent (only possible for a non-blocking connection).")
{
#define FUNC_NAME s_pg_flush
  ASSERT_CONNECTION (1, conn);

  return gh_int2scm (PQflush (CONN_CONN (conn)));
#undef FUNC_NAME
}


/*
 * installation features
 */

#define SIMPLE_SYMBOL(s)                        \
  SCM_SYMBOL (pg_sym_ ## s, # s)

#define SYM(s)  (pg_sym_ ## s)

SIMPLE_SYMBOL (PQPROTOCOLVERSION);
SIMPLE_SYMBOL (PQRESULTERRORMESSAGE);
SIMPLE_SYMBOL (PQPASS);
SIMPLE_SYMBOL (PQBACKENDPID);
SIMPLE_SYMBOL (PQOIDVALUE);
SIMPLE_SYMBOL (PQBINARYTUPLES);
SIMPLE_SYMBOL (PQFMOD);
SIMPLE_SYMBOL (PQSETNONBLOCKING);
SIMPLE_SYMBOL (PQISNONBLOCKING);


/*
 * init
 */

static
void
init_module (void)
{
#define DEFSMOB(tagvar,name,m,f,p)                              \
  tagvar = scm_make_smob_type_mfpe (name, 0, m, f, p, NULL)

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
#undef DEFSMOB

#include "libpq.x"

#define KLIST(...)  GH_STONED (PCHAIN (__VA_ARGS__))

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
      pgrs[i] = GH_STONED (gh_symbol2scm (PQresStatus (i)));
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

  goodies
    = KLIST (SYM (PQPROTOCOLVERSION),
             SYM (PQRESULTERRORMESSAGE),
             SYM (PQPASS),
             SYM (PQBACKENDPID),
             SYM (PQOIDVALUE),
             SYM (PQBINARYTUPLES),
             SYM (PQFMOD),
             SYM (PQSETNONBLOCKING),
             SYM (PQISNONBLOCKING));

#undef KLIST
}

GH_MODULE_LINK_FUNC ("database postgres"
                     ,database_postgres
                     ,init_module)

/* libpq.c ends here */
