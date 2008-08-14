/* libpq.c

   Copyright (C) 2003,2004,2005,2006,2007,2008 Thien-Thi Nguyen
   Portions Copyright (C) 1999,2000 Ian Grant

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

#include <config.h>
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

/*
 * guile abstractions
 */

#define NOT_FALSEP(x)      (SCM_NFALSEP (x))
#define EXACTLY_FALSEP(x)  (SCM_FALSEP (x))
#define EXACTLY_TRUEP(x)   (gh_eq_p ((x), SCM_BOOL_T))

/* We fudge this. I wonder why only libguile sources can access this?  */
#define SCM_SYSCALL(line) line

/* Coerce a string that is to be used in contexts where the extracted C
   string is expected to be zero-terminated and is read-only.  We check
   this condition precisely instead of simply coercing all substrings,
   to avoid waste for those substrings that may in fact already satisfy
   the condition.  Callers should extract w/ ROZT.  */
#define ROZT_X(x)                                               \
  if (SCM_ROCHARS (x) [SCM_ROLENGTH (x)] != '\0')               \
    x = scm_makfromstr (SCM_ROCHARS (x), SCM_ROLENGTH (x), 0)

#define ROZT(x)  (SCM_ROCHARS (x))

/* For some versions of Guile, (make-string (ash 1 24)) => "".

   That is, `make-string' doesn't fail, but lengths past (1- (ash 1 24))
   overflow an internal limit and silently return an incorrect value.
   We hardcode this limit here for now.  */
#define MAX_NEWSTRING_LENGTH ((1 << 24) - 1)


/*
 * smob: connection
 */

static unsigned long int pg_conn_tag;

typedef struct
{
  SCM          notice;        /* port to send notices to */
  SCM          client;
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
  return ((xc_t *) SCM_SMOB_DATA (obj));
}

#define ASSERT_CONNECTION(n,arg)                        \
  SCM_ASSERT (xc_p (arg), arg, SCM_ARG ## n, FUNC_NAME)

static int
xc_display (SCM exp, SCM port, UNUSED scm_print_state *pstate)
{
  xc_t *xc = xc_unbox (exp);

  scm_puts ("#<PG-CONN:", port);
  if (! xc->dbconn)
    scm_putc ('-', port);
  else
    {
      char *dbstr = PQdb (xc->dbconn);
      char *hoststr = PQhost (xc->dbconn);
      char *portstr = PQport (xc->dbconn);
      char *optionsstr = PQoptions (xc->dbconn);

      /* A port without a host is misleading.  */
      if (! hoststr)
        portstr = NULL;

#define IFNULL(x,y) ((x) == NULL ? (y) : (x))

      scm_puts (IFNULL (dbstr, "?"), port); scm_putc (':', port);
      scm_puts (IFNULL (hoststr, ""), port); scm_putc (':', port);
      scm_puts (IFNULL (portstr, ""), port); scm_putc (':', port);
      scm_puts (IFNULL (optionsstr, ""), port);

#undef IFNULL
    }
  scm_puts (">", port);

  return 1;
}

static SCM
xc_mark (SCM obj)
{
  xc_t *xc = xc_unbox (obj);

  scm_gc_mark (xc->notice);
  return xc->client;
}

static scm_sizet
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
#define CONN_CLIENT(conn)   (xc_unbox (conn)->client)
#define CONN_FPTRACE(conn)  (xc_unbox (conn)->fptrace)

#define VALIDATE_CONNECTION_UNBOX_DBCONN(n,arg,cvar)            \
  do {                                                          \
    SCM_ASSERT (xc_p (arg), arg, SCM_ARG ## n, FUNC_NAME);      \
    cvar = xc_unbox (arg)->dbconn;                              \
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

#define LOB_CONN(x) (xc_unbox ((x)->conn)->dbconn)

#define LOBPORTP(x) (SCM_TYP16 (x) == lobp_tag)

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

  SCM_DEFER_INTS;
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
      pt->read_buf = malloc (LOB_BUFLEN);
      if (pt->read_buf == NULL)
        SCM_MEMORY_ERROR;
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
      pt->write_buf = malloc (LOB_BUFLEN);
      if (pt->write_buf == NULL)
        SCM_MEMORY_ERROR;
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

  SCM_ALLOW_INTS;

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
  SCM_ASSERT (SCM_NIMP (modes) && SCM_ROSTRINGP (modes),
              modes, SCM_ARG2, FUNC_NAME);
  ROZT_X (modes);

  mode_bits = scm_mode_bits (ROZT (modes));

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (pg_modes == 0)
    SCM_MISC_ERROR ("Invalid mode specification: ~S",
                    scm_listify (modes, SCM_UNDEFINED));
  SCM_DEFER_INTS;
  if ((oid = lo_creat (dbconn, INV_READ | INV_WRITE)) != 0)
    alod = lo_open (dbconn, oid, pg_modes);
  SCM_ALLOW_INTS;

  if (oid <= 0)
    return SCM_BOOL_F;

  if (alod < 0)
    {
      SCM_DEFER_INTS;
      (void) lo_unlink (dbconn, oid);
      SCM_ALLOW_INTS;
      return SCM_BOOL_F;
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
  SCM_ASSERT (SCM_NIMP (modes) && SCM_ROSTRINGP (modes),
              modes, SCM_ARG3, FUNC_NAME);
  ROZT_X (modes);

  mode_bits = scm_mode_bits (ROZT (modes));

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (pg_modes == 0)
    SCM_MISC_ERROR ("Invalid mode specification: ~S",
                    scm_listify (modes, SCM_UNDEFINED));
  SCM_DEFER_INTS;
  alod = lo_open (dbconn, pg_oid, pg_modes);
  SCM_ALLOW_INTS;

  if (alod < 0)
    return SCM_BOOL_F;

  if (strchr (ROZT (modes), 'a'))
    {
      SCM_DEFER_INTS;
      if (lo_lseek (dbconn, alod, 0, SEEK_END) < 0)
        {
          (void) lo_close (dbconn, alod);
          SCM_ALLOW_INTS;
          return SCM_BOOL_F;
        }
      SCM_ALLOW_INTS;
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

  SCM_DEFER_INTS;
  ret = lo_unlink (dbconn, pg_oid);
  SCM_ALLOW_INTS;
  return (ret < 0
          ? SCM_BOOL_F
          : SCM_BOOL_T);
#undef FUNC_NAME
}

GH_DEFPROC
(lob_lo_get_connection, "pg-lo-get-connection", 1, 0, 0,
 (SCM port),
 "Return the connection associated with a given large object port.\n"
 "@var{port} must be a large object port returned from\n"
 "@code{pg-lo-creat} or @code{pg-lo-open}.")
{
#define FUNC_NAME s_lob_lo_get_connection
  SCM_ASSERT (SCM_NIMP (port) && OPLOBPORTP (port),
              port, SCM_ARG1, FUNC_NAME);

  return ((lob_stream *)SCM_STREAM (port))->conn;
#undef FUNC_NAME
}

GH_DEFPROC
(lob_lo_get_oid, "pg-lo-get-oid", 1, 0, 0,
 (SCM port),
 "Return the integer identifier of the object to which a given\n"
 "port applies.  @var{port} must be a large object port returned\n"
 "from @code{pg-lo-creat} or @code{pg-lo-open}.")
{
#define FUNC_NAME s_lob_lo_get_oid
  SCM_ASSERT (SCM_NIMP (port) && LOBPORTP (port),
              port, SCM_ARG1, FUNC_NAME);
  return gh_int2scm (((lob_stream *)SCM_STREAM (port))->oid);
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
  SCM_ASSERT (SCM_NIMP (port)&&OPLOBPORTP (port),port,SCM_ARG1,FUNC_NAME);

  return scm_seek (port, SCM_INUM0, gh_int2scm (SEEK_CUR));
#undef FUNC_NAME
}

/* During lob_flush error, we decide whether to use SCM_SYSERROR ("normal"
   error mechanism) or to write directly to stderr, depending on libguile's
   variable: scm_terminating.  If it's not available in some form (see
   configure.in comments), we arrange to unconditionally write to stderr
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
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  unsigned char *ptr = pt->write_buf;
  int init_size = pt->write_pos - pt->write_buf;
  int remaining = init_size;

  while (remaining > 0)
    {
      int count;
      SCM_DEFER_INTS;
      count = lo_write (conn, lobp->alod, (char *) ptr, remaining);
      SCM_ALLOW_INTS;
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
            SCM_SYSERROR;
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
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  int ret;

  offset += pt->read_end - pt->read_pos;

  if (offset > 0)
    {
      pt->read_pos = pt->read_end;
      SCM_DEFER_INTS;
      ret = lo_lseek (conn, lobp->alod, -offset, SEEK_CUR);
      SCM_ALLOW_INTS;
      if (ret == -1)
        SCM_MISC_ERROR ("Error seeking on lo port ~S",
                        scm_listify (port, SCM_UNDEFINED));
    }
  pt->rw_active = SCM_PORT_NEITHER;
}

static off_t
lob_seek (SCM port, off_t offset, int whence)
{
  const char *FUNC_NAME = "lob_seek";
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  off_t ret;

  SCM_DEFER_INTS;
  ret = lo_lseek (conn, lobp->alod, offset, whence);
  SCM_ALLOW_INTS;
  if (ret == -1)
    SCM_MISC_ERROR ("Error (~S) seeking on lo port ~S",
                    scm_listify (gh_int2scm (ret), port, SCM_UNDEFINED));

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
  SCM_ASSERT (SCM_NIMP (port) && OPLOBPORTP (port),
              port, SCM_ARG1, FUNC_NAME);
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
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);

  int ret;

  if (pt->write_pos > pt->write_buf)
    lob_flush (port);

  SCM_DEFER_INTS;
  ret = lo_read (conn, lobp->alod, (char *) pt->read_buf, pt->read_buf_size);
  SCM_ALLOW_INTS;
  if (ret != pt->read_buf_size)
    {
      if (ret == 0)
        return EOF;
      else if (ret < 0)
        SCM_MISC_ERROR ("Error (~S) reading from lo port ~S",
                        scm_listify (gh_int2scm (ret), port, SCM_UNDEFINED));
    }
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
      int fdes = SCM_FSTREAM (port)->fdes;

      if (write (fdes, data, size) == -1)
        SCM_SYSERROR;
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
  SCM_ASSERT (SCM_NIMP (port) && OPINLOBPORTP (port),
              port, SCM_ARG3, FUNC_NAME);

  if (0 > (len = csiz * cnum)
      || (MAX_NEWSTRING_LENGTH < len)
      || (! (wp = stage = (char *) malloc (1 + len))))
    return SCM_BOOL_F;
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
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *dbconn = LOB_CONN (lobp);
  int ret;

  lob_flush (port);
  SCM_DEFER_INTS;
  ret = lo_close (dbconn, lobp->alod);
  SCM_ALLOW_INTS;

  if (pt->read_buf != &pt->shortbuf)
    scm_must_free (pt->read_buf);
  if (pt->write_buf != &pt->shortbuf)
    scm_must_free (pt->write_buf);

  if (ret != 0)
    return EOF;

  return 0;
}

static SCM
lob_mark (SCM port)
{
  lob_stream *lobp;

  if (SCM_OPENP (port))
    {
      lobp = (lob_stream *) SCM_STREAM (port);
      return lobp->conn;
    }
  return SCM_BOOL_F;
}

static scm_sizet
lob_free (SCM port)
{
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
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
  int ret;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_ASSERT (SCM_NIMP (filename) && SCM_ROSTRINGP (filename),
              filename, SCM_ARG2, FUNC_NAME);
  ROZT_X (filename);

  SCM_DEFER_INTS;
  ret = lo_import (dbconn, ROZT (filename));
  SCM_ALLOW_INTS;

  if (ret <= 0)
    return SCM_BOOL_F;

  return gh_int2scm (ret);
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
  SCM_ASSERT (SCM_NIMP (filename) && SCM_ROSTRINGP (filename), filename,
              SCM_ARG3, FUNC_NAME);
  ROZT_X (filename);

  SCM_DEFER_INTS;
  ret = lo_export (dbconn, pg_oid, ROZT (filename));
  SCM_ALLOW_INTS;

  if (ret != 1)
    return SCM_BOOL_F;

  return SCM_BOOL_T;
#undef FUNC_NAME
}

static int
lob_printpt (SCM exp, SCM port, scm_print_state *pstate)
{
  scm_puts ("#<PG-LO-PORT:", port);
  scm_print_port_mode (exp, port);
  if (SCM_OPENP (exp))
    {
      lob_stream *lobp = (lob_stream *) SCM_STREAM (exp);

      scm_intprint (lobp->alod, 10, port); scm_puts (":", port);
      scm_intprint (lobp->oid, 10, port); scm_puts (":", port);
      xc_display (lobp->conn, port, pstate);
    }
  scm_putc ('>', port);
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
  return ((PGresult*) SCM_SMOB_DATA (obj));
}

#define VALIDATE_RESULT_UNBOX(pos,arg,cvar)                     \
  do {                                                          \
    SCM_ASSERT (res_p (arg), arg, SCM_ARG ## pos, FUNC_NAME);   \
    cvar = res_unbox (arg);                                     \
  } while (0)

static SCM
res_box (PGresult *res)
{
  if (res)
    SCM_RETURN_NEWSMOB (pg_result_tag, res);
  else
    return SCM_BOOL_F;
}

static int
res_display (SCM exp, SCM port, UNUSED scm_print_state *pstate)
{
  PGresult *res = res_unbox (exp);
  ExecStatusType status;
  int ntuples = 0;
  int nfields = 0;

  SCM_DEFER_INTS;
  status = PQresultStatus (res);
  if (status == PGRES_TUPLES_OK)
    {
      ntuples = PQntuples (res);
      nfields = PQnfields (res);
    }
  SCM_ALLOW_INTS;

  if (PGRES_FATAL_ERROR < status)
    status = PGRES_FATAL_ERROR;

  scm_puts ("#<PG-RESULT:", port);
  scm_puts (6 + PQresStatus (status), port); scm_putc (':', port);
  scm_intprint (ntuples, 10, port); scm_putc (':', port);
  scm_intprint (nfields, 10, port);
  scm_putc ('>', port);

  return 1;
}

static scm_sizet
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
  ps->len = len = SCM_LENGTH (v);
  for (i = 0; i < len; i++)
    {
      elem = VREF (v, i);
      if (! gh_string_p (elem))
        SCM_MISC_ERROR ("bad parameter-vector element: ~S",
                        SCM_LIST1 (elem));
    }
  ps->types = NULL;
  ps->values = (const char **) malloc (len * sizeof (char *));
  if (! ps->values)
    SCM_MISC_ERROR ("memory exhausted", SCM_EOL);
  for (i = 0; i < len; i++)
    ps->values[i] = ROZT (VREF (v, i));
  ps->lengths = NULL;
  ps->formats = NULL;
}

static void
drop_paramspecs (struct paramspecs *ps)
{
  if (ps->types)   free (ps->types);
  if (ps->values)  free (ps->values);
  if (ps->lengths) free (ps->lengths);
  if (ps->formats) free (ps->formats);
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
    return SCM_BOOL_F;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  v = PQprotocolVersion (dbconn);

  return (0 == v ? SCM_BOOL_F : gh_int2scm (v));
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
    PQconninfoFree (head);

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
      PQfinish (dbconn);
    }
  SCM_ALLOW_INTS;

  if (connstat == CONNECTION_BAD)
    SCM_MISC_ERROR ("~A", SCM_LIST1 (pgerrormsg));

  xc = ((xc_t *) scm_must_malloc (sizeof (xc_t), "PG-CONN"));

  xc->dbconn = dbconn;
  xc->client = SCM_BOOL_F;
  xc->notice = SCM_BOOL_T;
  xc->fptrace = (FILE *) NULL;

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
  return xc_p (obj) ? SCM_BOOL_T : SCM_BOOL_F;
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

  return SCM_UNSPECIFIED;
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
  SCM_DEFER_INTS;
  PQreset (dbconn);
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_client_data, "pg-get-client-data", 1, 0, 0,
 (SCM conn),
 "Return the the client data associated with @var{conn}.")
{
#define FUNC_NAME s_pg_get_client_data
  ASSERT_CONNECTION (1, conn);
  return CONN_CLIENT (conn);
#undef FUNC_NAME
}

GH_DEFPROC
(pg_set_client_data, "pg-set-client-data!", 2, 0, 0,
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
  SCM_ASSERT (SCM_NIMP (string) && SCM_ROSTRINGP (string),
              string, SCM_ARG2, FUNC_NAME);
  ROZT_X (string);

  ilen = SCM_ROLENGTH (string);
  if (! (answer = malloc (1 + 2 * ilen)))
    SCM_SYSERROR;

  olen = PQescapeStringConn (dbconn, answer, ROZT (string), ilen, &errcode);
  if (errcode)
    {
      if (answer)
        free (answer);
      return SCM_BOOL_F;
    }
  rv = gh_str02scm (answer);
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
  SCM_ASSERT (SCM_NIMP (bytea) && SCM_ROSTRINGP (bytea),
              bytea, SCM_ARG2, FUNC_NAME);

  ilen = SCM_ROLENGTH (bytea);
  if (! (answer = PQescapeByteaConn (dbconn, SCM_ROUCHARS (bytea),
                                     ilen, &olen)))
    return SCM_BOOL_F;
  rv = gh_str2scm ((char *) answer, olen ? olen - 1 : 0);
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

  SCM_ASSERT (SCM_NIMP (bytea) && SCM_ROSTRINGP (bytea),
              bytea, SCM_ARG1, FUNC_NAME);

  if (! (answer = PQunescapeBytea (SCM_ROUCHARS (bytea), &olen)))
    return SCM_BOOL_F;
  rv = gh_str2scm ((char *) answer, olen);
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
  SCM_ASSERT (SCM_NIMP (statement) && SCM_ROSTRINGP (statement),
              statement, SCM_ARG2, FUNC_NAME);
  ROZT_X (statement);

  SCM_DEFER_INTS;
  result = PQexec (dbconn, ROZT (statement));

  z = res_box (result);
  SCM_ALLOW_INTS;
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
  SCM_DEFER_INTS;
  result = PQexecParams (dbconn, ROZT (statement), ps.len,
                         ps.types, ps.values, ps.lengths, ps.formats,
                         RESFMT_TEXT);
  z = res_box (result);
  SCM_ALLOW_INTS;
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
  SCM_DEFER_INTS;
  result = PQexecPrepared (dbconn, ROZT (stname), ps.len,
                           ps.values, ps.lengths, ps.formats,
                           RESFMT_TEXT);
  z = res_box (result);
  SCM_ALLOW_INTS;
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
  return res_p (obj) ? SCM_BOOL_T : SCM_BOOL_F;
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
  SCM rv = SCM_BOOL_F;
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
  return rv;
#undef CHKFC

 gotfc:
  if ((s = PQresultErrorField (res, fc)))
    {
      rv = gh_str02scm (s);
      switch (fc)
        {
        case PG_DIAG_STATEMENT_POSITION:
        case PG_DIAG_SOURCE_LINE:
          rv = scm_string_to_number (rv, SCM_MAKINUM (10));
          break;
        case PG_DIAG_SOURCE_FUNCTION:
          rv = gh_symbol2scm (ROZT (rv));
          break;
        default:
          /* Do nothing; leave result as a string.  */
          ;
        }
    }

  return rv;
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
    SCM_DEFER_INTS;
    msg = PQerrorMessage (dbconn);
    rv = strip_newlines (msg);
    SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  rv = PQdb (dbconn);
  SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  rv = PQuser (dbconn);
  SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  rv = PQpass (dbconn);
  SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  rv = PQhost (dbconn);
  SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  rv = PQport (dbconn);
  SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  rv = PQtty (dbconn);
  SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  rv = PQoptions (dbconn);
  SCM_ALLOW_INTS;

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
  SCM_DEFER_INTS;
  pid = PQbackendPID (dbconn);
  SCM_ALLOW_INTS;

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
 "Return the status (a string) of a parameter for @var{conn}.\n"
 "@var{parm} is a keyword, such as @code{#:client_encoding}.")
{
#define FUNC_NAME s_pg_parameter_status
  PGconn *dbconn;
  const char *cstatus = NULL;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  SCM_VALIDATE_KEYWORD (2, parm);

  parm = SCM_KEYWORDSYM (parm);
  ROZT_X (parm);
  /* Offset by one to skip the symbol name's initial hyphen.  */
  cstatus = PQparameterStatus (dbconn, 1 + ROZT (parm));

  return (cstatus ? gh_str02scm (cstatus) : SCM_BOOL_F);
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

  SCM_DEFER_INTS;
  result_status = PQresultStatus (res);
  SCM_ALLOW_INTS;

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

  SCM_DEFER_INTS;
  ntuples = PQntuples (res);
  SCM_ALLOW_INTS;

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

  SCM_DEFER_INTS;
  rv = gh_int2scm (PQnfields (res));
  SCM_ALLOW_INTS;

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

  SCM_DEFER_INTS;
  cmdtuples = PQcmdTuples (res);
  SCM_ALLOW_INTS;

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

  SCM_DEFER_INTS;
  oid_value = PQoidValue (res);
  SCM_ALLOW_INTS;

  if (oid_value == InvalidOid)
    return SCM_BOOL_F;

  return gh_int2scm (oid_value);
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
  SCM_ASSERT (SCM_NIMP (fname) && SCM_ROSTRINGP (fname), fname,
              SCM_ARG2, FUNC_NAME);
  ROZT_X (fname);

  SCM_DEFER_INTS;
  fnum = PQfnumber (res, ROZT (fname));
  SCM_ALLOW_INTS;

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

GH_DEFPROC
(pg_getvalue, "pg-getvalue", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 "Return a string containing the value of the attribute\n"
 "@var{sfield}, tuple @var{stuple} of @var{result}.  It is\n"
 "up to the caller to convert this to the required type.")
{
#define FUNC_NAME s_pg_getvalue
  PGresult *res;
  int maxtuple, tuple;
  int maxfield, field;
  const char *val;
  int isbinary, veclen = 0;
  SCM srv;

  VALIDATE_RESULT_UNBOX (1, result, res);
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
  if ((isbinary = PQbinaryTuples (res)) != 0)
    veclen = PQgetlength (res, tuple, field);
  SCM_ALLOW_INTS;

  if (isbinary)
    srv = gh_str2scm (val, veclen);
  else
    srv = gh_str02scm (val);

  return srv;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_getlength, "pg-getlength", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 "The size of the datum in bytes.")
{
#define FUNC_NAME s_pg_getlength
  PGresult *res;
  int maxtuple, tuple;
  int maxfield, field;
  int len;
  SCM ret;

  VALIDATE_RESULT_UNBOX (1, result, res);
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

GH_DEFPROC
(pg_getisnull, "pg-getisnull", 3, 0, 0,
 (SCM result, SCM stuple, SCM sfield),
 "Return @code{#t} if the attribute is @code{NULL},\n"
 "@code{#f} otherwise.")
{
#define FUNC_NAME s_pg_getisnull
  PGresult *res;
  int maxtuple, tuple;
  int maxfield, field;
  SCM rv;

  VALIDATE_RESULT_UNBOX (1, result, res);
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

  SCM_DEFER_INTS;
  if (PQbinaryTuples (res))
    rv = SCM_BOOL_T;
  else
    rv = SCM_BOOL_F;
  SCM_ALLOW_INTS;

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
  if (errmsg != SCM_UNDEFINED)
    SCM_VALIDATE_STRING (2, errmsg);

  if (errmsg != SCM_UNDEFINED)
    {
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
  if (SCM_OUTPORTP (port))
    pwritep = 1;
  else
    {
      if (gh_pair_p (port))
        swritep = 1;
      else
        SCM_WTA (SCM_ARG2, port);
    }

  SCM_DEFER_INTS;
  rv = PQgetCopyData (dbconn, &newbuf, SCM_NFALSEP (asyncp));
  if (0 < rv)
    {
      SCM s = gh_str2scm (newbuf, rv);
      if (pwritep)
        scm_display (s, port);
      if (swritep)
        scm_set_car_x (port, s);
    }
  SCM_ALLOW_INTS;
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
  SCM_ASSERT (SCM_STRINGP (buf), buf, SCM_ARG2, FUNC_NAME);

  if (tickle != SCM_UNDEFINED && NOT_FALSEP (tickle))
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
  SCM_ASSERT (SCM_NIMP (str)&&SCM_ROSTRINGP (str), str, SCM_ARG2, FUNC_NAME);
  SCM_DEFER_INTS;
  status = PQputnbytes (dbconn, SCM_ROCHARS (str), SCM_ROLENGTH (str));
  SCM_ALLOW_INTS;
  return (0 == status ? SCM_BOOL_T : SCM_BOOL_F);
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
  SCM_DEFER_INTS;
  ret = PQendcopy (dbconn);
  SCM_ALLOW_INTS;

  return (0 == ret ? SCM_BOOL_T : SCM_BOOL_F);
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
      SCM_MISC_ERROR ("Invalid verbosity: ~A",
                      SCM_LIST1 (verbosity));

    switch (PQsetErrorVerbosity (dbconn, now))
      {
      case PQERRORS_TERSE:   return KWD (terse);
      case PQERRORS_DEFAULT: return KWD (default);
      case PQERRORS_VERBOSE: return KWD (verbose);
      default:               return SCM_BOOL_F; /* TODO: abort.  */
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
    SCM_SYSERROR;
  SCM_SYSCALL (fpout = fdopen (fd, "w"));
  if (fpout == NULL)
    SCM_SYSERROR;

  SCM_DEFER_INTS;
  PQtrace (dbconn, fpout);
  CONN_FPTRACE (conn) = fpout;
  SCM_ALLOW_INTS;

  return SCM_UNSPECIFIED;
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
    return SCM_UNSPECIFIED;

  SCM_DEFER_INTS;
  PQuntrace (dbconn);
  SCM_SYSCALL (ret = fclose (CONN_FPTRACE (conn)));
  CONN_FPTRACE (conn) = (FILE *) NULL;
  SCM_ALLOW_INTS;
  if (ret)
    SCM_SYSERROR;

  return SCM_UNSPECIFIED;
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
  return ((PQprintOpt *) SCM_SMOB_DATA (obj));
}

static scm_sizet
sepo_free (SCM obj)
{
  PQprintOpt *po = sepo_unbox (obj);
  scm_sizet size = 0;

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

static int
sepo_display (UNUSED SCM sepo, SCM port, UNUSED scm_print_state *pstate)
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
  SCM check, substnames = SCM_BOOL_F, flags = SCM_EOL, keys = SCM_EOL;

  SCM_ASSERT (SCM_NULLP (spec) || SCM_CONSP (spec),
              spec, SCM_ARG1, FUNC_NAME);

  /* Hairy validation/collection: symbols in `flags', pairs in `keys'.  */
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

#define _FLAG_CHECK(m)                                  \
  (NOT_FALSEP (scm_memq (pg_sym_no_ ## m, flags))       \
   ? 0 : (NOT_FALSEP (scm_memq (pg_sym_ ## m, flags))   \
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
  options = ((options == SCM_UNDEFINED)
             ? pg_make_print_options (SCM_EOL)
             : options);
  SCM_ASSERT (sepo_p (options), options, SCM_ARG2, FUNC_NAME);

  fd = (SCM_OPFPORTP (curout = scm_current_output_port ())
        ? gh_scm2int (scm_fileno (curout))
        : -1);

  if (0 > fd)
    fout = tmpfile ();
  else
    {
      scm_force_output (curout);
      if (fileno (stdout) == fd)
        fout = stdout;
      else
        {
          SCM_SYSCALL (fd = dup (fd));
          if (0 > fd)
            SCM_SYSERROR;
          SCM_SYSCALL (fout = fdopen (fd, "w"));
        }
    }

  if (! fout)
    SCM_SYSERROR;
  PQprint (fout, res, sepo_unbox (options));

  if (0 > fd)
    {
      char buf[BUF_LEN];
      int howmuch = 0;

      buf[BUF_LEN - 1] = '\0';          /* elephant */
      fseek (fout, 0, SEEK_SET);

      while (BUF_LEN - 1 == (howmuch = fread (buf, 1, BUF_LEN - 1, fout)))
        scm_display (gh_str02scm (buf), curout);
      if (feof (fout))
        {
          buf[howmuch] = '\0';
          scm_display (gh_str02scm (buf), curout);
        }
    }

  if (stdout != fout)
    fclose (fout);

  return SCM_UNSPECIFIED;
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
  SCM rv = SCM_BOOL_F;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  if (tickle != SCM_UNDEFINED && NOT_FALSEP (tickle))
    /* We don't care if there was an error consuming input; caller can use
       `pg_error_message' to find out afterwards, or simply avoid tickling in
       the first place.  */
    PQconsumeInput (dbconn);
  n = PQnotifies (dbconn);
  if (n)
    {
      rv = gh_str02scm (n->relname);
      rv = gh_cons (rv, gh_int2scm (n->be_pid));
      PQfreemem (n);
    }
  return rv;
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

  return (0 == PQsetnonblocking (dbconn, ! EXACTLY_FALSEP (mode))
          ? SCM_BOOL_T
          : SCM_BOOL_F);
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

  return (PQisnonblocking (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
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
  SCM_ASSERT (SCM_STRINGP (query), query, SCM_ARG2, FUNC_NAME);
  ROZT_X (query);

  return (PQsendQuery (dbconn, ROZT (query))
          ? SCM_BOOL_T
          : SCM_BOOL_F);
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
  SCM_DEFER_INTS;
  result = PQsendQueryParams (dbconn, ROZT (query), ps.len,
                              ps.types, ps.values, ps.lengths, ps.formats,
                              RESFMT_TEXT);
  SCM_ALLOW_INTS;
  drop_paramspecs (&ps);
  return result ? SCM_BOOL_T : SCM_BOOL_F;
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
  SCM_DEFER_INTS;
  result = PQsendQueryPrepared (dbconn, ROZT (stname), ps.len,
                                ps.values, ps.lengths, ps.formats,
                                RESFMT_TEXT);
  SCM_ALLOW_INTS;
  drop_paramspecs (&ps);
  return result ? SCM_BOOL_T : SCM_BOOL_F;
#undef FUNC_NAME
}

GH_DEFPROC
(pg_get_result, "pg-get-result", 1, 0, 0,
 (SCM conn),
 "Return a result from @var{conn}, or @code{#f}.")
{
#define FUNC_NAME s_pg_send_query
  PGconn *dbconn;
  PGresult *result;
  SCM z;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);

  SCM_DEFER_INTS;
  result = PQgetResult (dbconn);
  z = res_box (result);
  SCM_ALLOW_INTS;
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
  return (PQconsumeInput (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
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
  return (PQisBusy (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
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
#define FUNC_NAME s_pg_is_busy_p
  PGconn *dbconn;

  VALIDATE_CONNECTION_UNBOX_DBCONN (1, conn, dbconn);
  return (PQrequestCancel (dbconn)
          ? SCM_BOOL_T
          : SCM_BOOL_F);
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

  return gh_int2scm (PQflush (xc_unbox (conn)->dbconn));
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
  pg_conn_tag = scm_make_smob_type ("PG-CONN", 0);
  scm_set_smob_mark (pg_conn_tag, xc_mark);
  scm_set_smob_free (pg_conn_tag, xc_free);
  scm_set_smob_print (pg_conn_tag, xc_display);

  pg_result_tag = scm_make_smob_type ("PG-RESULT", 0);
  scm_set_smob_free (pg_result_tag, res_free);
  scm_set_smob_print (pg_result_tag, res_display);

  sepo_type_tag = scm_make_smob_type ("PG-PRINT-OPTION", 0);
  scm_set_smob_free (sepo_type_tag, sepo_free);
  scm_set_smob_print (sepo_type_tag, sepo_display);

#include "libpq.x"

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
    unsigned int i;
    for (i = 0; i < sizeof (pgrs) / sizeof (SCM); i++)
      pgrs[i] = scm_protect_object
        (gh_car (scm_sysintern0 (PQresStatus (i))));
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

  goodies = SCM_EOL;

#define PUSH(x)  goodies = gh_cons (SYM (x), goodies)

  PUSH (PQPROTOCOLVERSION);
  PUSH (PQRESULTERRORMESSAGE);
  PUSH (PQPASS);
  PUSH (PQBACKENDPID);
  PUSH (PQOIDVALUE);
  PUSH (PQBINARYTUPLES);
  PUSH (PQFMOD);
  PUSH (PQSETNONBLOCKING);
  PUSH (PQISNONBLOCKING);

  goodies = scm_protect_object (goodies);
}

GH_MODULE_LINK_FUNC ("database postgres"
                     ,database_postgres
                     ,init_module)

/* libpq.c ends here */
