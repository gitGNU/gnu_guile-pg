/* libpostgres_lo.c

   Copyright (C) 1999 Ian Grant
   Copyright (C) 2002,2003,2004 Thien-Thi Nguyen

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

#define XCONN(x)      (xc_unbox (x)->dbconn)

#define LOB_READING 1
#define LOB_WRITING 2

#define LOB_BUFLEN 512

#define MAX_LOB_WRITE 7000

typedef struct lob_stream_tag {
   SCM conn; /* The connection on which the LOB fd is open */
   Oid oid;  /* The Oid of the LOB */
   int alod; /* A Large-Object Descriptor */
} lob_stream;


/* LOB_CONN takes a lob_stream pointer and returns the PGconn pointer for
   that stream. Because SCM_DEFER/ALLOW_INTS are used in xc_unbox, this
   macro cannot be used inside SCM_DEFER/ALLOW_INTS. FIXME: Is this still true?
*/

#define LOB_CONN(x) (XCONN ((x)->conn))

#define SCM_LOBPORTP(x) (SCM_TYP16 (x)==lob_ptype)

#define SCM_OPLOBPORTP(x) \
  (((0xffff | SCM_OPN) & (int) gh_car (x)) == (lob_ptype | SCM_OPN))

#define SCM_OPINLOBPORTP(x)                           \
  (((0xffff | SCM_OPN | SCM_RDNG) & (int) gh_car (x)) \
   == (lob_ptype | SCM_OPN | SCM_RDNG))

/* ttn hack */
#define TTN_COERCE_INT(x) ((int)(x))

long lob_ptype;

static SCM lob_mklobport (SCM conn, Oid oid, int alod,
                          long modes, const char *caller);

PG_DEFINE (lob_lo_creat, "pg-lo-creat", 2, 0, 0,
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

  SCM_ASSERT (xc_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (modes) && SCM_ROSTRINGP (modes),
              modes, SCM_ARG2, FUNC_NAME);
  ROZT_X (modes);

  mode_bits = scm_mode_bits (ROZT (modes));
  dbconn = XCONN (conn);

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (pg_modes == 0)
    scm_misc_error (FUNC_NAME, "Invalid mode specification: ~S",
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

PG_DEFINE (lob_lo_open, "pg-lo-open", 3, 0, 0,
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

  SCM_ASSERT (xc_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_INUMP (oid), oid, SCM_ARG2, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (modes) && SCM_ROSTRINGP (modes),
              modes, SCM_ARG3, FUNC_NAME);
  ROZT_X (modes);

  mode_bits = scm_mode_bits (ROZT (modes));
  dbconn = XCONN (conn);

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (pg_modes == 0)
    scm_misc_error (FUNC_NAME, "Invalid mode specification: ~S",
                    scm_listify (modes, SCM_UNDEFINED));
  pg_oid = gh_scm2int (oid);
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

static SCM
lob_mklobport (SCM conn, Oid oid, int alod, long modes, const char *caller)
{
  SCM port;
  lob_stream *lobp;
  scm_port *pt;
  const char *s_lob_mklobport = "lob_mklobport";

  lobp = (lob_stream *) scm_must_malloc (sizeof (lob_stream), "PG-LO-PORT");

  SCM_NEWCELL (port);

  SCM_DEFER_INTS;
  lobp->conn = conn;
  lobp->oid = oid;
  lobp->alod = alod;
  pt = scm_add_to_port_table (port);
  SCM_SETPTAB_ENTRY (port, pt);
  SCM_SETCAR (port, lob_ptype | modes);
  SCM_SETSTREAM (port, (SCM) lobp);

  pt->rw_random = 1;
  if (SCM_INPUT_PORT_P (port))
    {
      pt->read_buf = malloc (LOB_BUFLEN);
      if (pt->read_buf == NULL)
        scm_memory_error (s_lob_mklobport);
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
        scm_memory_error (s_lob_mklobport);
      pt->write_pos = pt->write_buf;
      pt->write_buf_size = LOB_BUFLEN;
    }
  else
    {
      pt->write_buf = pt->write_pos = &pt->shortbuf;
      pt->write_buf_size = 1;
    }
  pt->write_end = pt->write_buf + pt->write_buf_size;

  SCM_SETCAR (port, TTN_COERCE_INT (gh_car (port)) & ~SCM_BUF0);

  SCM_ALLOW_INTS;

  return port;
}

PG_DEFINE (lob_lo_unlink, "pg-lo-unlink", 2, 0, 0,
           (SCM conn, SCM oid),
           "Delete the large object identified by @var{oid}.\n"
           "Return @code{#t} if the object was successfully deleted,\n"
           "@code{#f} otherwise, in which case @code{pg-error-message}\n"
           "applied to @code{conn} should give an idea of what went wrong.")
{
#define FUNC_NAME s_lob_lo_unlink
  int ret;
  PGconn *dbconn;

  SCM_ASSERT (xc_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_INUMP (oid), oid, SCM_ARG2, FUNC_NAME);

  dbconn = XCONN (conn);

  SCM_DEFER_INTS;
  ret = lo_unlink (dbconn, gh_scm2int (oid));
  SCM_ALLOW_INTS;
  return (ret < 0
          ? SCM_BOOL_F
          : SCM_BOOL_T);
#undef FUNC_NAME
}

PG_DEFINE (lob_lo_get_connection, "pg-lo-get-connection", 1, 0, 0,
           (SCM port),
           "Return the connection associated with a given large object port.\n"
           "@var{port} must be a large object port returned from\n"
           "@code{pg-lo-creat} or @code{pg-lo-open}.")
{
#define FUNC_NAME s_lob_lo_get_connection
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPLOBPORTP (port),
              port, SCM_ARG1, FUNC_NAME);

  return ((lob_stream *)SCM_STREAM (port))->conn;
#undef FUNC_NAME
}

PG_DEFINE (lob_lo_get_oid, "pg-lo-get-oid", 1, 0, 0,
           (SCM port),
           "Return the integer identifier of the object to which a given\n"
           "port applies.  @var{port} must be a large object port returned\n"
           "from @code{pg-lo-creat} or @code{pg-lo-open}.")
{
#define FUNC_NAME s_lob_lo_get_oid
  SCM_ASSERT (SCM_NIMP (port) && SCM_LOBPORTP (port),
              port, SCM_ARG1, FUNC_NAME);
  return gh_int2scm (((lob_stream *)SCM_STREAM (port))->oid);
#undef FUNC_NAME
}

PG_DEFINE (lob_lo_tell, "pg-lo-tell", 1, 0, 0,
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
  SCM_ASSERT (SCM_NIMP (port)&&SCM_OPLOBPORTP (port),port,SCM_ARG1,FUNC_NAME);

  return scm_seek (port, SCM_INUM0, gh_int2scm (SEEK_CUR));
#undef FUNC_NAME
}

/* During lob_flush error, we decide whether to use scm_syserror ("normal"
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
  scm_port *pt = SCM_PTAB_ENTRY (port);
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  char *ptr = pt->write_buf;
  int init_size = pt->write_pos - pt->write_buf;
  int remaining = init_size;

  while (remaining > 0)
    {
      int count;
#ifdef DEBUG_TRACE_LO_WRITE
      fprintf (stderr, "lob_flush (): lo_write (%.*s, %d) ... ",
               remaining, ptr, remaining);
#endif
      SCM_DEFER_INTS;
      count = lo_write (conn, lobp->alod, ptr, remaining);
      SCM_ALLOW_INTS;
#ifdef DEBUG_TRACE_LO_WRITE
      fprintf (stderr, "returned %d\n", count);
#endif
      if (count < remaining)
        {
          /* error.  assume nothing was written this call, but
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
            scm_syserror ("lob_flush");
          else
#endif /* HAVE_LIBGUILE_TERMINATING || HAVE_LIBGUILE_TERMINATING */
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
        scm_misc_error ("lob_end_input", "Error seeking on lo port ~S",
                        scm_listify (port, SCM_UNDEFINED));
    }
  pt->rw_active = SCM_PORT_NEITHER;
}

static off_t lob_seek (SCM port, off_t offset, int whence);

PG_DEFINE (lob_lo_seek, "pg-lo-seek", 3, 0, 0,
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
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPLOBPORTP (port),
              port, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_INUMP (where), where, SCM_ARG2, FUNC_NAME);
  SCM_ASSERT (SCM_INUMP (whence), whence, SCM_ARG3, FUNC_NAME);

  lob_flush (port);

  return gh_int2scm (lob_seek (port, gh_scm2int (where), gh_scm2int (whence)));
#undef FUNC_NAME
}

/* fill a port's read-buffer with a single read.
   returns the first char and moves the read_pos pointer past it.
   or returns EOF if end of file.  */
static int
lob_fill_input (SCM port)
{
  scm_port *pt = SCM_PTAB_ENTRY (port);
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);

  int ret;

  if (pt->write_pos > pt->write_buf)
    lob_flush (port);

  SCM_DEFER_INTS;
  ret = lo_read (conn, lobp->alod, pt->read_buf, pt->read_buf_size);
  SCM_ALLOW_INTS;
#ifdef DEBUG_LO_READ
  fprintf (stderr, "lob_fill_input: lo_read (%d) returned %d.\n",
           pt->read_buf_size, ret);
#endif
  if (ret != pt->read_buf_size)
    {
      if (ret == 0)
        return EOF;
      else if (ret < 0)
        scm_misc_error ("lob_fill_buffer", "Error (~S) reading from lo port ~S",
                        scm_listify (gh_int2scm (ret), port, SCM_UNDEFINED));
    }
  pt->read_pos = pt->read_buf;
  pt->read_end = pt->read_buf + ret;
#ifdef DEBUG_LO_READ
  fprintf (stderr, "lob_fill_input: returning %c.\n", * (pt->read_buf));
#endif
  return * (pt->read_buf);
}

static void
lob_write (SCM port, const void *data, size_t size)
{
  scm_port *pt = SCM_PTAB_ENTRY (port);

  if (pt->write_buf == &pt->shortbuf)
    {
      /* "unbuffered" port.  */
      int fdes = SCM_FSTREAM (port)->fdes;

      if (write (fdes, data, size) == -1)
        scm_syserror ("fport_write");
    }
  else
    {
      const char *input = (char *) data;
      size_t remaining = size;

      while (remaining > 0)
        {
          int space = pt->write_end - pt->write_pos;
          int write_len = (remaining > space) ? space : remaining;

          memcpy (pt->write_pos, input, write_len);
          pt->write_pos += write_len;
          remaining -= write_len;
          input += write_len;
          if (write_len == space)
            lob_flush (port);
        }
      /* handle line buffering.  */
      if ((TTN_COERCE_INT (gh_car (port)) & SCM_BUFLINE)
          && memchr (data, '\n', size))
        lob_flush (port);
    }
}

static off_t
lob_seek (SCM port, off_t offset, int whence)
{
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  off_t ret;

  SCM_DEFER_INTS;
  ret = lo_lseek (conn, lobp->alod, offset, whence);
  SCM_ALLOW_INTS;
  if (ret == -1)
    scm_misc_error ("lob_seek", "Error (~S) seeking on lo port ~S",
                    scm_listify (gh_int2scm (ret), port, SCM_UNDEFINED));

  /* Adjust return value to account for guile port buffering.  */
  if (SEEK_CUR == whence)
    {
      scm_port *pt = SCM_PTAB_ENTRY (port);
      ret -= (pt->read_end - pt->read_pos);
    }

  return ret;
}

/* Check whether a port can supply input.  */
static int
lob_input_waiting_p (SCM port)
{
  return 1;
}

PG_DEFINE (lob_lo_read, "pg-lo-read", 3, 0, 0,
           (SCM siz, SCM num, SCM port),
           "Read @var{num} objects each of length @var{siz} from @var{port}.\n"
           "Return a string containing the data read from the port or\n"
           "@code{#f} if an error occurred.")
{
#define FUNC_NAME s_lob_lo_read
  scm_sizet n;
  SCM str;
  int len;
  int done = 0;

  SCM_ASSERT (SCM_INUMP (siz), siz, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_INUMP (num), num, SCM_ARG2, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPINLOBPORTP (port),
              port, SCM_ARG3, FUNC_NAME);

  len = gh_scm2int (siz) * gh_scm2int (num);
  str = scm_make_string (gh_int2scm (len), SCM_UNDEFINED);
  for (n = 0 ; n < gh_scm2int (num) && ! done; n++)
    {
      scm_sizet m;
      int c;
      for (m = 0; m < gh_scm2int (siz); m++)
        {
          c = scm_getc (port);
          if (c == EOF)
            {
              done = 1;
              break;
            }
          * (SCM_CHARS (str) + n * gh_scm2int (siz) + m) = c;
        }
    }
  if (n < 0)
    return SCM_BOOL_F;
  if (n < gh_scm2int (num))
    {
      SCM_DEFER_INTS; /* See comment re scm_vector_set_len in libguile/unif.c */
      scm_vector_set_length_x (str, gh_int2scm (n * gh_scm2int (siz)));
      SCM_ALLOW_INTS;
    }
  return str;
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

PG_DEFINE (lob_lo_import, "pg-lo-import", 2, 0, 0,
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

  SCM_ASSERT (xc_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (filename) && SCM_ROSTRINGP (filename),
              filename, SCM_ARG2, FUNC_NAME);
  ROZT_X (filename);

  dbconn = XCONN (conn);

  SCM_DEFER_INTS;
  ret = lo_import (dbconn, ROZT (filename));
  SCM_ALLOW_INTS;

  if (ret <= 0)
    return SCM_BOOL_F;

  return gh_int2scm (ret);
#undef FUNC_NAME
}

PG_DEFINE (lob_lo_export, "pg-lo-export", 3, 0, 0,
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

  SCM_ASSERT (xc_p (conn), conn, SCM_ARG1, FUNC_NAME);
  SCM_ASSERT (SCM_INUMP (oid), oid, SCM_ARG2, FUNC_NAME);
  SCM_ASSERT (SCM_NIMP (filename) && SCM_ROSTRINGP (filename), filename,
              SCM_ARG3, FUNC_NAME);
  ROZT_X (filename);
  dbconn = XCONN (conn);
  pg_oid = gh_scm2int (oid);

  SCM_DEFER_INTS;
  ret = lo_export (dbconn, pg_oid, ROZT (filename));
  SCM_ALLOW_INTS;

  if (ret != 1)
    return SCM_BOOL_F;

  return SCM_BOOL_T;
#undef FUNC_NAME
}

static int lob_printpt (SCM exp, SCM port, scm_print_state *pstate);

static int
lob_printpt (SCM exp, SCM port, scm_print_state *pstate)
{
  scm_puts ("#<PG-LO-PORT:", port);
  scm_print_port_mode (exp, port);
  if (SCM_OPENP (exp))
    {
      lob_stream *lobp = (lob_stream *) SCM_STREAM (exp);
      xc_t *sec = xc_unbox (lobp->conn);
      char *dbstr = PQdb (sec->dbconn);
      char *hoststr = PQhost (sec->dbconn);
      char *portstr = PQport (sec->dbconn);
      char *optionsstr = PQoptions (sec->dbconn);

      scm_intprint (lobp->alod, 10, port); scm_puts (":", port);
      scm_intprint (lobp->oid, 10, port); scm_puts (":", port);
      scm_puts ("#<PG-CONN:", port);
      scm_intprint (sec->count, 10, port); scm_putc (':', port);
      scm_puts (IFNULL (dbstr,"db?"), port); scm_putc (':', port);
      scm_puts (IFNULL (hoststr,"localhost"), port); scm_putc (':', port);
      scm_puts (IFNULL (portstr,"port?"), port); scm_putc (':', port);
      scm_puts (IFNULL (optionsstr,"options?"), port);
      scm_putc ('>', port);
    }
  scm_putc ('>', port);
  return 1;
}

void init_libpostgres_lo (void)
{
  long tc = scm_make_port_type ("pg-lo-port", lob_fill_input, lob_write);
  scm_set_port_free          (tc, lob_free);
  scm_set_port_mark          (tc, lob_mark);
  scm_set_port_print         (tc, lob_printpt);
  scm_set_port_flush         (tc, lob_flush);
  scm_set_port_end_input     (tc, lob_end_input);
  scm_set_port_close         (tc, lob_close);
  scm_set_port_seek          (tc, lob_seek);
  scm_set_port_truncate      (tc, NULL);
  scm_set_port_input_waiting (tc, lob_input_waiting_p);

  lob_ptype = tc;

#include "libpostgres_lo.x"
}

/* libpostgres_lo.c ends here */
