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

#define LOB_READING 1
#define LOB_WRITING 2

#define LOB_BUFLEN 512

#define MAX_LOB_WRITE 7000

typedef struct lob_stream_tag {
   SCM conn; /* The connection on which the LOB fd is open */
   Oid oid;  /* The Oid of the LOB */
   int fd;   /* The file-descriptor for an open large object */
} lob_stream;


/* LOB_CONN takes a lob_stream pointer and returns the PGconn pointer for
   that stream. Because SCM_DEFER/ALLOW_INTS are used in sec_unbox, this
   macro cannot be used inside SCM_DEFER/ALLOW_INTS. FIXME: Is this still true?
*/

#define LOB_CONN(x) (sec_unbox ((x)->conn)->dbconn)

#define SCM_LOBPORTP(x) (SCM_TYP16 (x)==lob_ptype)
#define SCM_OPLOBPORTP(x) (((0xffff | SCM_OPN) & (int)SCM_CAR (x))== (lob_ptype | SCM_OPN))
#define SCM_OPINLOBPORTP(x) (((0xffff | SCM_OPN | SCM_RDNG) & (int)SCM_CAR (x))== (lob_ptype | SCM_OPN | SCM_RDNG))
#define SCM_OPOUTLOBPORTP(x) (((0xffff | SCM_OPN | SCM_WRTNG) & (int)SCM_CAR (x))== (lob_ptype | SCM_OPN | SCM_WRTNG))

/* ttn hack */
#define TTN_COERCE_INT(x) ((int)(x))

long lob_ptype;

static SCM lob_mklobport (SCM conn, Oid oid, int fd, long modes, const char *caller);

SCM_PROC (s_lob_lo_creat, "pg-lo-creat", 2, 0, 0, lob_lo_creat);

static SCM
lob_lo_creat (SCM conn, SCM modes)
{
  long mode_bits;
  PGconn *dbconn;
  int fd = 0;
  Oid oid;
  int pg_modes = 0;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, s_lob_lo_creat);
  SCM_ASSERT (SCM_NIMP (modes) && SCM_ROSTRINGP (modes),
              modes, SCM_ARG2, s_lob_lo_creat);

  if (SCM_SUBSTRP (modes)) /* Why do we do this? I don't know. */
    modes = scm_makfromstr (SCM_ROCHARS (modes), SCM_ROLENGTH (modes), 0);

  mode_bits = scm_mode_bits (SCM_ROCHARS (modes));
  dbconn = sec_unbox (conn)->dbconn;

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (pg_modes == 0)
    scm_misc_error (s_lob_lo_creat, "Invalid mode specification %s",
                    scm_listify (modes, SCM_UNDEFINED));
  SCM_DEFER_INTS;
  if ((oid = lo_creat (dbconn, INV_READ | INV_WRITE)) != 0)
    fd = lo_open (dbconn, oid, pg_modes);
  SCM_ALLOW_INTS;

  if (oid <= 0)
    return SCM_BOOL_F;

  if (fd < 0) {
    SCM_DEFER_INTS;
    (void) lo_unlink (dbconn, oid);
    SCM_ALLOW_INTS;
    return SCM_BOOL_F;
  }
  return lob_mklobport (conn, oid, fd, mode_bits, s_lob_lo_creat);
}

SCM_PROC (s_lob_lo_open, "pg-lo-open", 3, 0, 0, lob_lo_open);

static SCM
lob_lo_open (SCM conn, SCM oid, SCM modes)
{
  long mode_bits;
  PGconn *dbconn;
  int fd;
  Oid pg_oid;
  int pg_modes = 0;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, s_lob_lo_open);
  SCM_ASSERT (SCM_INUMP (oid), oid, SCM_ARG2, s_lob_lo_open);
  SCM_ASSERT (SCM_NIMP (modes) && SCM_ROSTRINGP (modes),
              modes, SCM_ARG3, s_lob_lo_open);

  if (SCM_SUBSTRP (modes))
    modes = scm_makfromstr (SCM_ROCHARS (modes), SCM_ROLENGTH (modes), 0);

  mode_bits = scm_mode_bits (SCM_ROCHARS (modes));
  dbconn = sec_unbox (conn)->dbconn;

  if (mode_bits & SCM_RDNG)
    pg_modes |= INV_READ;
  if (mode_bits & SCM_WRTNG)
    pg_modes |= INV_WRITE;

  if (pg_modes == 0)
    scm_misc_error (s_lob_lo_open, "Invalid mode specification %s",
                    scm_listify (modes, SCM_UNDEFINED));
  pg_oid = SCM_INUM (oid);
  SCM_DEFER_INTS;
  fd = lo_open (dbconn, pg_oid, pg_modes);
  SCM_ALLOW_INTS;

  if (fd < 0)
    return SCM_BOOL_F;

  if (strchr (SCM_ROCHARS (modes), 'a')) {
    SCM_DEFER_INTS;
    if (lo_lseek (dbconn, fd, 0, SEEK_END) < 0) {
      (void) lo_close (dbconn, fd);
      SCM_ALLOW_INTS;
      return SCM_BOOL_F;
    }
    SCM_ALLOW_INTS;
  }
  return lob_mklobport (conn, pg_oid, fd, mode_bits, s_lob_lo_open);
}

static SCM
lob_mklobport (SCM conn, Oid oid, int fd, long modes, const char *caller)
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
  lobp->fd = fd;
  pt = scm_add_to_port_table (port);
  SCM_SETPTAB_ENTRY (port, pt);
  SCM_SETCAR (port, lob_ptype | modes);
  SCM_SETSTREAM (port, (SCM) lobp);

  pt->rw_random = 1;
  if (SCM_INPUT_PORT_P (port)) {
    pt->read_buf = malloc (LOB_BUFLEN);
    if (pt->read_buf == NULL)
      scm_memory_error (s_lob_mklobport);
    pt->read_pos = pt->read_end = pt->read_buf;
    pt->read_buf_size = LOB_BUFLEN;
  } else {
    pt->read_buf = ((unsigned char *) pt->read_pos) = pt->read_end = &pt->shortbuf;
    pt->read_buf_size = 1;
  }
  if (SCM_OUTPUT_PORT_P (port)) {
    pt->write_buf = malloc (LOB_BUFLEN);
    if (pt->write_buf == NULL)
      scm_memory_error (s_lob_mklobport);
    pt->write_pos = pt->write_buf;
    pt->write_buf_size = LOB_BUFLEN;
  } else {
    pt->write_buf = pt->write_pos = &pt->shortbuf;
    pt->write_buf_size = 1;
  }
  pt->write_end = pt->write_buf + pt->write_buf_size;

  SCM_SETCAR (port, TTN_COERCE_INT (SCM_CAR (port)) & ~SCM_BUF0);

  SCM_ALLOW_INTS;

  return port;
}

SCM_PROC (s_lob_lo_unlink, "pg-lo-unlink", 2, 0, 0, lob_lo_unlink);

static SCM
lob_lo_unlink (SCM conn, SCM oid)
{
  int ret;
  PGconn *dbconn;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, s_lob_lo_unlink);
  SCM_ASSERT (SCM_INUMP (oid), oid, SCM_ARG2, s_lob_lo_unlink);

  dbconn = sec_unbox (conn)->dbconn;

  SCM_DEFER_INTS;
  ret = lo_unlink (dbconn, SCM_INUM (oid));
  SCM_ALLOW_INTS;
  if (ret != 0)
    return SCM_BOOL_F;

  return SCM_BOOL_T;
}

SCM_PROC (s_lob_lo_get_connection, "pg-lo-get-connection", 1, 0, 0, lob_lo_get_connection);

static SCM
lob_lo_get_connection (SCM port)
{
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPLOBPORTP (port),
              port, SCM_ARG1, s_lob_lo_get_connection);

  return ((lob_stream *)SCM_STREAM (port))->conn;
}

SCM_PROC (s_lob_lo_get_oid, "pg-lo-get-oid", 1, 0, 0, lob_lo_get_oid);

static SCM
lob_lo_get_oid (SCM port)
{
  SCM_ASSERT (SCM_NIMP (port) && SCM_LOBPORTP (port),
              port, SCM_ARG1, s_lob_lo_get_oid);
  return SCM_MAKINUM (((lob_stream *)SCM_STREAM (port))->oid);
}

SCM_PROC (s_lob_lo_tell, "pg-lo-tell", 1, 0, 0, lob_lo_tell);

static SCM
lob_lo_tell (SCM port)
{
  SCM_ASSERT (SCM_NIMP (port)&&SCM_OPLOBPORTP (port),port,SCM_ARG1,s_lob_lo_tell);

  return scm_seek (port, SCM_INUM0, SCM_MAKINUM (SEEK_CUR));
}

static int terminating = 0;             /* wtf? -ttn */

static void
lob_flush (SCM port)
{
  scm_port *pt = SCM_PTAB_ENTRY (port);
  lob_stream *lobp = (lob_stream *) SCM_STREAM (port);
  PGconn *conn = LOB_CONN (lobp);
  char *ptr = pt->write_buf;
  int init_size = pt->write_pos - pt->write_buf;
  int remaining = init_size;

  while (remaining > 0) {
    int count;
#ifdef DEBUG_TRACE_LO_WRITE
    fprintf (stderr, "lob_flush (): lo_write (%.*s, %d) ... ", remaining, ptr, remaining);
#endif
    SCM_DEFER_INTS;
    count = lo_write (conn, lobp->fd, ptr, remaining);
    SCM_ALLOW_INTS;
#ifdef DEBUG_TRACE_LO_WRITE
    fprintf (stderr, "returned %d\n", count);
#endif
    if (count < remaining) {
      /* error.  assume nothing was written this call, but
         fix up the buffer for any previous successful writes.  */
      int done = init_size - remaining;

      if (done > 0) {
        int i;

        for (i = 0; i < remaining; i++) {
          * (pt->write_buf + i) = * (pt->write_buf + done + i);
        }
        pt->write_pos = pt->write_buf + remaining;
      }
      if (!terminating)
        scm_syserror ("lob_flush");
      else {
        const char *msg = "Error: could not flush large object file descriptor ";
        char buf[11];

        write (2, msg, strlen (msg));
        sprintf (buf, "%d\n", lobp->fd);
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

  if (offset > 0) {
    pt->read_pos = pt->read_end;
    SCM_DEFER_INTS;
    ret = lo_lseek (conn, lobp->fd, -offset, SEEK_CUR);
    SCM_ALLOW_INTS;
    if (ret == -1)
      scm_misc_error ("lob_end_input", "Error seeking on lo port %s",
                      scm_listify (port, SCM_UNDEFINED));
  }
  pt->rw_active = SCM_PORT_NEITHER;
}

SCM_PROC (s_lob_lo_seek, "pg-lo-seek", 3, 0, 0, lob_lo_seek);

static SCM
lob_lo_seek (SCM port, SCM where, SCM whence)
{
  int ret;
  lob_stream *lobp;
  PGconn *conn;

  SCM_ASSERT (SCM_NIMP (port) && SCM_OPLOBPORTP (port),
              port, SCM_ARG1, s_lob_lo_seek);
  SCM_ASSERT (SCM_INUMP (where), where, SCM_ARG2, s_lob_lo_seek);
  SCM_ASSERT (SCM_INUMP (whence), whence, SCM_ARG3, s_lob_lo_seek);

  lobp = (lob_stream *) SCM_STREAM (port);
  conn = LOB_CONN (lobp);

  lob_flush (port);

  SCM_DEFER_INTS;
  ret = lo_lseek (conn, lobp->fd, SCM_INUM (where), SCM_INUM (whence));
  SCM_ALLOW_INTS;

  return SCM_MAKINUM (ret);
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
  ret = lo_read (conn, lobp->fd, pt->read_buf, pt->read_buf_size);
  SCM_ALLOW_INTS;
#ifdef DEBUG_LO_READ
  fprintf (stderr, "lob_fill_input: lo_read (%d) returned %d.\n", pt->read_buf_size, ret);
#endif
  if (ret != pt->read_buf_size) {
    if (ret == 0)
      return EOF;
    else if (ret < 0)
      scm_misc_error ("lob_fill_buffer","Error (%s) reading from lo port %s",
                      scm_listify (SCM_MAKINUM (ret), port, SCM_UNDEFINED));
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

  if (pt->write_buf == &pt->shortbuf) {
    /* "unbuffered" port.  */
    int fdes = SCM_FSTREAM (port)->fdes;

    if (write (fdes, data, size) == -1)
      scm_syserror ("fport_write");
  } else {
    const char *input = (char *) data;
    size_t remaining = size;

    while (remaining > 0) {
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
    if ((TTN_COERCE_INT (SCM_CAR (port)) & SCM_BUFLINE)
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
  ret = lo_lseek (conn, lobp->fd, offset, whence);
  SCM_ALLOW_INTS;
  if (ret == -1)
    scm_misc_error ("lob_seek", "Error (%s) seeking on lo port %s",
                    scm_listify (SCM_MAKINUM (ret), port, SCM_UNDEFINED));

  /* Adjust return value to account for guile port buffering.  */
  if (SEEK_CUR == whence) {
    scm_port *pt = SCM_PTAB_ENTRY (port);
    ret -= (pt->read_end - pt->read_ret);
  }

  return ret;
}

/* Check whether a port can supply input.  */
static int
lob_input_waiting_p (SCM port)
{
  return 1;
}

SCM_PROC (s_lob_lo_read, "pg-lo-read", 3, 0, 0, lob_lo_read);

static SCM
lob_lo_read (SCM siz, SCM num, SCM port)
{
  scm_sizet n;
  SCM str;
  int len;
  int done = 0;

  SCM_ASSERT (SCM_INUMP (siz), siz, SCM_ARG1, s_lob_lo_read);
  SCM_ASSERT (SCM_INUMP (num), num, SCM_ARG2, s_lob_lo_read);
  SCM_ASSERT (SCM_NIMP (port) && SCM_OPINLOBPORTP (port),
              port, SCM_ARG3, s_lob_lo_read);

  len = SCM_INUM (siz) * SCM_INUM (num);
  str = scm_make_string (SCM_MAKINUM (len), SCM_UNDEFINED);
  for (n = 0 ; n < SCM_INUM (num) && ! done; n++) {
    scm_sizet m;
    int c;
    for (m = 0; m < SCM_INUM (siz); m++) {
      c = scm_getc (port);
      if (c == EOF) {
        done = 1;
        break;
      }
      * (SCM_CHARS (str) + n * SCM_INUM (siz) + m) = c;
    }
  }
  if (n < 0)
    return SCM_BOOL_F;
  if (n < SCM_INUM (num)) {
    SCM_DEFER_INTS; /* See comment re scm_vector_set_len in libguile/unif.c */
    scm_vector_set_length_x (str, SCM_MAKINUM (n * SCM_INUM (siz)));
    SCM_ALLOW_INTS;
  }
  return str;
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
  ret = lo_close (dbconn, lobp->fd);
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

  if (SCM_OPENP (port)) {
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

  if (SCM_OPENP (port)) {
    ret = lob_close (port);
  }
  free (lobp);
  return 0;
}

SCM_PROC (s_lob_lo_import, "pg-lo-import", 2, 0, 0, lob_lo_import);

static SCM
lob_lo_import (SCM conn, SCM filename)
{
  PGconn *dbconn;
  int ret;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, s_lob_lo_import);
  SCM_ASSERT (SCM_NIMP (filename) && SCM_ROSTRINGP (filename),
              filename, SCM_ARG2, s_lob_lo_import);

  dbconn = sec_unbox (conn)->dbconn;

  SCM_DEFER_INTS;
  ret = lo_import (dbconn, SCM_ROCHARS (filename));
  SCM_ALLOW_INTS;

  if (ret <= 0)
    return SCM_BOOL_F;

  return SCM_MAKINUM (ret);
}

SCM_PROC (s_lob_lo_export, "pg-lo-export", 3, 0, 0, lob_lo_export);

static SCM
lob_lo_export (SCM conn, SCM oid, SCM filename)
{
  PGconn *dbconn;
  Oid pg_oid;
  int ret;

  SCM_ASSERT (sec_p (conn), conn, SCM_ARG1, s_lob_lo_export);
  SCM_ASSERT (SCM_INUMP (oid), oid, SCM_ARG2, s_lob_lo_export);
  SCM_ASSERT (SCM_NIMP (filename) && SCM_ROSTRINGP (filename), filename,
              SCM_ARG3, s_lob_lo_export);
  dbconn = sec_unbox (conn)->dbconn;
  pg_oid = SCM_INUM (oid);

  SCM_DEFER_INTS;
  ret = lo_export (dbconn, pg_oid, SCM_ROCHARS (filename));
  SCM_ALLOW_INTS;

  if (ret != 1)
    return SCM_BOOL_F;

  return SCM_BOOL_T;
}

static int lob_printpt (SCM exp, SCM port, scm_print_state *pstate);

static int
lob_printpt (SCM exp, SCM port, scm_print_state *pstate)
{
  scm_puts ("#<PG-LO-PORT:", port);
  scm_print_port_mode (exp, port);
  if (SCM_OPENP (exp)) {
    lob_stream *lobp = (lob_stream *) SCM_STREAM (exp);
    scm_extended_dbconn *sec = sec_unbox (lobp->conn);
    char *dbstr = PQdb (sec->dbconn);
    char *hoststr = PQhost (sec->dbconn);
    char *portstr = PQport (sec->dbconn);
    char *optionsstr = PQoptions (sec->dbconn);

    scm_intprint (lobp->fd, 10, port); scm_puts (":", port);
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
