/* tmpfile.c

   Copyright (C) 2003,2004,2005,2006,2008 Thien-Thi Nguyen

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

/* This file provides a replacement for tmpfile(3) named "guile_pg_tmpfile"
   for those systems that don't have it (as determined by the "configure"
   script).  Clients should do something like:

        #include "config.h"
        #ifndef HAVE_TMPFILE
        #include "tmpfile.h"
        #define tmpfile guile_pg_tmpfile
        #endif

   Implementation is very close (one could say inspired or even snarfed) to
   that found in glibc-2.2.5/sysdeps/generic/tmpfile.c; thanks for the Free
   Software!  */

#include "config.h"

#ifndef HAVE_TMPFILE

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "tmpfile.h"

FILE *
guile_pg_tmpfile (void)
{
  FILE *f;
  char buf[FILENAME_MAX];
  struct stat ignored;
  int fd, i, pid = getpid ();

  for (i = 0; i < 25; i++)
    {
      sprintf (buf, "/tmp/guile-pg.tmpfile.%d.%d", pid, i);
      if (0 > stat (buf, &ignored))
        {
          pid = 0;                      /* overloaded meaning: zero => ok */
          break;
        }
    }
  if (pid)
    return NULL;
  f = fopen (buf, "w+b");
  fd = fileno (f);
  if (fd < 0)
    return NULL;

  /* Note that this relies on the Unix semantics that
     a file is not really removed until it is closed.  */
  (void) remove (buf);

  if ((f = fdopen (fd, "w+b")) == NULL)
    close (fd);

  return f;
}
#endif /* !defined (HAVE_TMPFILE) */

/* tmpfile.c ends here */
