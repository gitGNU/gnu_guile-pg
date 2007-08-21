/* tmpfile.h

   Copyright (C) 2003,2004,2005,2006 Thien-Thi Nguyen

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

/* Clients should do something like:

        #include "config.h"
        #ifndef HAVE_TMPFILE
        #include "tmpfile.h"
        #define tmpfile guile_pg_tmpfile
        #endif

   See tmpfile.c for more info.  */

#ifndef GUILE_PG_TMPFILE_H
#define GUILE_PG_TMPFILE_H

#include <stdio.h>
extern FILE *guile_pg_tmpfile (void);

#endif /* !defined (GUILE_PG_TMPFILE_H) */

/* tmpfile.h ends here */
