/* $Id$
    Guile-pg - A Guile interface to PostgreSQL
    Copyright (C) 1999-2000 The Free Software Foundation

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

/* We fudge this. I wonder why only libguile sources can access this? */
#define SCM_SYSCALL(line) line

typedef struct _scm_extended_dbconn {
    SCM          client;
    int          count;         /* which dbconn is this? */
    PGconn      *dbconn;        /* Postgres data structure */
    FILE        *fptrace;       /* The current trace stream */
} scm_extended_dbconn;

typedef struct _scm_extended_result {
    SCM          conn;          /* Connection */
    int          count;         /* which result is this? */
    PGresult    *result;        /* Postgres result structure */
} scm_extended_result;

extern int sec_p(SCM obj);
extern scm_extended_dbconn *sec_unbox(SCM obj);
extern SCM sec_box(scm_extended_dbconn *sec);
extern int ser_p(SCM obj);
extern scm_extended_result *ser_unbox(SCM obj);
extern SCM ser_box(scm_extended_result *ser);
extern void init_libpostgres_lo(void);
