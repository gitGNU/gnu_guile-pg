/* gi.h

   Copyright (C) 2004,2005,2006,2008 Thien-Thi Nguyen

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

/*
 * always
 */

#ifndef _GI_H_
#define _GI_H_

#include <libguile.h>
#include <guile/gh.h>

/*
 * backward (sometimes foresight was incomplete)
 */

#ifdef HAVE_GUILE_MODSUP_H
#include <guile/modsup.h>
#else
#include "modsup.h"
#endif

/*
 * forward (sometimes kids make messes for others to clean up)
 */

#ifdef HAVE_SCM_GC_PROTECT_OBJECT
#define scm_protect_object(x)  (scm_gc_protect_object (x))
#endif

#endif /* _GI_H_ */

/* gi.h ends here */
