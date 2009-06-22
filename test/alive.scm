;;; alive.scm

;; Copyright (C) 2008, 2009 Thien-Thi Nguyen
;;
;; This file is part of Guile-PG.
;;
;; Guile-PG is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3 of the License, or
;; (at your option) any later version.
;;
;; Guile-PG is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with Guile-PG; if not, write to the Free Software Foundation,
;; Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

(or (not *all-tests*)
    (begin (display "Bad env.")
           (newline)
           (exit #f)))

(simple-format #t "PGHOST: ~A\n" (or (getenv "PGHOST")
                                     '(unset)))

(use-modules (database postgres))

(exit #t)

;;; alive.scm ends here
