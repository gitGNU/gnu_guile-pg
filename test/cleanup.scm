;;; cleanup.scm

;; Copyright (C) 2014 Thien-Thi Nguyen
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
;; along with Guile-PG.  If not, see <http://www.gnu.org/licenses/>.

(or (not *all-tests*)
    (begin (display "Bad env.")
           (newline)
           (exit #f)))

(exit (if (env-1? "KEEPD")
          77
          (equal? 0 (system (string-append
                             (in-vicinity (getenv "srcdir")
                                          "fake-cluster-control")
                             " 0"
                             (if (env-1? "VERBOSE")
                                 ""
                                 " 1>/dev/null 2>&1"))))))

;;; cleanup.scm ends here
