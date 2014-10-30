;;; alive.scm

;; Copyright (C) 2008, 2009, 2014 Thien-Thi Nguyen
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

(fso "PGHOST: ~A~%" (or (getenv "PGHOST")
                        '(unset)))

(use-modules (database postgres))

(or (defined? 'pg-guile-pg-loaded)
    (error "failed to load module: (database postgres)"))

(define FEATURES (pg-guile-pg-loaded))

(or (and (list? FEATURES)
         (and-map symbol? FEATURES))
    (error "‘pg-guile-pg-loaded’ rv malformed:" FEATURES))

(for-each (lambda (s)
            (fso "FEATURE: ~A~%" s))
          FEATURES)

(exit #t)

;;; alive.scm ends here
