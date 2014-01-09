;;; meta.scm

;; Copyright (C) 2008 Thien-Thi Nguyen
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

(cond ((not *all-tests*))
      (else (display "Bad env.")
            (newline)
            (exit #f)))

(use-modules (database postgres) (database postgres-meta))

(fresh!)

(define N-EXPECTED 39)                  ; maintain me

(define CONN (pg-connectdb ""))

(define VIEWS (map (lambda (name)
                     (simple-format
                      #f "(SELECT count(*),'~A' AS view FROM ~A)"
                      name name))
                   (information-schema-names #t)))

(define QUERY (apply string-append
                     (car VIEWS)
                     (map (lambda (s)
                            (string-append " UNION " s))
                          (cdr VIEWS))))

(define RES (pg-exec CONN (string-append QUERY ";")))
(with-output-to-file "meta.log"
  (lambda () (pg-print RES)))

(define RV (and (= N-EXPECTED (length VIEWS))
                (eq? 'PGRES_TUPLES_OK (pg-result-status RES))
                (= N-EXPECTED (pg-ntuples RES))))

(set! RES #f)
(pg-finish CONN)
(set! CONN #f)

(drop!)

(exit RV)

;;; meta.scm ends here
