;;; postgres-meta.scm --- Methods for understanding PostgreSQL data structures

;;    Guile-pg - A Guile interface to PostgreSQL
;;    Copyright (C) 2002, 2003, 2004 Free Software Foundation, Inc.
;;
;;    This program is free software; you can redistribute it and/or modify
;;    it under the terms of the GNU General Public License as published by
;;    the Free Software Foundation; either version 2 of the License, or
;;    (at your option) any later version.
;;
;;    This program is distributed in the hope that it will be useful,
;;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;    GNU General Public License for more details.
;;
;;    You should have received a copy of the GNU General Public License
;;    along with this program; if not, write to the Free Software
;;    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

;;; Author: Thien-Thi Nguyen <ttn@gnu.org>

;;; Commentary:

;; This module exports the procs:
;;   (infer-defs CONN TABLE-NAME) => defs
;;   (describe-table! DB-NAME TABLE-NAME)

;;; Code:

(define-module (database postgres-meta)
  #:use-module (database postgres)
  #:use-module (database postgres-types)
  #:use-module (database postgres-resx)
  #:use-module (database postgres-table)
  #:use-module (srfi srfi-13)
  #:export (infer-defs
            describe-table!))

(define (make-M:pg-class db-name)
  (pgtable-manager db-name "pg_class"
                   '((relname      name)
                     (reltype      oid)
                     (relowner     integer)
                     (relam        oid)
                     (relpages     integer)
                     (reltuples    integer)
                     (rellongrelid oid)
                     (relhasindex  boolean)
                     (relisshared  boolean)
                     (relkind      char)
                     (relnatts     smallint)
                     (relchecks    smallint)
                     (reltriggers  smallint)
                     (relukeys     smallint)
                     (relfkeys     smallint)
                     (relrefs      smallint)
                     (relhaspkey   boolean)
                     (relhasrules  boolean)
                     (relacl       aclitem[]))))

(define (table-info M:pg-class name)
  ((M:pg-class 'select)
   (string-join (map (lambda (field)
                       (let ((s (symbol->string field)))
                         (simple-format #f "rel~A as ~A" s s)))
                     '(name
                       kind
                       natts
                       hasindex
                       checks
                       triggers
                       hasrules))
                ",")
   (string-append "where relname='" name "'")))

(define (table-fields-info conn table-name)
  (pg-exec conn (string-append
                 "   SELECT a.attname, t.typname, a.attlen, a.atttypmod,"
                 "          a.attnotnull, a.atthasdef, a.attnum"
                 "     FROM pg_class c, pg_attribute a, pg_type t"
                 "    WHERE c.relname = '" table-name "'"
                 "      AND a.attnum > 0"
                 "      AND a.attrelid = c.oid"
                 "      AND a.atttypid = t.oid"
                 " ORDER BY a.attnum")))

;; Return a @dfn{defs} form suitable for use with @code{pgtable-manager} for
;; connection @var{conn} and @var{table-name}.  The column names are exact.
;; The column types are incorrect for array types, which are described as
;; @code{_FOO}; there is currently no way to infer whether this means
;; @code{FOO[]} or @code{FOO[][]}, etc, without looking at the table's data.
;; No type options are checked at this time.
;;
(define (infer-defs conn table-name)
  (let ((res (table-fields-info conn table-name)))
    (map (lambda args args)
         (result-field->object-list res 0 string->symbol)
         (result-field->object-list res 1 string->symbol))))

;; Display information on database @var{db-name} table @var{table-name}.
;; Include a defs form suitable for use with @code{pgtable-manager};
;; info about the table (kind, natts, hasindex, checks, triggers, hasrules);
;; and info about each field in the table (typname, attlen, atttypmod,
;; attnotnull, atthasdef, attnum).
;;
(define (describe-table! db-name table-name)
  (let ((M:pg-class (make-M:pg-class db-name)))
    (for-each (lambda (x) (format #t "~A\n" x))
              (infer-defs (M:pg-class 'pgdb) table-name))
    (for-each (lambda (x) (display-table
                           (cond ((pg-result? x)
                                  (tuples-result->table x))
                                 (else x))))
              `(,(table-info M:pg-class table-name)
                ,(table-fields-info (M:pg-class 'pgdb) table-name)))))

;; --------------------------------------------------------------------------
;; this belongs elsewhere

(define (display-table table . style)

  (define (styler name)
    (case name
      ((space)      (lambda (x) (case x ((h) #\space) (else " "))))
      ((h-only)     (lambda (x) (case x ((h) #\-) ((v) " ") ((+) "-"))))
      ((v-only)     (lambda (x) (case x ((h) #\space) ((v) "|") ((+) "|"))))
      ((+-only)     (lambda (x) (case x ((h) #\space) ((v) " ") ((+) "+"))))
      ((no-h)       (lambda (x) (case x ((h) #\space) ((v) "|") ((+) "+"))))
      ((no-v)       (lambda (x) (case x ((h) #\-) ((v) " ") ((+) "+"))))
      ((no-+)       (lambda (x) (case x ((h) #\-) ((v) "|") ((+) " "))))
      ((fat-space)  (lambda (x) (case x ((h) #\space) (else "  "))))
      ((fat-no-v)   (lambda (x) (case x ((h) #\-) ((v) "   ") ((+) "-+-"))))
      ((fat-h-only) (lambda (x) (case x ((h) #\-) ((v) "  ") ((+) "--"))))
      (else         (error "bad style:" style))))

  (let* ((style (if (null? style)
                    (lambda (x) (case x ((h) #\-) ((v) "|") ((+) "+")))
                    (let ((style (car style)))
                      (cond ((procedure? style) style)
                            ((symbol? style) (styler style))
                            (else (error "bad style:" style))))))
         (names  (object-property table 'names))
         (widths (object-property table 'widths))
         (tuples (iota (car  (array-dimensions table))))
         (fields (iota (cadr (array-dimensions table)))))

    (define (-row sep producer padding)
      (for-each (lambda (fn)
                  (display sep)
                  (let ((s (producer fn)))
                    (display s)
                    (display (make-string (- (array-ref widths fn)
                                             (string-length s))
                                          padding))))
                fields)
      (display sep)
      (newline))

    (define (-hr) (-row (style '+) (lambda (fn) "") (style 'h)))

    ;; do it
    (-hr)
    (-row (style 'v) (lambda (fn) (array-ref names fn)) #\space)
    (-hr)
    (for-each (lambda (tn)
                (-row (style 'v)
                      (lambda (fn) (array-ref table tn fn))
                      #\space))
              tuples)
    (-hr)))

;;; postgres-meta.scm ends here
