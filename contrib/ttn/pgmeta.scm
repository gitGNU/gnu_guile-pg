;;; ttn/pgmeta.scm --- Support PostgreSQL reflection

;; $State$:$Name$
;;
;; Copyright (C) 2001-2002 Thien-Thi Nguyen
;; This file is part of ttn's personal scheme library, released under GNU
;; GPL with ABSOLUTELY NO WARRANTY.  See the file COPYING for details.

;;; Commentary:

;; WARNING WARNING WARNING:
;; This module is still highly unstable!
;; Use at your own risk!

;;; Code:

(define-module (ttn pgmeta)
  :use-module (database postgres)
  :use-module (ttn stringutils)
  :use-module (ttn echo)
  :use-module (ttn pgtype)
  :use-module (ttn pgtable))

(define-db-col-type 'name "???"         ; like text?
  identity
  identity)

(define-db-col-type 'oid "-1"
  number->string
  string->number)

(define-db-col-type 'integer "0"
  number->string
  string->number)

(define-db-col-type 'char "?"
  (lambda (c) (make-string 1 c))
  (lambda (s) (string-ref s 0)))

(define-db-col-type 'smallint "0"
  number->string
  string->number)

(define-db-col-type 'boolean "f"
  (lambda (x) (if x "t" "f"))
  (lambda (s) (if (string=? s "f") #f #t)))

(define-db-col-type 'aclitem "?"
  identity
  identity)

(define-db-col-type-array-variant 'aclitem[] 'aclitem double-quote identity)

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
   (mapconcat (lambda (field)
                (let ((s (symbol->string field)))
                  (simple-format #f "rel~A as ~A" s s)))
              '(name kind natts hasindex checks triggers hasrules)
              ",")
   (string-append "where relname='" name "'")))

(define (table-fields-info M:pg-class name)
  (pg-exec (M:pg-class 'pgdb)
           (string-append
            "   SELECT a.attname, t.typname, a.attlen, a.atttypmod,"
            "          a.attnotnull, a.atthasdef, a.attnum"
            "     FROM pg_class c, pg_attribute a, pg_type t"
            "    WHERE c.relname = '" name "'"
            "      AND a.attnum > 0"
            "      AND a.attrelid = c.oid"
            "      AND a.atttypid = t.oid"
            " ORDER BY a.attnum")))

(define (OLD-fields-as-scheme-defs M:pg-class name)
  (let ((defs '()))
    ((M:pg-class 't-obj-walk)
     (tuples-result->table (table-fields-info M:pg-class name))
     (lambda (table tn fn string obj)
       (and (= 0 fn) (set! defs (cons string defs))))
     (lambda (table tn fn string)
       (and (= 0 fn) (set! defs (cons string defs)))))
    (reverse defs)))

(define (col-alist->alist-tuples object-alist) ; ugh
  (let ((cols (map car object-alist)))
    (apply map (lambda tuple-data
                 (map (lambda (col cell-data)
                        (cons col cell-data))
                      cols
                      tuple-data))
           (map cdr object-alist))))

(define (fields-as-scheme-defs M:pg-class name)
  (map (lambda (alist-tuple)
         (list (assq-ref alist-tuple 'attname)
               (assq-ref alist-tuple 'typname)))
       (col-alist->alist-tuples
        ((M:pg-class 'table->object-alist)
         (tuples-result->table (table-fields-info M:pg-class name))))))

(define (describe-table db-name table-name)
  (let ((M:pg-class (make-M:pg-class db-name)))
    (for-each echo (fields-as-scheme-defs M:pg-class table-name))
    (echo "[[[" table-name "]]]")
    (for-each (lambda (res) (display-table (tuples-result->table res)))
              `(,(table-info M:pg-class table-name)
                ,(table-fields-info M:pg-class table-name)))))

(debug-enable 'backtrace 'debug)

(export make-M:pg-class
        describe-table)

;;; ttn/pgmeta.scm ends here
