;;; postgres-meta.scm --- Methods for understanding PostgreSQL data structures

;;	Copyright (C) 2002, 2003, 2004, 2005 Thien-Thi Nguyen
;;
;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 2 of the License, or
;; (at your option) any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program; if not, write to the Free Software
;; Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

;;; Commentary:

;; This module exports the procs:
;;   (defs-from-psql PSQL DB-NAME TABLE-NAME) => defs
;;   (check-type/elaborate TYPE) => #f, or KNOWN-TOBJ, or NEW-TYPE
;;   (strictly-check-types/elaborate! TABLE-NAME TYPES) => #t, or ERROR
;;   (infer-defs CONN TABLE-NAME) => defs
;;   (describe-table! DB-NAME TABLE-NAME)
;;
;; DB-NAME and TABLE-NAME are strings.  DEF is a single column def.  DEFS is a
;; list of column defs.  TYPE and NEW-TYPE are symbols.  KNOWN-TOBJ is the
;; type object already registered.  CONN is a the result of `pg-connectdb'.
;; ERROR means an error is thrown.  PSQL is either a string, a thunk, or #t.

;;; Code:

(define-module (database postgres-meta)
  #:use-module ((database postgres)
                #:select (pg-exec
                          pg-print))
  #:use-module ((database postgres-qcons)
                #:select (sql-command<-trees
                          make-SELECT/FROM/COLS-tree
                          parse+make-SELECT/tail-tree))
  #:use-module ((database postgres-types)
                #:select (dbcoltype-lookup
                          define-db-col-type-array-variant))
  #:use-module ((database postgres-resx)
                #:select (result-field->object-list))
  #:use-module ((database postgres-table)
                #:select (pgtable-manager
                          compile-outspec))
  #:autoload (srfi srfi-13) (string-trim-both)
  #:autoload (ice-9 popen) (open-input-pipe)
  #:export (defs-from-psql
            check-type/elaborate
            strictly-check-types/elaborate!
            infer-defs
            describe-table!))

(define (fs s . args)
  (apply simple-format #f s args))

;; Run @var{psql} and return a @dfn{defs} form for db @var{db-name}, table
;; @var{table-name}.  @var{psql} can be a string specifying the filename of
;; the psql (or psql-workalike) program, a thunk that produces such a string,
;; or #t, which means use the first "psql" found in the directories named by
;; the @code{PATH} env var.  @code{defs-from-psql} signals "bad psql" error
;; otherwise.
;;
;; In the returned defs, the column names are exact.  The column types and
;; options are only as exact as @var{psql} can produce.  Options are returned
;; as a list, each element of which is either a string (possibly with embedded
;; spaces), or a sub-list of symbols and/or numbers.  Typically the sub-list,
;; if any, will be the first option.  For example, if the column is specified
;; as @code{amount numeric(9,2) not null}, the returned def is the four-element
;; list: @code{(amount numeric (9 2) "not null")}.
;;
(define (defs-from-psql psql db-name table-name)

  (define (psql-command)
    (fs "~A -d ~A -F ' ' -P t -A -c '\\d ~A'"
        (cond ((string? psql) psql)
              ((procedure? psql) (psql))
              ((eq? #t psql) "psql")
              (else (error "bad psql:" psql)))
        db-name table-name))

  (define (read-def p)                  ; NAME TYPE [OPTIONS...]
                                        ; => eof object, or def
    (define (read-type)
      (let ((type (read p)))
        (if (string? type)              ; handle "TYPE" as well as TYPE
            (string->symbol type)
            type)))

    (define (read-options)              ; => eof object, or list

      (define (stb s)
        (string-trim-both s))

      (define (line-remainder)
        (let drain ((c (read-char p)) (acc '()))
          (if (char=? #\newline c)
              (stb (list->string (reverse! acc)))
              (drain (read-char p)
                     (cons (if (char=? #\, c) #\space c) acc)))))

      (define (eol-if-null-string s)
        (and (string-null? s) '()))

      (let ((opts (line-remainder)))
        (cond ((eol-if-null-string opts))
              ((and (char=? #\( (string-ref opts 0))
                    (string-index opts #\)))
               => (lambda (cut)
                    (let ((rest (stb (substring opts (1+ cut)))))
                      (cons (with-input-from-string opts read)
                            (or (eol-if-null-string rest)
                                (list rest))))))
              (else
               (list opts)))))

    (let ((name (read p)))
      (or (and (eof-object? name) name)
          (cons* name (read-type) (read-options)))))

  (let* ((psql-spew (open-input-pipe (psql-command)))
         (next (lambda () (read-def psql-spew))))
    (let loop ((def (next)) (acc '()))
      (if (eof-object? def)
          (begin
            (close-pipe psql-spew)
            (reverse! acc))             ; rv
          (loop (next) (cons def acc))))))

;; Check @var{type}, a symbol.  If it not an array variant, return non-#f only
;; if its type converters are already registered with Guile-PG.  If @var{type}
;; is an array variant, check the base (non-array) type first, and if needed,
;; ensure that the array variant type is registered.  Return non-#f if
;; successful.
;;
(define (check-type/elaborate type)
  (let* ((s (symbol->string type))
         (n (string-index s #\[))
         (base (string->symbol (if n (substring s 0 n) s))))
    (and (dbcoltype-lookup base)
         (or (not n)
             (let ((array-variant type))
               (or (dbcoltype-lookup array-variant)
                   (begin
                     (define-db-col-type-array-variant array-variant base)
                     array-variant)))))))

;; For table @var{table-name}, check @var{types} (list of symbols) with
;; @code{check-type/elaborate} and signal error for those types that do not
;; validate, or return non-#f otherwise.  The @var{table-name} is used only to
;; form the error message.
;;
(define (strictly-check-types/elaborate! table-name types)
  (let ((bad '()))
    (for-each (lambda (type)
                (or (check-type/elaborate type)
                    (set! bad (cons type bad))))
              types)
    (or (null? bad)
        (error (fs "bad ~S types: ~S" table-name bad)))))

(define *class-defs*
  ;; todo: Either mine from "psql \d pg_class", or verify at "make install"
  ;;       time, and invalidate this with external psql fallback.
  '((relname           name "not null")
    (relnamespace       oid "not null")
    (reltype            oid "not null")
    (relowner       integer "not null")
    (relam              oid "not null")
    (relfilenode        oid "not null")
    (relpages       integer "not null")
    (reltuples         real "not null")
    (reltoastrelid      oid "not null")
    (reltoastidxid      oid "not null")
    (relhasindex    boolean "not null")
    (relisshared    boolean "not null")
    (relkind           char "not null")
    (relnatts      smallint "not null")
    (relchecks     smallint "not null")
    (reltriggers   smallint "not null")
    (relukeys      smallint "not null")
    (relfkeys      smallint "not null")
    (relrefs       smallint "not null")
    (relhasoids     boolean "not null")
    (relhaspkey     boolean "not null")
    (relhasrules    boolean "not null")
    (relhassubclass boolean "not null")
    (relacl         aclitem[])))

(define (make-M:pg-class)
  (pgtable-manager "template1" "pg_class" *class-defs*))

(define *table-info-selection*
  (delay (compile-outspec
          (map (lambda (field)
                 (list #t (symbol->string field) (symbol-append 'rel field)))
               '(name
                 kind
                 natts
                 hasindex
                 checks
                 triggers
                 hasrules))
          *class-defs*)))

(define (table-info M:pg-class name)
  ((M:pg-class #:select) (force *table-info-selection*)
   #:where `(= relname ,name)))

(define (table-fields-info conn table-name)
  (pg-exec conn (sql-command<-trees
                 (make-SELECT/FROM/COLS-tree
                  '((c . pg_class) (a . pg_attribute) (t . pg_type))
                  '(a.attname t.typname a.attlen a.atttypmod
                              a.attnotnull a.atthasdef a.attnum))
                 (parse+make-SELECT/tail-tree
                  `(#:where
                    (and (= c.relname ,table-name)
                         (> a.attnum 0)
                         (= a.attrelid c.oid)
                         (= a.atttypid t.oid))
                    #:order-by
                    ((< a.attnum)))))))

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
  (let* ((M:pg-class (make-M:pg-class))
         (conn ((M:pg-class #:k) #:connection)))
    (for-each (lambda (x) (display x) (newline))
              (infer-defs conn table-name))
    (newline)
    (pg-print (table-info M:pg-class table-name))
    (pg-print (table-fields-info conn table-name))))

;;; postgres-meta.scm ends here
