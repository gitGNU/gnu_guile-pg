;;; postgres-types.scm --- convert PostgreSQL <-> Scheme objects

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

;; This module provides the procs:
;;  (oid-type-name-cache CONN . FRESH?) => ((OID . TYPE-NAME) ...)
;;  (dbcoltypes) => list of names
;;  (dbcoltype-lookup NAME) => tobj
;;  (dbcoltype:name TOBJ) => symbol
;;  (dbcoltype:stringifier TOBJ) => proc
;;  (dbcoltype:objectifier TOBJ) => proc
;;  (dbcoltype:default) => string
;;  (define-db-col-type NAME DEFAULT STRINGIFY OBJECTIFY)
;;  (define-db-col-type-array-variant COMPOSED SIMPLE [STRINGIFY [OBJECTIFY]])
;;
;; NAME, COMPOSED and SIMPLE are symbols naming a type.  COMPOSED is
;; conventionally formed by appending square-bracket paris to SIMPLE.  For
;; example, we can declare a two-dimensional array type of text elements:
;;
;;  (define-db-col-type-array-variant 'text[][] 'text ...)
;;
;; STRINGIFY is a procedure that takes a Scheme object and returns a string,
;; suitable for sending to PostgreSQL.  OBJECTIFY does the opposite: it takes
;; a string from PostgreSQL and returns a Scheme object.
;;
;; DEFAULT is a string.  TOBJ is a "type object" which should be considered
;; opaque (use the dbcoltype:foo procs to access the components).
;;
;; TODO: Look into "user-defined" types from PostgreSQL point of view.

;;; Code:

(define-module (database postgres-types)
  #:autoload (database postgres) (pg-exec)
  #:export (oid-type-name-cache
            dbcoltypes
            dbcoltype-lookup
            dbcoltype:name
            dbcoltype:stringifier
            dbcoltype:default
            dbcoltype:objectifier
            define-db-col-type
            define-db-col-type-array-variant))

(define put set-object-property!)
(define get object-property)

;; Query connection @var{conn} for oid/type-name info, caching results.
;; Optional arg @var{fresh?} forces a (re-)query, bypassing the cache.
;; Return a list of oid/type-name (number/string) pairs.
;;
(define (oid-type-name-cache conn . fresh?)
  (or (and (null? fresh?)
           (get conn oid-type-name-cache))
      (put conn oid-type-name-cache
           (let ((res (pg-exec conn "SELECT oid,typname FROM pg_type;")))
             (and (eq? 'PGRES_TUPLES_OK (pg-result-status res))
                  (let loop ((n (1- (pg-ntuples res))) (acc '()))
                    (if (> 0 n)
                        acc
                        (loop (1- n)
                              (acons (string->number (pg-getvalue res n 0))
                                     (pg-getvalue res n 1)
                                     acc)))))))))

;; column types / definition

(define *db-col-types* '())     ; (NAME STRINGIFIER DEFAULT OBJECTIFIER)

;; Return names of all registered type converters.
;;
(define (dbcoltypes)
  (map car *db-col-types*))

;; Return a type-converter object given its @var{type-name}, a symbol.
;; Return #f if no such t-c object by that name exists.
;;
(define (dbcoltype-lookup type-name)
  (assq type-name *db-col-types*))

;; Extract type name from the type-converter object @var{tc}.
(define (dbcoltype:name tc) (car tc))
;; Extract stringifier from the type-converter object @var{tc}.
(define (dbcoltype:stringifier tc) (cadr tc))
;; Extract default string from the type-converter object @var{tc}.
(define (dbcoltype:default tc) (caddr tc))
;; Extract objectifier from the type-converter object @var{tc}.
(define (dbcoltype:objectifier tc) (cadddr tc))

(define (read-pgarray-1 objectifier port)
  ;; ugh, i hate parsing...  the right thing to do would be find out if
  ;; postgres supports (easily!)  parameterizable array output formatting.
  ;; then, we could just specify using "()" instead of "{}" and space instead
  ;; of comma, and this proc becomes a simple objectifier walk over `read'
  ;; output.
  (let ((next (lambda () (peek-char port))))
    (let loop ((c (next)) (acc '()))
      (cond ((eof-object? c) (reverse acc))                     ;;; retval
            ((char=? #\} c) (read-char port) (reverse acc))     ;;; retval
            ((char=? #\{ c)
             (read-char port)
             (let ((sub (read-pgarray-1 objectifier port)))
               (loop (next) (cons sub acc))))
            ((char=? #\" c)
             (let ((string (read port)))
               (loop (next) (cons (objectifier string) acc))))
            ((char=? #\, c)
             (read-char port)
             (loop (next) acc))
            (else (let ((o (let iloop ((ic (read-char port)) (iacc '()))
                             (case ic
                               ((#\} #\,)
                                (unread-char ic port)
                                (objectifier (list->string (reverse iacc))))
                               (else
                                (iloop (read-char port) (cons ic iacc)))))))
                    (loop (next) (cons o acc))))))))

(define (read-array-string objectifier string)
  (call-with-input-string string
                          (lambda (port)
                            (read-char port)
                            (read-pgarray-1 objectifier port))))

(define (dimension->string-proc stringifier)
  (lambda (ls/vec) (dimension->string stringifier ls/vec)))

(define (read-array-string-proc objectifier)
  (lambda (string) (read-array-string objectifier string)))

;; Register type NAME with DEFAULT, STRINGIFIER and OBJECTIFIER procs.
;; NAME is a symbol.  DEFAULT is a string to use if the Scheme object is #f.
;; STRINGIFIER is a proc that takes a Scheme object and returns a string
;; suitable for use in an "INSERT VALUES" SQL command.
;; OBJECTIFIER is a proc that takes a string and returns the Scheme object
;; parsed out of it.
;;
;; Both STRINGIFIER and OBJECTIFIER need not worry about SQL-style
;; quoting (using single quotes) and related quote escaping.
;;
;; If NAME already exists, it is redefined.  See also `dbcoltype-lookup'.
;;
(define (define-db-col-type name default stringifier objectifier)
  (let ((lookup (dbcoltype-lookup name))
        (body (list stringifier default objectifier)))
    (if lookup
        (set-cdr! lookup body)
        (set! *db-col-types* (acons name body *db-col-types*)))))

(define (dimension->string stringifier x)
  (letrec ((dive (lambda (ls)
                   (list "{" (dimension->string stringifier (car ls))
                         (map (lambda (y)
                                (list "," (dimension->string stringifier y)))
                              (cdr ls))
                         "}")))
           (walk (lambda (x)
                   (cond ((string? x) (display x))
                         ((list? x) (for-each walk x))
                         (else (error "bad type:" x)))))
           (flatten (lambda (tree)
                      (with-output-to-string
                        (lambda () (walk tree))))))
    (cond ((list? x)   (flatten (dive x)))
          ((vector? x) (flatten (dive (vector->list x))))
          (else        (stringifier x)))))

;; Register type COMPOSED, an array variant of SIMPLE, with optional PROCS.
;; SIMPLE should be a type name already registered using `define-db-col-type'.
;; COMPOSED is conventionally formed by appending SIMPLE with one or more pairs
;; of square braces "[]", with the number of pairs indicating the array
;; dimensionality.  For example, if SIMPLE is `text', a two-dimensional text
;; array would be named `text[][]'.
;;
;; Optional arg PROCS is a list specifying alternative stringifier and
;; objectifier procedures (in that order).  If unspecified, SIMPLE is looked up
;; and its stringifier and objectifier are used.  See `dbcoltype-lookup'.
;;
;; The default value of all array types is "@{@}" and cannot be changed.
;;
(define (define-db-col-type-array-variant composed simple . procs)
  (let* ((lookup (dbcoltype-lookup simple))
         (stringifier (or (and (not (null? procs))
                               (car procs))
                          (dbcoltype:stringifier lookup)))
         (objectifier (or (and (not (null? procs))
                               (not (null? (cdr procs)))
                               (cadr procs))
                          (dbcoltype:objectifier lookup))))
    (define-db-col-type composed "{}"
      (dimension->string-proc stringifier)
      (read-array-string-proc objectifier))))

;;;---------------------------------------------------------------------------
;;; load-time actions: set up *db-col-types*

;; support

(define (double-quote s)
  (string-append "\"" s "\""))

;; non-array (simple) types

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

(define-db-col-type 'bool "f"
  ;; see also `boolean' below
  (lambda (x) (if x "t" "f"))
  (lambda (s) (not (string=? "f" s))))

(define-db-col-type 'boolean "f"
  ;; see also `bool' above
  (lambda (x) (if x "t" "f"))
  (lambda (s) (not (string=? "f" s))))

(define-db-col-type 'serial "0"
  ;; This is a magic PostgreSQL type that actually causes the backend
  ;; to create a sequence `SEQ' w/ the column typed `serial' doing a
  ;; nextval('SEQ').  The sequence name is TABLENAME_COLNAME_seq,
  ;; where TABLENAME and COLNAME are not defined here.  Arguably, this
  ;; simplistic stringification/objectification is insufficient.
  (lambda (val) (number->string val))
  (lambda (string) (string->number string)))

(define-db-col-type 'timestamp "1970-01-01 00:00:00"
  (lambda (time)
    (cond ((string? time) time)
          ((number? time) (strftime "%Y-%m-%d %H:%M:%S" (localtime time)))
          (else (error "bad timestamp-type input:" time))))
  (lambda (string)
    (car (mktime (car (strptime "%Y-%m-%d %H:%M:%S" string))))))

(define-db-col-type 'text ""
  identity
  identity)

(define-db-col-type 'int4 "0"
  number->string
  string->number)

(define-db-col-type 'float4 "0.0"
  number->string
  string->number)

(define-db-col-type 'real "0.0"
  number->string
  string->number)

(define-db-col-type 'name "???"
  (lambda (val) (substring val 0 31))   ; PostgreSQL's User's Guide 3.3
  identity)

(define-db-col-type 'aclitem "?"
  identity
  identity)

;; array variants

(define-db-col-type-array-variant 'text[]   'text double-quote identity)
(define-db-col-type-array-variant 'text[][] 'text double-quote identity)
(define-db-col-type-array-variant 'int4[]   'int4)
(define-db-col-type-array-variant 'aclitem[] 'aclitem double-quote identity)

;;; postgres-types.scm ends here
