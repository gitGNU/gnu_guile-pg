;;; postgres-types.scm --- convert PostgreSQL <-> Scheme objects

;; Copyright (C) 2002, 2003, 2004, 2005, 2006,
;;   2007, 2008, 2009, 2011, 2012 Thien-Thi Nguyen
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

;;; Commentary:

;; This module provides the procs:
;;  (oid-type-name-cache CONN . FRESH?) => ((OID . TYPE-NAME) ...)
;;  (dbcoltypes) => list of names
;;  (dbcoltype-lookup NAME) => tobj
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
;; opaque (and thus subject to change w/o warning).  Use the dbcoltype:foo
;; procs to access the components.
;;
;; TODO: Look into "user-defined" types from PostgreSQL point of view.

;;; Code:

(define-module (database postgres-types)
  #:export (oid-type-name-cache
            dbcoltypes
            dbcoltype-lookup
            dbcoltype:stringifier
            dbcoltype:default
            dbcoltype:objectifier
            define-db-col-type
            define-db-col-type-array-variant)
  #:autoload (database postgres) (pg-exec))

(define o/t (make-object-property))     ; oid/type-name info

;; Query connection @var{conn} for oid/type-name info, caching results.
;; Optional arg @var{fresh?} non-@code{#f} a (re-)query, updating the cache.
;; Return a list of oid/type-name (number/string) pairs.
;;
;;-args: (- 1 0 fresh?)
;;
(define (oid-type-name-cache conn . opt)
  (define (fresh)
    (let ((res (pg-exec conn "SELECT oid,typname FROM pg_type;")))
      (and (eq? 'PGRES_TUPLES_OK (pg-result-status res))
           (let loop ((n (1- (pg-ntuples res))) (acc '()))
             (if (> 0 n)
                 acc
                 (loop (1- n)
                       (acons (string->number (pg-getvalue res n 0))
                              (pg-getvalue res n 1)
                              acc)))))))
  (define (fresh!)
    (let ((alist (fresh)))
      (set! (o/t conn) alist)
      alist))
  (cond ((and (not (null? opt)) (car opt)) (fresh!))
        ((o/t conn))
        (else (fresh!))))

;; column types / definition

(define *db-col-types* '())     ; (NAME . #(STRINGIFIER DEFAULT OBJECTIFIER))

;; Return names of all registered type converters.
;;
(define (dbcoltypes)
  (map car *db-col-types*))

;; Return a type-converter object given its @var{type-name}, a symbol.
;; Return @code{#f} if no such t-c object by that name exists.
;;
(define (dbcoltype-lookup type-name)
  (assq-ref *db-col-types* type-name))

;; Extract stringifier from the type-converter object @var{tc}.
(define (dbcoltype:stringifier tc) (vector-ref tc 0))
;; Extract default string from the type-converter object @var{tc}.
(define (dbcoltype:default tc) (vector-ref tc 1))
;; Extract objectifier from the type-converter object @var{tc}.
(define (dbcoltype:objectifier tc) (vector-ref tc 2))

(define (read-pgarray-1 objectifier port)
  ;; ugh, i hate parsing...  the right thing to do would be find out if
  ;; postgres supports (easily!)  parameterizable array output formatting.
  ;; then, we could just specify using "()" instead of "{}" and space instead
  ;; of comma, and this proc becomes a simple objectifier walk over ‘read’
  ;; output.
  (let ((next (lambda () (peek-char port))))
    (let loop ((c (next)) (acc '()))
      (cond ((eof-object? c) (reverse! acc))                    ;;; retval
            ((char=? #\} c) (read-char port) (reverse! acc))    ;;; retval
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
                                (objectifier (list->string (reverse! iacc))))
                               (else
                                (iloop (read-char port) (cons ic iacc)))))))
                    (loop (next) (cons o acc))))))))

(define (read-array-string objectifier string)
  (call-with-input-string string
                          (lambda (port)
                            (read-char port)
                            (read-pgarray-1 objectifier port))))

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

(define (dimension->string-proc stringifier)
  (lambda (ls/vec) (dimension->string stringifier ls/vec)))

(define (read-array-string-proc objectifier)
  (lambda (string) (read-array-string objectifier string)))

;; Register type @var{name} with @var{default}, @var{stringifier} and
;; @var{objectifier} procs.  @var{name} is a symbol.  @var{default} is a
;; string to use if the Scheme object is @code{#f}.  @var{stringifier} is a
;; proc that takes a Scheme object and returns a string suitable for use in an
;; @code{INSERT VALUES} SQL command.  @var{objectifier} is a proc that takes a
;; string and returns the Scheme object parsed out of it.
;;
;; Both @var{stringifier} and @var{objectifier} need not worry about SQL-style
;; quoting (using single quotes) and related quote escaping.
;;
;; If @var{name} already exists, it is redefined.
;; See also @code{dbcoltype-lookup}.
;;
(define (define-db-col-type name default stringifier objectifier)
  (set! *db-col-types*
        (assq-set! *db-col-types* name
                   (vector stringifier default objectifier))))

;; Register type @var{composed}, an array variant of @var{simple}, with
;; optional @var{procs}.  @var{simple} should be a type name already
;; registered using @code{define-db-col-type}.  @var{composed} is
;; conventionally formed by appending @var{simple} with one or more pairs of
;; @samp{[]} (square braces), with the number of pairs indicating the array
;; dimensionality.  For example, if @var{simple} is @code{text}, a
;; two-dimensional text array would be named @code{text[][]}.
;;
;; Optional arg @var{procs} is a list specifying alternative stringifier and
;; objectifier procedures (in that order).  If unspecified, @var{simple} is
;; looked up and its stringifier and objectifier are used.  See
;; @code{dbcoltype-lookup}.
;;
;; The default value of all array types is @samp{@{@}} and cannot be changed.
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

;; non-array (simple) types -- X.Y.Z is from PostgreSQL 7.4.17 User's Guide

;; -- numeric types
;; -- the integer types

(define-db-col-type 'smallint "0"
  number->string
  string->number)

(define-db-col-type 'integer "0"
  number->string
  string->number)

(define-db-col-type 'bigint "0"
  number->string
  string->number)

(define-db-col-type 'int "0"
  number->string
  string->number)

(define-db-col-type 'int2 "0"
  number->string
  string->number)

(define-db-col-type 'int4 "0"
  number->string
  string->number)

(define-db-col-type 'int8 "0"
  number->string
  string->number)

;; -- arbitrary precision numbers

(define-db-col-type 'numeric "0"
  number->string
  string->number)

(define-db-col-type 'decimal "0"
  number->string
  string->number)

;; -- floating-point types

(define-db-col-type 'real "0.0"
  number->string
  string->number)

(define-db-col-type 'double "0.0"
  ;; NOTE: PostgreSQL uses the name: "double precision"
  number->string
  string->number)

(define-db-col-type 'float4 "0.0"
  number->string
  string->number)

(define-db-col-type 'float8 "0.0"
  number->string
  string->number)

;; -- serial types

(define-db-col-type 'serial "0"
  ;; This is a magic PostgreSQL type that actually causes the backend
  ;; to create a sequence ‘SEQ’ w/ the column typed ‘serial’ doing a
  ;; nextval('SEQ').  The sequence name is TABLENAME_COLNAME_seq,
  ;; where TABLENAME and COLNAME are not defined here.
  number->string
  string->number)

(define-db-col-type 'bigserial "0"
  number->string
  string->number)

(define-db-col-type 'serial4 "0"
  number->string
  string->number)

(define-db-col-type 'serial8 "0"
  number->string
  string->number)

;; -- monetary type
;; -- character types

(define-db-col-type 'varchar #f
  identity
  identity)

(define-db-col-type 'character #f
  identity
  identity)

(define-db-col-type 'char "?"
  (lambda (c) (make-string 1 c))
  (lambda (s) (string-ref s 0)))

(define-db-col-type 'text ""
  identity
  identity)

(define-db-col-type 'name "???"
  ;; NOTE: User's Guide sez "not intended for use by the general user"
  (lambda (val) (if (< 63 (string-length val)) (substring val 0 62) val))
  identity)

;; -- binary data types

(define-db-col-type 'bytea #f
  (lambda (s)
    (with-output-to-string
      (lambda ()
        (define (out! zeroes n)
          (display "\\")
          (display zeroes)
          (display (number->string n 8)))
        (let ((len (string-length s))
              (c #f) (n #f))
          (do ((i 0 (1+ i)))
              ((= len i))
            (set! c (string-ref s i))
            (set! n (char->integer c))
            (cond ((= 39 n)         (out! "0" n)) ; apostrophe
                  ((= 92 n)         (out! "" n))  ; backslash
                  ((<= 32 n 126)    (display c))
                  ((<= 0 n #o7)     (out! "00" n))
                  ((<= #o10 n #o77) (out! "0" n))
                  (else             (out! "" n))))))))
  (lambda (s)
    (if (and (<= 4 (string-length s))
             (char=? #\\ (string-ref s 0))
             (char=? #\x (string-ref s 1)))
        ;; Handle "hex format", introduced in PostgreSQL 9.0.
        (let* ((from-a (- (char->integer #\a) 10))
               (from-0 (char->integer #\0))
               (end (string-length s))
               (ans (make-string (ash (- end 2) -1) #\nul)))
          (define (n<- idx)
            (let ((c (string-ref s idx)))
              (- (char->integer c)
                 (case c
                   ((#\a #\b #\c #\d #\e #\f) from-a)
                   (else from-0)))))
          (do ((i 2 (+ 2 i))
               (o 0 (1+ o)))
              ((= end i))
            (string-set! ans o (integer->char
                                (logior (ash (n<- i) 4)
                                        (n<- (1+ i))))))
          ans)
        ;; Handle the traditional "escape format".
        (with-output-to-string
          (lambda ()
            (let ((len (string-length s))
                  (b #f))
              (let loop ((i 0))
                (set! b (string-index s #\\ i))
                (cond ((not b)
                       (display (substring s i)))
                      ((char=? #\\ (string-ref s (1+ b)))
                       (display (substring s i (1+ b)))
                       (loop (+ 2 b)))
                      (else
                       (display (substring s i b))
                       (display (integer->char
                                 (string->number
                                  (substring s (1+ b) (+ 4 b))
                                  8)))
                       (loop (+ 4 b)))))))))))

;; -- date/time types

(define-db-col-type 'timestamp "1970-01-01 00:00:00"
  (lambda (time)
    (cond ((string? time) time)
          ((number? time) (strftime "%Y-%m-%d %H:%M:%S" (localtime time)))
          (else (error "bad timestamp-type input:" time))))
  (lambda (string)
    (car (mktime (car (strptime "%Y-%m-%d %H:%M:%S" string))))))

;; -- date/time input
;; -- date/time output
;; -- time zones
;; -- internals
;; -- boolean type

(define-db-col-type 'boolean "f"
  (lambda (x) (if x "t" "f"))
  (lambda (s) (not (string=? "f" s))))

(define-db-col-type 'bool "f"
  (lambda (x) (if x "t" "f"))
  (lambda (s) (not (string=? "f" s))))

;; -- geometric types
;; -- points
;; -- line segments
;; -- boxes
;; -- paths
;; -- polygons
;; -- circles
;; -- network address types

(define (n+m-stringifier n+m)           ; n+m is #(NUMBER MASKCOUNT)
  (simple-format #f "~A/~A"
                 (inet-ntoa (vector-ref n+m 0))
                 (vector-ref n+m 1)))

(define (n+m-objectifier s)
  (let ((cut (string-index s #\/)))
    (if cut
        (vector (inet-aton (substring s 0 cut))
                (string->number (substring s (1+ cut))))
        (vector (inet-aton s) 32))))

;; -- inet

(define-db-col-type 'inet "0.0.0.0"
  n+m-stringifier
  n+m-objectifier)

;; -- cidr

(define-db-col-type 'cidr "0.0.0.0"
  n+m-stringifier
  n+m-objectifier)

(define (host-stringifier n)
  (simple-format #f "~A/32" (inet-ntoa n)))

(define (host-objectifier s)
  (vector-ref (n+m-objectifier s) 0))

(define-db-col-type 'inet-host "127.0.0.1"
  host-stringifier
  host-objectifier)

(define-db-col-type 'macaddr "00:00:00:00:00:00"
  (lambda (n)
    (let loop ((bpos 0) (acc '()) (n n))
      (if (= bpos 48)
          (apply simple-format #f "~A:~A:~A:~A:~A:~A"
                 (map (lambda (x) (number->string x 16)) acc))
          (loop (+ bpos 8)
                (cons (logand #xff n) acc)
                (ash n -8)))))
  (lambda (s)
    (let loop ((cut 2) (acc '()) (shift 40))
      (if (> 0 shift)
          (apply + acc)
          (loop (+ 3 cut)
                (cons (ash (string->number (substring s (- cut 2) cut) 16)
                           shift)
                      acc)
                (- shift 8))))))

;; -- inet vs cidr
;; -- macaddr
;; -- bit string types
;; -- arrays
;; -- object identifier types

(define-db-col-type 'oid "-1"
  number->string
  string->number)

(define-db-col-type 'aclitem "?"
  identity
  identity)

;; -- pseudo-types

;; array variants

(define-db-col-type-array-variant 'text[]   'text double-quote identity)
(define-db-col-type-array-variant 'text[][] 'text double-quote identity)
(define-db-col-type-array-variant 'int4[]   'int4)
(define-db-col-type-array-variant 'aclitem[] 'aclitem double-quote identity)

;;; postgres-types.scm ends here
