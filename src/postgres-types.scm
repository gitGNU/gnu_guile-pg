;;; postgres-types.scm --- convert PostgreSQL <-> Scheme objects

;; Copyright (C) 2002-2009, 2011, 2012, 2014, 2015 Thien-Thi Nguyen
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

;;; Code:

(define-module (database postgres-types)
  #:export (oid-type-name-cache
            dbcoltypes
            dbcoltype-lookup
            dbcoltype:stringifier
            dbcoltype:default
            dbcoltype:objectifier
            type-registered?
            type-stringifier
            type-default
            type-objectifier
            type-sql-name
            define-db-col-type
            register-array-variant
            define-db-col-type-array-variant)
  #:autoload (database postgres) (pg-exec))

(define (fs s . args)
  (apply simple-format #f s args))

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

(define ALL
  ;; key: NAME (symbol)
  ;; val: #(STRINGIFIER DEFAULT OBJECTIFIER)
  (make-hash-table))

(define ARRAY-VARIANT-INFO
  ;; key: NAME (symbol)
  ;; val: #(SQL-NAME RANK SIMPLE CANONICAL-NAME)
  (make-hash-table))

(define (av-lookup symbol)
  (hashq-ref ARRAY-VARIANT-INFO symbol))

(define (make-array-variant-info sql-name rank simple canonical-name)
  (vector sql-name rank simple canonical-name))

(define (av-sql-name  v) (vector-ref v 0))
(define (av-rank      v) (vector-ref v 1))
(define (av-simple    v) (vector-ref v 2))
(define (av-canonical v) (vector-ref v 3))

;; @zonkable{2015-12-31,procedure}
;;
;; Return names of all registered converters.
;;
(define (dbcoltypes)
  (hash-fold (lambda (key val ls)
               (cons key ls))
             '() ALL))

;; @zonkable{2015-12-31,procedure}
;;
;; Return a converter object given its @var{type-name}, a symbol.
;; Return @code{#f} if no such object by that name exists.
;;
(define (dbcoltype-lookup type-name)
  (hashq-ref ALL (cond ((av-lookup type-name) => av-canonical)
                       (else                     type-name))))

;; @zonkable{2015-12-31,procedure}
;;
;; Extract stringifier from the converter object @var{tc}.
;;
(define (dbcoltype:stringifier tc) (vector-ref tc 0))

;; @zonkable{2015-12-31,procedure}
;;
;; Extract default string from the converter object @var{tc}.
;;
(define (dbcoltype:default tc) (vector-ref tc 1))

;; @zonkable{2015-12-31,procedure}
;;
;; Extract objectifier from the converter object @var{tc}.
;;
(define (dbcoltype:objectifier tc) (vector-ref tc 2))

;; Return @code{#t} if @var{type} (a symbol) has registered converters.
;;
(define (type-registered? type)
  (->bool (hashq-get-handle ALL type)))

;; Return the stringifier for @var{type} (a symbol).
;;
(define (type-stringifier type)
  (dbcoltype:stringifier (dbcoltype-lookup type)))

;; Return the default for @var{type} (a symbol).
;;
(define (type-default type)
  (dbcoltype:default (dbcoltype-lookup type)))

;; Return the objectifier for @var{type} (a symbol).
;;
(define (type-objectifier type)
  (dbcoltype:objectifier (dbcoltype-lookup type)))

;; Return the SQL name (a string) of @var{type} (a symbol).
;;
(define (type-sql-name type)
  (cond ((av-lookup type) => av-sql-name)
        (else                (symbol->string type))))

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
  (hashq-set! ALL name
              (vector stringifier default objectifier)))

;; Register an array type of @var{rank} dimensions based on @var{simple}.
;; @var{rank} is a (typically small) positive integer.
;; @var{simple} is a type name already
;; registered using @code{define-db-col-type}.
;;
;; By default, the associated stringifier and objectifier are
;; those of @var{simple}.  If @var{stringifier} and @var{objectifier}
;; are specified and non-@code{#f}, Guile-PG uses them instead.
;;
;; The default value of all array types is @samp{@{@}} and cannot be changed.
;;
;; Return the name (a symbol) of the array type.  This is basically
;; @var{rank} asterisks followed immediately by @var{simple}.  For example,
;; if @var{rank} is 2 and and @var{simple} is @code{int4}, the name would
;; be @code{**int4}.
;;
;;-args: (- 2 0 stringifier objectifier)
;;
(define (register-array-variant rank simple . procs)

  (define (array-variant-name)
    (string-append (make-string rank #\*)
                   (symbol->string simple)))

  (define (sql-name)
    (apply string-append
           (symbol->string simple)
           (make-list rank "[]")))

  (or (type-registered? simple)
      (error "unregistered type:" simple))
  (let* ((stringifier (and (not (null? procs))
                           (car procs)))
         (objectifier (and (not (null? procs))
                           (not (null? (cdr procs)))
                           (cadr procs)))
         (name (string->symbol (array-variant-name))))

    (hashq-set! ARRAY-VARIANT-INFO name
                (make-array-variant-info
                 (sql-name) rank simple name))

    (define-db-col-type name "{}"
      (dimension->string-proc (or stringifier (type-stringifier simple)))
      (read-array-string-proc (or objectifier (type-objectifier simple))))

    ;; rv
    name))

;; @zonkable{2015-12-31,procedure}
;;
;; Register type @var{composed} (string or symbol)
;; as an array variant of @var{simple}.
;; @var{simple} should be a type name already
;; registered using @code{define-db-col-type}.  @var{composed} @strong{must}
;; be formed by appending @var{simple} with one or more pairs of
;; @samp{[]} (square braces), with the number of pairs indicating the array
;; dimensionality.  For example, if @var{simple} is @code{text}, a
;; two-dimensional text array would be named @code{text[][]}.
;;
;; By default, the stringifier and objectifier for @var{composed} are
;; those of @var{simple}.  If @var{stringifier} and @var{objectifier}
;; are specified and non-@code{#f}, Guile-PG uses them instead.
;;
;; The default value of all array types is @samp{@{@}} and cannot be changed.
;;
;;-args: (- 2 0 stringifier objectifier)
;;
(define (define-db-col-type-array-variant composed simple . procs)
  (and (string? composed)
       (set! composed (string->symbol composed)))
  (let* ((sql-name (symbol->string composed))
         (neck (or (string-index sql-name #\[)
                   (error "no ‘[]’ in composed:" composed)))
         (rank (ash (- (string-length sql-name)
                       neck)
                    -1)))
    ;; sanity checks
    (or (eq? (string->symbol (substring sql-name 0 neck))
             simple)
        (error (fs "composed ‘~A’ not based on simple ‘~A’"
                   composed simple)))
    (or (positive? rank)
        (error "malformed:" composed))
    ;; do it!
    (let ((name (apply register-array-variant rank simple procs)))
      (define (add-ref ht)
        (hashq-set! ht composed (hashq-ref ht name)))
      (add-ref ALL)
      (add-ref ARRAY-VARIANT-INFO))))

;;;---------------------------------------------------------------------------
;;; load-time actions: set up built-ins

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
  (fs "~A/~A"
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
  (fs "~A/32" (inet-ntoa n)))

(define (host-objectifier s)
  (vector-ref (n+m-objectifier s) 0))

(define-db-col-type 'inet-host "127.0.0.1"
  host-stringifier
  host-objectifier)

(define-db-col-type 'macaddr "00:00:00:00:00:00"
  (lambda (n)
    (let loop ((bpos 0) (acc '()) (n n))
      (if (= bpos 48)
          (apply fs "~A:~A:~A:~A:~A:~A"
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

(define-db-col-type-array-variant "text[]"    'text double-quote identity)
(define-db-col-type-array-variant "text[][]"  'text double-quote identity)
(define-db-col-type-array-variant "int4[]"    'int4)
(define-db-col-type-array-variant "aclitem[]" 'aclitem double-quote identity)

;;; postgres-types.scm ends here
