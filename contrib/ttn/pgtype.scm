;;; ttn/pgtype.scm --- Convert PostgreSQL <-> Scheme objects

;; $State$:$Name$
;;
;; Copyright (C) 2001-2002 Thien-Thi Nguyen
;; This file is part of ttn's personal scheme library, released under GNU
;; GPL with ABSOLUTELY NO WARRANTY.  See the file COPYING for details.

;;; Commentary:

;; Some of this used to be in pgtable.scm.

;; NOTE: A large number of "native" PostgreSQL types are not yet implemented.
;; These can be added easily by the user, however.  See `define-db-col-type'
;; and `define-db-col-type-array-variant' for more information.
;;
;; TODO: Look into "user-defined" types, from both PostgreSQL point of view
;;       and from (ttn pgtype) point of view.

;;; Code:

(define-module (ttn pgtype))

;; support

(define (double-quote s)
  (string-append "\"" s "\""))

;; column types / definition

(define *db-col-types* '())     ; (NAME STRINGIFIER DEFAULT OBJECTIFIER)

;; Return lookup value of TYPE-NAME, a symbol, from `*db-col-types*'.
;; Use procs `dbcoltype:name', `dbcoltype:default', `dbcoltype:stringifier'
;; and `dbcoltype:objectifier' on this value to access those components,
;; respectively.
;;
(define (dbcoltype-lookup type-name)
  (assq type-name *db-col-types*))
(define dbcoltype:name car)
(define dbcoltype:stringifier cadr)
(define dbcoltype:default caddr)
(define dbcoltype:objectifier cadddr)

(define (read-pgarray-1 objectifier port)
  ;; ugh, i hate parsing...  the right thing to do would be find out if
  ;; postgres supports (easily!)  parameterizable array output formatting.
  ;; then, we could just specify using "()" instead of "{}" and space instead
  ;; of comma, and this proc becomes a simple objectifier walk over `read'
  ;; output.  on the bright side, maybe this proc can go into official guile
  ;; distribution (w/ some tweaks).
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
;; The default value of all array types is "{}" and cannot be changed.
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

;; set up *db-col-types*

(define-db-col-type 'serial "0"
  ;; This is a magic PostgreSQL type that actually causes the backend
  ;; to create a sequence `SEQ' w/ the column typed `serial' doing a
  ;; nextval('SEQ').  The sequence name is TABLENAME_COLNAME_seq,
  ;; where TABLENAME and COLNAME are not defined here.  Arguably, this
  ;; simplistic stringification/objectification is insufficient.
  ;; See `drop-proc' above for more hair.
  (lambda (val) (number->string val))
  (lambda (string) (string->number string)))

(define-db-col-type 'bool "f"
  (lambda (val) (if val "t" "f"))
  (lambda (string) (not (string=? "f" string))))

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

;; array variants

(define-db-col-type-array-variant 'text[]   'text double-quote identity)
(define-db-col-type-array-variant 'text[][] 'text double-quote identity)
(define-db-col-type-array-variant 'int4[]   'int4)

;; export

(export double-quote

        *db-col-types*
        dbcoltype-lookup
        dbcoltype:name
        dbcoltype:stringifier
        dbcoltype:default
        dbcoltype:objectifier

        define-db-col-type
        define-db-col-type-array-variant)

;;; ttn/pgtype.scm ends here
