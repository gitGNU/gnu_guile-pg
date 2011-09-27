;;; postgres-gxrepl.scm --- like psql(1) but still evolving

;; Copyright (C) 2005, 2006, 2008, 2009, 2011 Thien-Thi Nguyen
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
;; along with Guile-PG; if not, write to the Free Software Foundation,
;; Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

;;; Commentary:

;; This module exports the procedure:
;;   (gxrepl conn)
;;
;; The "gx" stands for "guile extensible".  That is not the case at the
;; moment, but we have great and humble plans for this module...

;;; Code:

(define-module (database postgres-gxrepl)
  #:export (gxrepl)
  #:use-module ((ice-9 rdelim)
                #:select (read-line
                          write-line))
  #:use-module ((database postgres)
                #:select (pg-connection?
                          pg-finish
                          pg-get-db
                          pg-exec
                          pg-result?
                          pg-result-status
                          pg-error-message
                          pg-result-error-message
                          pg-connectdb
                          pg-make-print-options
                          pg-print))
  #:use-module ((database postgres-qcons)
                #:select (parse+make-SELECT-tree
                          sql-command<-trees))
  #:autoload (ice-9 pretty-print) (pretty-print))

(define *comma-commands* '())

(define-macro (defcc formals . body)
  `(set! *comma-commands*
         (assq-set! *comma-commands*
                    ',(car formals)
                    (lambda ,(cdr formals)
                      ,@body))))

(define (fs s . args)
  (apply simple-format #f s args))

(define --for-display (make-object-property))

(define (for-display s)
  (set! (--for-display s) #t)
  s)

(defcc (help . command)
  "Display a list of commands, or full help for COMMAND if specified."
  (define (get-doc proc yes)
    (cond ((procedure-documentation proc) => yes)
          (else "(no docs)")))
  (for-display
   (cond ((null? command)
          (apply string-append
                 (map (lambda (ent)
                        (fs "~A~A-- ~A~A"
                            (car ent)
                            #\ht
                            (get-doc (cdr ent)
                                     (lambda (doc)
                                       (cond ((string-index doc #\nl)
                                              => (lambda (cut)
                                                   (substring doc 0 cut)))
                                             (else doc))))
                            #\newline))
                      (reverse *comma-commands*))))
         ((assq-ref *comma-commands* (car command))
          => (lambda (proc)
               (get-doc proc identity)))
         (else
          "no such command"))))

(define (sqlsel cols . rest)
  (sql-command<-trees (apply parse+make-SELECT-tree #t cols rest)))

(defcc (obvious . something)
  "Life, the universe, and everything!
Optional arg CC names a comma-command to make obvious (pretty-print its
source).  Note that display of `(A . (B C))' shows up as `(A B C)'; that
is normal."
  (cond ((and (not (null? something))
              (assq-ref *comma-commands* (car something)))
         => (lambda (proc)
              (for-display
               (with-output-to-string
                 (lambda ()
                   (pretty-print (let ((all (procedure-source proc)))
                                   (cons* (car all)
                                          (cadr all)
                                          ;; skip docstring
                                          (cdddr all)))))))))
        (else
         (sqlsel `((,(if (null? something)
                         "obvious"
                         (fs "~A" (car something)))
                    . (+ 6 (* 6 6))))))))

(defcc (dt . which)
  "Describe columns in table TABLE-NAME.
Output includes the name, type, length, mod (?), and other information
extracted from system tables `pg_class', `pg_attribute' and `pg_type'."
  (if (null? which)
      (sqlsel '(("schema" . n.nspname)
                ("name"   . c.relname)
                ("type"   . (case c.relkind
                              ("r" "table")
                              ("v" "view")
                              ("i" "index")
                              ("S" "sequence")
                              ("s" "special")
                              (else "huh?")))
                ("owner"  . u.usename))
              #:from
              '((#:left-join (#:on (= n.oid c.relnamespace))
                             (#:left-join (#:on (= u.usesysid c.relowner))
                                          (c . pg_catalog.pg_class)
                                          (u . pg_catalog.pg_user))
                             (n . pg_catalog.pg_namespace)))
              #:where
              '(and (not (= n.nspname "pg_catalog"))
                    (not (= n.nspname "pg_toast")))
              #:order-by
              '((< 1) (< 2)))
      (sqlsel '(("name"   . a.attname)
                ("type"   . t.typname)
                (" bytes" . (if (< 0 a.attlen)
                                (to_char a.attlen "99999")
                                "varies"))
                ("mod"    . (to_char a.atttypmod "999"))
                ("etc"    . (|| (if a.attnotnull
                                    "NOT NULL"
                                    "NULL ok")
                                ", "
                                (if a.atthasdef
                                    "has defs"
                                    "no defs"))))
              #:from
              '((c . pg_class) (a . pg_attribute) (t . pg_type))
              #:where
              `(and (= c.relname ,(symbol->string (car which)))
                    (> a.attnum 0)
                    (= a.attrelid c.oid)
                    (= a.atttypid t.oid))
              #:order-by
              '((< a.attnum)))))

(defcc (gxrepl conn)
  "Run a recursive repl, talking to database CONN.
Exiting from the recursive repl returns to this one."
  (gxrepl (symbol->string conn))
  (for-display "\nExiting recursive repl."))

;; Run a read-eval-print loop, talking to database @var{conn}.
;; @var{conn} may be a string naming a database, a string with
;; @code{var=val} options suitable for passing to @code{pg-connectdb},
;; or a connection object that satisfies @code{pg-connection?}.
;;
;; The repl accepts two kinds of commands:
;;
;; @itemize
;; @item SQL statements such as @code{CREATE TABLE} or
;; @code{SELECT} are executed using @code{pg-exec}.
;;
;; @item @dfn{Comma commands} are short commands beginning with
;; a comma (the most important being @samp{,help}) that do various
;; meta-repl or prepackaged operations.
;; @end itemize
;;
;; Sending an EOF exits the repl.
;;
(define (gxrepl conn)

  (define (conn-get prop)
    (object-property conn prop))

  (define (conn-put prop value)
    (set-object-property! conn prop value))

  (define (make-prompt style)
    (case style
      ((#:ellipse) "... ")
      ((#:conn) (fs "(~A) " (pg-get-db conn)))))

  (defcc (conn . db-name)
    "Connect to DB-NAME, or reconnect if not specified."
    (set! db-name (if (null? db-name)
                      (pg-get-db conn)
                      (car db-name)))
    (for-display
     (fs "~Aonnecting to: ~A ... ~A."
         (if (equal? db-name (pg-get-db conn)) "Rec" "C")
         db-name
         (cond ((pg-connectdb (string-append "dbname=" db-name))
                => (lambda (good)
                     (set! conn good)
                     "OK"))
               (else "FAILED")))))

  (defcc (echo . setting)
    "Toggle echoing of SQL command prior to sending to `pg-exec'.
Optional arg SETTING turns on echoing if a positive number."
    (let ((new (cond ((null? setting)
                      (not (conn-get #:gxrepl-echo)))
                     ((number? (car setting))
                      (positive? (car setting)))
                     (else #f))))
      (conn-put #:gxrepl-echo new)
      (for-display (fs "SQL command echoing now ~A." (if new "ON" "OFF")))))

  (defcc (fix part . set)
    "Fix query PART to be SET..., or clear that part if SET is 0 (zero).
When a part is fixed, it is used (unless overridden) in the \",fsel\"
comma-command.  PART is a keyword, one of #:cols, #:from, #:where,
#:where/combiner, #:group-by, #:having, #:order-by, #:limit or #:offset.
SET is a space-separated list of elements.

If PART is #:cols, each element of SET is either a column name, possibly
qualified by the table name with a dot, e.g., `t.oid'; a prefix-style
SQL expression, e.g., `(to_char 42 \"999\")'; or pair with the car a
string and the cdr one of the previous options, e.g., `(\"id\" . t.oid)'.

If PART is #:from, each element of SET is either the name of a table,
or a pair in the form (ALIAS . TABLE-NAME), e.g., `(t . pg_type)'.  The
alias can be used in `outs' and `where' sets.

If PART is #:where, each element of SET is prefix-style SQL expression,
.e.g, `(= t.oid 42)', and there is an implicit \"and\" clause surrounding
SET.  For \"or\" behavior, fix #:where/combiner to be `or'.

If PART is #:group-by or #:having, each element of SET is a
prefix-style SQL expression.

If PART is #:order-by, each element of SET is a list (ORDFUNC EXPR),
where ORDFUNC is either `<', '>' or the name of an SQL function that
takes two args and returns their ordering; and EXPR is a prefix-style
SQL expression.

If PART is #:limit or #:offset, the first element of SET specifies an integer.

If PART is `?' display all the parts and their related expressions.
If SET is omitted display the current value for PART."
    (define (new-val! check)
      (conn-put part (cond ((null? set) (conn-get part))
                           ((equal? 0 (car set)) #f)
                           ((check))
                           (else (conn-get part)))))
    (case part
      ((#:where/combiner)
       (new-val! (lambda () (and (memq (car set) '(and or))
                                 (car set)))))
      ((#:limit #:offset)
       (new-val! (lambda () (and (integer? (car set))
                                 (car set)))))
      ((#:cols #:from #:where #:group-by #:having #:order-by)
       (new-val! (lambda () set)))
      ((?)
       (for-display
        (let* ((all (list #:cols #:from #:where #:where/combiner
                          #:group-by #:having #:order-by #:limit #:offset))
               ;; do this here to avoid potential arg-eval-order issue...
               (set (map (lambda (part)
                           (cond ((conn-get part)
                                  => (lambda (v)
                                       ;; ...introduced by this side effect
                                       (set! all (delq! part all))
                                       (list part
                                             (make-string
                                              (- 16 (string-length
                                                     (symbol->string
                                                      (keyword->symbol
                                                       part))))
                                              #\space)
                                             (map (lambda (x)
                                                    (fs " ~S" x))
                                                  (if (pair? v)
                                                      v
                                                      (list v)))
                                             "\n")))
                                 (else '())))
                         all)))
          (list set (cons (if (null? all)
                              "(no unset parts)"
                              "unset:")
                          (map (lambda (part)
                                 (list " " part))
                               all))))))
      (else
       (for-display "No such part, try \",help fix\""))))

  (defcc (fsel . etc)
    "Do a \"select\", merging fixed elements and those from ETC.
ETC is a series of zero or more column expressions, optionally followed
by a series of keywords and related expressions.  These override those
specified by \",fix\".  Keywords without related expressions are ignored."
    (define (collect-keys ls)
      (let loop ((ls ls) (acc (list)))
        (if (null? ls)
            (reverse! acc)              ; rv
            (let ((kw (car ls)))
              (let collect-one ((ls (cdr ls)) (partial (list)))
                (if (or (null? ls) (keyword? (car ls)))
                    (loop ls (if (null? partial)
                                 acc
                                 (acons kw (reverse! partial) acc)))
                    (collect-one (cdr ls) (cons (car ls) partial))))))))
    (let ((fixed (map (lambda (part)
                        (cons part (conn-get part)))
                      '(#:limit #:offset #:cols #:from #:where
                                #:group-by #:having #:order-by)))
          (oride (collect-keys (cons #:cols etc))))
      (define (o/f part)
        (or (assq-ref oride part)
            (assq-ref fixed part)))
      (cond ((o/f #:cols)
             => (lambda (cols)
                  (define (decide part . massage)
                    (list part (and=> (o/f part)
                                      (if (null? massage)
                                          identity
                                          (car massage)))))
                  (apply
                   sqlsel cols
                   `(,@(decide #:from)
                     ,@(decide #:where
                               (lambda (v)
                                 (cons (or (conn-get #:where/combiner)
                                           'and)
                                       v)))
                     ,@(decide #:group-by)
                     ,@(decide #:having)
                     ,@(decide #:order-by)
                     ,@(decide #:limit)
                     ,@(decide #:offset)))))
            (else
             (for-display "No columns selected")))))

  ;; do it!
  (cond ((pg-connection? conn))
        ((and (string? conn) (not (string-index conn #\=)))
         (set! conn (pg-connectdb (fs "dbname=~A" conn))))
        ((string? conn)
         (set! conn (pg-connectdb conn)))
        (else
         (set! conn #f)))

  (cond ((and conn (pg-connection? conn)))
        (else (display (pg-error-message conn)) (newline)
              (error "connection failed")))

  (conn-put #:gxrepl-echo #f)
  (conn-put #:cols '(*))

  (call-with-current-continuation
   (lambda (return)

     (define (-read . ignored-args)
       (false-if-exception
        (let loop ((so-far "") (prompt (make-prompt #:conn)))
          (display prompt)
          (force-output)
          (let* ((input (let ((in (read-line)))
                          (if (eof-object? in)
                              (return #t)
                              in)))
                 (line (string-append so-far input))
                 (len (string-length line)))
            (cond ((and (zero? len) (string-null? so-far))
                   (loop so-far prompt))
                  ((zero? len)
                   (loop line (make-prompt #:ellipse)))
                  ((char=? #\, (string-ref line 0))
                   (let ((cmd (with-input-from-string
                                  (fs "(~A)" (substring line 1))
                                read)))
                     (cond ((assq-ref *comma-commands* (car cmd))
                            => (lambda (proc)
                                 (apply proc (cdr cmd))))
                           (else
                            (display (fs "~A (try ~S)\n"
                                         "unrecognized comma command"
                                         ",help"))
                            (loop "" psql-repl-prompt)))))
                  ((string=? (substring line (1- len) len) ";")
                   line)
                  (else
                   (loop (string-append line " ")
                         (make-prompt #:ellipse))))))))

     (define (-eval source)
       (cond ((eof-object? source)
              (return #t))
             ((and (string? source) (not (--for-display source)))
              (and (conn-get #:gxrepl-echo)
                   (write-line source))
              (pg-exec conn source))
             (else
              source)))

     (define (-print res)
       (cond ((pg-result? res)
              (let ((status (pg-result-status res)))
                (if (eq? 'PGRES_TUPLES_OK status)
                    (pg-print res (pg-make-print-options
                                   '((field-sep . " | "))))
                    (let ((msg (pg-result-error-message res)))
                (display status) (newline)
                  (or (string-null? msg)
                          (begin (display msg) (newline)))))))
             ((--for-display res)
              (letrec ((out (lambda (x)
                              (if (list? x)
                                  (for-each out x)
                                  (display x)))))
                (out res))
              (newline))
             (else
              (write res) (newline))))

     ;; do it!
     (repl -read -eval -print)))

  (pg-finish conn)
  (set! conn #f)
  (gc)
  (if #f #f))

;;; Local Variables:
;;; eval: (font-lock-add-keywords nil '("defcc"))
;;; End:

;;; postgres-gxrepl.scm ends here
