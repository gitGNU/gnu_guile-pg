;;; postgres-gxrepl.scm --- like psql(1) but still evolving

;;	Copyright (C) 2005 Thien-Thi Nguyen
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

;; This module exports the procedure:
;;   (gxrepl conn)
;;
;; The "gx" stands for "guile extensible".  That is not the case at the
;; moment, but we have great and humble plans for this module...

;;; Code:

(define-module (database postgres-gxrepl)
  #:use-module ((ice-9 rdelim)
                #:select (read-line
                          write-line))
  #:use-module ((database postgres)
                #:select (pg-connection?
                          pg-get-db
                          pg-exec
                          pg-result?
                          pg-result-status
                          pg-error-message
                          pg-connectdb
                          pg-print))
  #:use-module ((database postgres-qcons)
                #:select (sql-quote
                          make-SELECT/FROM/OUT-tree
                          make-WHERE-tree
                          make-ORDER-BY-tree
                          sql-command<-trees))
  #:export (gxrepl))

(define *comma-commands* '())

(define-macro (defcc formals . body)
  `(set! *comma-commands*
         (assq-set! *comma-commands*
                    ',(car formals)
                    (lambda ,(cdr formals)
                      ,@body))))

(define (fs s . args)
  (apply simple-format #f s args))

(define (for-display s)
  (set-object-property! s #:display #t)
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
                        (fs "~A\t-- ~A\n"
                            (car ent)
                            (get-doc (cdr ent)
                                     (lambda (doc)
                                       (cond ((string-index doc #\nl)
                                              => (lambda (cut)
                                                   (substring doc 0 cut)))
                                             (else doc))))))
                      (reverse *comma-commands*))))
         ((assq-ref *comma-commands* (car command))
          => (lambda (proc)
               (get-doc proc identity)))
         (else
          "no such command"))))

(defcc (dt table-name)
  "Describe columns in table TABLE-NAME.
Output includes the name, type, length, mod (?), and other information
extracted from system tables `pg_class', `pg_attribute' and `pg_type'."
  (sql-command<-trees
   (make-SELECT/FROM/OUT-tree
    '((c . pg_class) (a . pg_attribute) (t . pg_type))
    '(("name"     . a.attname)
      ("type"     . t.typname)
      ("bytes"    . a.attlen)
      ("mod??"    . a.atttypmod)
      ("not null" . a.attnotnull)
      ("hasdefs"  . a.atthasdef)))
   (make-WHERE-tree
    `(and (= c.relname ,(sql-quote table-name))
          (> a.attnum 0)
          (= a.attrelid c.oid)
          (= a.atttypid t.oid)))
   (make-ORDER-BY-tree
    '((< a.attnum)))))

;; Run a read-eval-print loop, talking to database @var{conn}.
;; @var{conn} may be a string naming a database, a string with
;; @code{var=val} options suitable for passing to @code{pg-connectdb},
;; or a connection object that satisfies @code{pg-connection?}.
;;
;; The repl accepts two kinds of commands:
;;
;; @itemize
;; @item SQL statements such as "CREATE TABLE" or "SELECT" are
;; executed using @code{pg-exec}.
;;
;; @item @dfn{Comma commands} are short commands beginning with
;; a comma (the most important being ",help") that do various
;; meta-repl or prepackaged operations.
;; @end itemize
;;
;; To exit the repl, send an EOF or use the ",q" command.
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
            (cond ((and (= 0 len) (string=? "" so-far))
                   (loop so-far prompt))
                  ((= 0 len)
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
             ((and (string? source) (not (object-property source #:display)))
              (and (conn-get #:gxrepl-echo)
                   (write-line source))
              (pg-exec conn source))
             (else
              source)))

     (define (-print res)
       (cond ((pg-result? res)
              (let ((status (pg-result-status res)))
                (display status) (newline)
                (let ((msg (pg-error-message res)))
                  (or (string=? "" msg)
                      (begin (display msg) (newline))))
                (and (eq? 'PGRES_TUPLES_OK status)
                     (pg-print res))))
             ((and (string? res) (object-property res #:display))
              (display res)
              (newline))
             (else
              (write res) (newline))))

     (defcc (q . ignored) "Quit the repl." (return #t))

     ;; do it!
     (repl -read -eval -print)))

  (set! conn #f)
  (gc)
  (if #f #f))

;;; Local Variables:
;;; eval: (font-lock-add-keywords nil '("defcc"))
;;; End:

;;; postgres-gxrepl.scm ends here
