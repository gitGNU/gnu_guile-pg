#!/bin/sh
#-*- scheme -*-
exec guile -e shell-psql -s $0 "$1"
!#

;;; ID: $Id$
;;; Copyright (C) 1999-2001 Thien-Thi Nguyen
;;; This program is part of ttn-do, released under GNU GPL v2 with ABSOLUTELY
;;; NO WARRANTY.  See http://www.gnu.org/copyleft/gpl.txt for details.

;;; Commentary:

;; Usage: psql DBNAME
;;
;; This is a PSQL work-alike program.  The prompt is set to "<DBNAME>".

;;; Code:

(use-modules (ice-9 rdelim) (database postgres))
(use-modules ((ttn pgtable) :select (tuples-result->table display-table)))

;;;---------------------------------------------------------------------------
;;; Variables

(define psql-debug (getenv "PSQL_DEBUG"))

(define psql-repl-prompt "<psql> ")

;;;---------------------------------------------------------------------------
;;; Support

(define (psql-repl conn)
  (call-with-current-continuation
   (lambda (return)
     (let ((-read
            (lambda ignored-args
              (let loop ((so-far "")
                         (prompt (cond ((string? psql-repl-prompt)
                                        psql-repl-prompt)
                                       ((thunk? psql-repl-prompt)
                                        (psql-repl-prompt))
                                       (psql-repl-prompt "> ")
                                       (else ""))))
                (display prompt)
                (force-output)
                (let* ((input (read-line))
                       (line (string-append so-far
                                            (if (eof-object? input)
                                                (return #t)
                                                input)))
                       (len (string-length line)))
                  (if (and (> len 0)
                           (string=? (substring line (1- len) len) ";"))
                      line
                      (loop (string-append line " ") "... "))))))
           (-eval
            (lambda (source)
              (and (eof-object? source)
                   (return #t))
              (pg-exec conn source)))
           (-print
            (lambda (res)
              (let ((status (pg-result-status res)))
                (display status) (newline)
                (let ((msg (pg-error-message res)))
                  (or (string=? "" msg)
                      (begin (display msg) (newline))))
                (case status
                  ((PGRES_TUPLES_OK)
                   (display-table (tuples-result->table res)))
                  ;; Add other stuff here.
                  )))))
       (repl -read -eval -print)))))

;;;---------------------------------------------------------------------------
;;; Entry points

(define (set-psql-prompt! new-prompt)
  "Set the PSQL prompt to NEW-PROMPT, which can be a string, a thunk
returning a string, #f (no prompt), or anything else (use \"> \")."
  (set! psql-repl-prompt new-prompt))

(define (psql dbname)
  "Connect to postgresql database DBNAME, and enter interactive repl.
Commands should end w/ \";\" and are executed by `pg-exec'.
Type EOF to finish."
  (let ((conn (pg-connectdb (string-append "dbname=" dbname))))
    (and (not conn)
         (begin
           (display (pg-error-message conn)) (newline)
           (error "Connection failed.")))
    (set-psql-prompt! (string-append "<" dbname "> "))
    ((if psql-debug id false-if-exception)
     (psql-repl conn))
    (display "Closing connection...")
    (display "done.") (newline)))

(define (shell-psql cmd-line)
  (psql (cadr cmd-line)))

;;; psql ends here
