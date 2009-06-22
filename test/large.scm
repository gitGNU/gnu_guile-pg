;;; large.scm

;; Copyright (C) 2002, 2003, 2004, 2005,
;;   2006, 2007, 2008, 2009 Thien-Thi Nguyen
;; Portions Copyright (C) 1999, 2000 Ian Grant
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

(or (not *all-tests*)
    (begin (display "Bad env.")
           (newline)
           (exit #f)))

(reset-all-tests!)

(use-modules (ice-9 rdelim) (database postgres))

(fresh!)

;; We use one connection for all the tests.

(define *C* #f)

;; We also want to keep track of the OID
(define *O* #f)

;; Define some basic procedures

;; cexec SQL
;; Execute SQL command on `*C*', return #t if OK, #f otherwise.
(define (cexec sql)
  (let ((res (pg-exec *C* sql)))
    (and res (eq? 'PGRES_COMMAND_OK (pg-result-status res)))))

;; transaction THUNK
;; Expand into an expression that returns the result of evaluating BODY forms
;; within a PostgreSQL transaction, or #f in the event of an error.
(define-macro (transaction . body)
  `(and (cexec "BEGIN TRANSACTION")
        (let* ((res-apply ((lambda () ,@body)))
               (res-end (cexec "END TRANSACTION")))
          (and res-end res-apply))))

;; Here we define procedures to carry out the tests.

(define test:make-connection
  (add-test #t
    (lambda ()
      (->bool (false-if-exception (set! *C* (pg-connectdb "")))))))

(define test:make-table
  (add-test #t
    (lambda ()
      (cexec "CREATE TABLE test (col1 int4, col2 oid)"))))

(define some-test-data "
;; This is a data file for use by guile-pg-lo-tests.scm;
;;   it will be imported to a large object and then
;;   `read' directly from the large object.

   (testing testing one two three)

;; End of test data")

(define test:make-data
  (add-test #t
    (lambda ()
      (let ((p (open-output-file "lo-tests-data-1")))
        (display some-test-data p)
        (close-port p)))))

(define test:lo-import
  (add-test #t
    (lambda ()
      (and (transaction
            (set! *O* (pg-lo-import *C* "lo-tests-data-1")))
           (->bool *O*)))))

(define test:lo-export
  (add-test #t
    (lambda ()
      (false-if-exception (delete-file "lo-tests-data-2"))
      (transaction
       (and (pg-lo-export *C* *O* "lo-tests-data-2")
            ;; poor man's cmp(1)
            (let* ((expected some-test-data)
                   (len (string-length expected))
                   (p (open-input-file "lo-tests-data-2"))
                   (next (lambda () (read-char p))))
              (let loop ((c (next)) (i 0))
                (cond ((eof-object? c)
                       (close-port p)
                       (= i len))       ; exact match required
                      ((= i len)
                       (close-port p)
                       #f)              ; too much
                      (else
                       (and (char=? c (string-ref expected i))
                            (loop (next) (1+ i))))))))))))

(define test:lo-open-read
  (add-test #t
    (lambda ()
      (transaction
       (let ((lo-port (pg-lo-open *C* *O* "r")))
         (and lo-port
              (eq? *C* (pg-lo-get-connection lo-port))
              (equal? (read lo-port) '(testing testing one two three))
              (eof-object? (read lo-port))
              (close-port lo-port)))))))

(define nchars 100)
(define *N* #f)                         ; new oid

(define (write-chars n c lop)
  (do ((i 0 (1+ i)))
      ((= i n) #t)
    (write-char c lop)))

(define test:lo-creat
  (add-test #t
    (lambda ()
      (transaction
       (let ((lo-port (pg-lo-creat *C* "w")))
         (and lo-port
              (write-chars nchars #\a lo-port)
              (set! *N* (pg-lo-get-oid lo-port))
              (close-port lo-port)))))))

(define test:lo-read
  (add-test #t
    (lambda ()
      (transaction
       (let ((lo-port (pg-lo-open *C* *N* "r"))
             (data #f))
         (and lo-port
              (set! data (pg-lo-read 1 nchars lo-port))
              (close-port lo-port)
              (equal? data (make-string nchars #\a))))))))

(define test:read-line
  (add-test #t
    (lambda ()
      (transaction
       (let ((lo-port (pg-lo-open *C* *N* "r"))
             (data #f))
         (and lo-port
              (set! data (read-line lo-port))
              (close-port lo-port)
              (equal? data (make-string nchars #\a))))))))

(define test:lo-seek
  (add-test #t
    (lambda ()
      (transaction
       (let* ((trace-port (open-output-file "test-lo-seek.log"))
              (lo-port (pg-lo-open *C* *N* "w")))
         (and (pg-trace *C* trace-port)
              lo-port
              (eq? (pg-lo-seek lo-port 1 SEEK_SET) 1)
              (write-char #\b lo-port)
              (force-output lo-port)
              (eq? (pg-lo-seek lo-port 3 SEEK_SET) 3)
              (write-char #\b lo-port)
              (force-output lo-port)
              (eq? (pg-lo-seek lo-port 0 SEEK_SET) 0)
              (close-port lo-port)
              (pg-untrace *C*)
              (close-port trace-port)))))))

;; Test pg-lo-seek pg-lo-read
;; expect "ababaa"

(define test:lo-seek2
  (add-test "ababaa"
    (lambda ()
      (transaction
       (let ((lo-port (pg-lo-open *C* *N* "r"))
             (data #f))
         (and lo-port
              (set! data (pg-lo-read 1 6 lo-port))
              (close-port lo-port)
              data))))))

(define test:lo-tell
  (add-test #t
    (lambda ()
      (transaction
       (let ((lo-port (pg-lo-open *C* *N* "r")))
         (and lo-port
              (eq? 0 (pg-lo-tell lo-port))
              (let ((data (pg-lo-read 1 1 lo-port)))
                (and (string? data)
                     (= 1 (string-length data))
                     (string=? "a" data)))
              (let ((location-after-read (pg-lo-tell lo-port)))
                (eq? 1 (pg-lo-seek lo-port 0 SEEK_CUR))
                (close-port lo-port)
                (eq? 1 location-after-read))))))))

(define test:lo-unlink
  (add-test #t
    (lambda ()
      (transaction
       (pg-lo-unlink *C* *N*)))))       ; todo: check access after unlink

(define (main)
  (set! verbose #t)
  (test-init "large" 13)
  (test! test:make-connection
         test:make-data
         test:make-table
         test:lo-import
         test:lo-export
         test:lo-open-read
         test:lo-creat
         test:lo-read
         test:read-line
         test:lo-seek
         test:lo-seek2
         test:lo-tell
         test:lo-unlink)
  (for-each delete-file '("lo-tests-data-1" "lo-tests-data-2"))
  (pg-finish *C*)
  (set! *C* #f)
  (drop!)
  (test-report))

(exit (main))

;;; large.scm ends here
