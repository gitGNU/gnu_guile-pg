;;; guile-pg-basic-tests.scm

;;	Copyright (C) 1999,2000 Ian Grant
;;	Copyright (C) 2002,2003,2004 Thien-Thi Nguyen
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

(load-from-path (in-vicinity *srcdir* "testing.scm"))
(reset-all-tests!)

(use-modules (database postgres))

;; We use one connection for all the tests.

(define *C* #f)

(define (cexec s)
  (pg-exec *C* s))

(define (result->output-string result)
  (let ((po (pg-make-print-options '(no-header))))
    (with-output-to-string
      (lambda ()
        (pg-print result po)))))

(define (ok? expected-status)
  (lambda (result)
    (and result (eq? expected-status (pg-result-status result)))))

(define tuples-ok? (let ((check (ok? 'PGRES_TUPLES_OK)))
                     (lambda (result)
                       (and (check result)
                            ;; this test never uses a binary cursor
                            (not (pg-binary-tuples? result))))))

(define command-ok? (ok? 'PGRES_COMMAND_OK))

(define (field-info access)
  (lambda (result)
    (map (lambda (fn)
           (access result fn))
         (iota (pg-nfields result)))))

(define field-names (field-info pg-fname))

(define (ftype-name result fnum)
  (let ((result (cexec (format #f "SELECT typname FROM pg_type WHERE oid = ~A"
                               (pg-ftype result fnum)))))
    (and (tuples-ok? result)
         (> (pg-ntuples result) 0)
         (pg-getvalue result 0 0))))

(define ftype-names (field-info ftype-name))

(define (run-cmd n sql-proc)
  (define (iter n)
    (let ((res (cexec (sql-proc n))))
      (if (eq? n 1)
          #t
          (if (and res (pg-result? res) (not (command-ok? res)))
              #f
              (iter (- n 1))))))
  (iter n))

(define (as-string obj)
  (cond ((string? obj)    obj)
        ((char? obj)      (make-string 1 obj))
        ((symbol? obj)    (symbol->string obj))
        ((number? obj)    (number->string obj))
        ;; this infinite loops -ttn
        ;; ((procedure? obj) (as-string (obj)))
        ((pair? obj) (interpolate-list "(" " " ")"
                                       (lambda (x) (as-string x))
                                       obj))
        (else (call-with-output-string (lambda (port) (write obj port))))))

(define (interpolate-list afore twixt aft proc lst)
  (define (iter lst)
    (let ((elt (car lst))
          (tail (cdr lst)))
      (string-append (proc elt)
                     (if (not (null? tail))
                         (string-append (as-string twixt) (iter tail))
                         (as-string aft)))))
  (string-append (as-string afore) (iter lst)))

(define (make-table! tablename fields)
  (cexec (string-append
          "CREATE TABLE " tablename " "
          (interpolate-list "(" ", " ")"
                            (lambda (p)
                              (string-append
                               (as-string (car p)) " "
                               (as-string (cdr p))))
                            fields))))

;; Here we define procedures to carry out the tests.

(define test:pg-guile-pg-loaded
  (add-test #t
    (lambda ()
      (and (defined? 'pg-guile-pg-loaded)
           (list? (pg-guile-pg-loaded))
           (and-map symbol? (pg-guile-pg-loaded))))))

(define test:pg-conndefaults
  (add-test #t
    (lambda ()
      (and pg-conndefaults
           (let ((val (pg-conndefaults)))
             (and val (let loop ((ls val))
                        (if (null? ls)
                            #t
                            (and (keyword? (caar ls))
                                 (let ((details (cdar ls)))
                                   (and (list? details)
                                        (let inner ((d details))
                                          (if (null? d)
                                              #t
                                              (and (keyword? (caar d))
                                                   (inner (cdr d)))))))
                                 (loop (cdr ls)))))))))))

(define test:make-connection
  (add-test #t
    (lambda ()
      (let ((new (false-if-exception (pg-connectdb ""))))
        (and (pg-connection? new)
             (begin (set! *C* new)
                    #t))))))

(define test:various-connection-info
  (add-test #t
    (lambda ()
      (let ((host (pg-get-host *C*))
            (pid (pg-backend-pid *C*))
            (enc (pg-client-encoding *C*)))
        (and (or (not host)
                 (and (string? host)
                      (not (string-null? host))))
             (integer? pid)
             (string? enc)
             (not (string-null? enc))
             (let ((new-enc "SQL_ASCII"))
               (and (pg-set-client-encoding! *C* new-enc)
                    (let ((check (pg-client-encoding *C*)))
                      (pg-set-client-encoding! *C* enc)
                      (and (string=? check new-enc)
                           (string=? enc (pg-client-encoding *C*)))))))))))

(define test:set-notice-out!-1
  (add-test #t
    (lambda ()
      (let ((n (call-with-output-string
                (lambda (port)
                  (pg-set-notice-out! *C* port)
                  (cexec "CREATE TABLE unused (ser serial, a int);")))))
        (and (string? n)
             (member n (map (lambda (output-format)
                              (string-append
                               "NOTICE:  CREATE TABLE will create"
                               " implicit sequence "
                               (format #f output-format
                                       "unused_ser_seq"
                                       "unused.ser")
                               "\n"))
                            '("'~A' for SERIAL column '~A'"
                              ;; postgresql 7.4.5
                              "~S for \"serial\" column ~S")))
             #t)))))

(define test:reset
  (add-test #t
    (lambda ()
      (pg-reset *C*)
      #t)))

(define test:set-notice-out!-2
  (add-test #t
    (lambda ()
      (let ((n (call-with-output-string
                (lambda (port)
                  (cexec "CREATE TABLE unused2 (ser serial, a int);")))))
        (and (string? n) (string-null? n))))))

(define test:set-client-data
  (add-test #t
    (lambda ()
      (equal? (pg-set-client-data! *C* '(elt1 elt2 elt3))
              '(elt1 elt2 elt3)))))

(define test:get-client-data
  (add-test #t
    (lambda ()
      (equal? (pg-get-client-data *C*) '(elt1 elt2 elt3)))))

(define test:make-table
  (add-test #t
    (lambda ()
      (command-ok?
       (make-table! "test" '(("col1" . "int4") ("col2" . "text")))))))

(define test:pg-error-message
  (add-test #t
    (lambda ()
      (let ((result (cexec "invalid--sql--command")))
        (and result
             (eq? 'PGRES_FATAL_ERROR (pg-result-status result))
             (let ((msg (pg-error-message *C*)))
               (and (string? msg)
                    (not (string-null? msg)))))))))

(define test:load-records
  (add-test #t
    (lambda ()
      (run-cmd 100
               (lambda (n)
                 (format #f "INSERT INTO test VALUES (~A, 'Column ~A')"
                         n n))))))

(define (count-records)
  (let* ((s (result->output-string
             (cexec "SELECT * FROM test WHERE col1 <= 100")))
         (line-count (let loop ((start 0) (count 0))
                       (cond ((string-index s #\newline start)
                              => (lambda (pos)
                                   (loop (1+ pos) (1+ count))))
                             (else count))))
         (res (cexec "SELECT COUNT(*) FROM test WHERE col1 <= 100")))
    (and (tuples-ok? res)
         (let ((backend-sez (string->number (pg-getvalue res 0 0))))
           (and (= line-count backend-sez)
                backend-sez)))))

(define test:count-records-expect-100
  (add-test 100 (lambda () (count-records))))

(define test:count-records-expect-50
  (add-test 50 (lambda () (count-records))))

(define (get-proc proc)
  (and (string? (proc *C*))
       (not (false-if-exception (proc #f)))))

(define test:get-proc:pg-get-db
  (add-test #t (lambda () (get-proc pg-get-db))))

(define test:get-proc:pg-get-user
  (add-test #t (lambda () (get-proc pg-get-user))))

(define test:get-proc:pg-get-pass
  (add-test #t (lambda () (get-proc pg-get-pass))))

(define test:get-proc:pg-get-tty
  (add-test #t (lambda () (get-proc pg-get-tty))))

(define test:get-proc:pg-get-port
  (add-test #t (lambda () (get-proc pg-get-port))))

(define test:get-proc:pg-get-options
  (add-test #t (lambda () (get-proc pg-get-options))))

(define test:delete-some-records
  (add-test 50
    (lambda ()
      (let ((res (cexec "DELETE FROM test WHERE col1 > 50")))
        (if (command-ok? res)
            (string->number (pg-cmdtuples res))
            #f)))))

(define test:fname
  (add-test '("col1" "col2")
    (lambda ()
      (let ((res (cexec "SELECT * FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (field-names res))))))

(define test:fmod+ftypes
  (add-test '("int4" "text")
    (lambda ()
      (let ((res (cexec "SELECT * FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (= -1 (pg-fmod res 0))     ; -1 means "no modification";
             (= -1 (pg-fmod res 1))     ;    these are simple types
             (ftype-names res))))))

(define test:get-connection
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT * FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (eq? *C* (pg-get-connection res)))))))

(define test:ntuples
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT col1 FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (eq? 1 (pg-ntuples res)))))))

(define test:nfields
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT col1 FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (eq? 1 (pg-nfields res)))))))

(define test:oid-value-was-oid-status
  (add-test #t
    (lambda ()
      (let ((res (cexec "INSERT INTO test VALUES (10000, 'Column 10000')"))
            (oid #f))
        (and (command-ok? res)
             (begin
               (set! oid (pg-oid-value res))
               (set! res (cexec "DELETE FROM test WHERE col1 = 10000"))
               (and (command-ok? res)
                    oid
                    (number? oid)
                    (> oid 0)
                    (eq? (string->number (pg-cmdtuples res)) 1))))))))

(define test:oid-value
  (add-test #t
    (lambda ()
      (let ((res (cexec "INSERT INTO test VALUES (10000, 'Column 10000')"))
            (oid #f))
        (and (command-ok? res)
             (begin
               (set! oid (pg-oid-value res))
               (set! res (cexec "DELETE FROM test WHERE col1 = 10000"))
               (and (command-ok? res)
                    oid
                    (number? oid)
                    (> oid 0)
                    (eq? (string->number (pg-cmdtuples res)) 1))))))))

(define test:fnumber
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT col1, col2 FROM test WHERE col1 <= 100")))
        (and (tuples-ok? res)
             (eq? (pg-fnumber res "col1") 0)
             (eq? (pg-fnumber res "col2") 1)
             (eq? (pg-fnumber res "invalid_column_name") -1))))))

(define test:getvalue
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT col1, col2 FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (string=? (pg-getvalue res 0 0) "1")
             (string=? (pg-getvalue res 0 1) "Column 1")
             (not (false-if-exception (pg-getvalue res 0 2)))
             (not (false-if-exception (pg-getvalue res -1 0))))))))

(define test:getisnull
  (add-test #t
    (lambda ()
      (let ((res (cexec "INSERT INTO test VALUES (10000)"))
            (res2 #f))
        (and (command-ok? res)
             (begin
               (set! res (cexec "SELECT * FROM test WHERE col1 = 10000"))
               (set! res2 (cexec "DELETE FROM test WHERE col1 = 10000"))
               (and (tuples-ok? res)
                    (not (pg-getisnull res 0 0))
                    (pg-getisnull res 0 1)
                    (not (false-if-exception (pg-getisnull res 0 2)))
                    (not (false-if-exception (pg-getisnull res -1 0)))
                    (command-ok? res2))))))))

(define test:fsize
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT col1, col2 FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (eq? (pg-fsize res 0) 4)
             (eq? (pg-fsize res 1) -1)
             (not (false-if-exception (pg-fsize res 2))))))))

(define test:getlength
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT col1, col2 FROM test WHERE col1 = 1")))
        (and (tuples-ok? res)
             (eq? (pg-getlength res 0 0) 1)
             (eq? (pg-getlength res 0 1) (string-length "Column 1"))
             (not (false-if-exception (pg-getlength res 0 2)))
             (not (false-if-exception (pg-getlength res 1 0))))))))

(define test:getline
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT * INTO test2 FROM test WHERE col1 = 1")))
        (and (command-ok? res)
             (let ((res (cexec "COPY test2 TO STDOUT")))
               (and ((ok? 'PGRES_COPY_OUT) res)
                    (let ((line (pg-getline *C*)))
                      (and (string=? (pg-getline *C*) "\\.")
                           (eq? #t (pg-endcopy *C*))
                           (string=? line "1\tColumn 1"))))))))))

(define test:getlineasync               ; needs test2
  (add-test #t
    (lambda ()
      (let ((res (cexec "COPY test2 TO STDOUT")))
        (and ((ok? 'PGRES_COPY_OUT) res)
             (let ((buf (make-string 8)))
               (let loop ((count (pg-getlineasync *C* buf)) (acc '()))
                 (if (< count 0)
                     (and (string=? "1\tColumn 1\n"
                                    (apply string-append
                                           (reverse acc)))
                          (eq? #t (pg-endcopy *C*)))
                     (let ((chunk (substring buf 0 count)))
                       (loop (pg-getlineasync *C* buf)
                             (cons chunk acc)))))))))))

(define test:putline
  (add-test #t
    (lambda ()
      (let ((res (cexec "COPY test2 FROM STDIN")))
        (and ((ok? 'PGRES_COPY_IN) res)
             (begin
               (for-each (lambda (s) (pg-putline *C* s))
                         '("2\tColumn 2" "\n" "\\." "\n"))
               (and (eq? #t (pg-endcopy *C*))
                    (let ((res (cexec "SELECT * FROM test2 WHERE col1 = 2")))
                      (and res (tuples-ok? res)
                           (string=? (pg-getvalue res 0 0) "2")
                           (string=? (pg-getvalue res 0 1) "Column 2"))))))))))

(define (main)
  (set! verbose #t)
  (test-init "basic-tests" 37)
  (test! test:pg-guile-pg-loaded
         test:pg-conndefaults
         test:make-connection
         test:various-connection-info
         test:set-notice-out!-1
         test:set-client-data
         test:make-table
         test:pg-error-message
         test:load-records
         test:count-records-expect-100
         test:reset
         test:set-notice-out!-2         ; must be after test:reset
         test:count-records-expect-100
         test:delete-some-records
         test:count-records-expect-50
         test:fname
         test:fmod+ftypes
         test:get-client-data
         test:get-connection
         test:ntuples
         test:nfields
         test:oid-value-was-oid-status
         test:oid-value
         test:fnumber
         test:getvalue
         test:getisnull
         test:fsize
         test:getlength
         test:getline
         test:getlineasync
         test:putline
         test:get-proc:pg-get-db
         test:get-proc:pg-get-user
         test:get-proc:pg-get-pass
         test:get-proc:pg-get-tty
         test:get-proc:pg-get-port
         test:get-proc:pg-get-options)
  (set! *C* #f)
  (test-report))

(exit (main))

;;; Local variables:
;;; eval: (put 'add-test 'scheme-indent-function 1)
;;; End:

;;; guile-pg-basic-tests.scm ends here
