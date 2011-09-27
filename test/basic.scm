;;; basic.scm

;; Copyright (C) 2002, 2003, 2004, 2005, 2006,
;;   2007, 2008, 2009, 2010, 2011 Thien-Thi Nguyen
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

(use-modules (database postgres))

(fresh!)

;; We use one connection for all the tests.

(define *C* #f)

(define (cexec s)
  (pg-exec *C* (string-append s ";")))

(define (result->output-string result)
  (let ((po (pg-make-print-options '(no-header))))
    (with-output-to-string
      (lambda ()
        (pg-print result po)))))

(define (ok? expected-status)
  (lambda (result)                      ; => #f unless All Goes Well
    (cond ((and result (string-null? (pg-result-error-message result))
                (eq? expected-status (pg-result-status result))))
          ((pg-result-error-field result #:sqlstate)
           => (lambda (s)
                (format #t "INFO: sqlstate is ~S\n" s)
                #f))
          (else
           #f))))

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
                            fields)
          ;; starting w/ PostgreSQL 8.1, this needs to be explicit.
          " WITH OIDS")))

;; Here we define procedures to carry out the tests.

(define test:pg-guile-pg-loaded
  (add-test #t
    (lambda ()
      (and (defined? 'pg-guile-pg-loaded)
           (let ((x (pg-guile-pg-loaded)))
             (and (list? x)
                  (begin
                    (for-each (lambda (s)
                                (simple-format #t "FEATURE: ~A\n" s))
                              x)
                    #t)
                  (and-map symbol? x)))))))

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

(define test:protocol-version/bad-connection
  (add-test #f
    (lambda ()
      (or (pg-protocol-version #:foo)
          (pg-protocol-version *C*))))) ; not yet set

(define test:make-connection
  (add-test #t
    (lambda ()
      (let ((new (false-if-exception (pg-connectdb ""))))
        (and (pg-connection? new)
             (begin (set! *C* new)
                    (simple-format #t "INFO: connection: ~S\n" *C*)
                    #t))))))

(define test:protocol-version
  (add-test #t
    (lambda ()
      (let ((v (pg-protocol-version *C*)))
        (and (number? v)
             (not (= 0 v))
             (< 1 v))))))

(define test:tracing-traced-connection
  (add-test #f
    (lambda ()
      (define (one!)
        (pg-trace *C* (current-error-port)))
      (one!)
      (let ((rv (false-if-exception (one!))))
        (pg-untrace *C*)
        rv))))

(define test:untracing-untraced-connection
  (add-test (if #f #f)
    (lambda ()
      (pg-untrace *C*))))               ; used to segfault

(define test:various-connection-info
  (add-test #t
    (lambda ()
      ;; Starting with PostgreSQL 8.0, "UTF8" is more correct than "UNICODE".
      ;; This is not mentioned until the 8.1 release notes, however.  :-/
      ;; Perhaps this should be moved into ‘pg-client-encoding’ proper.
      (define (normalized-cenc)
        (let ((s (pg-client-encoding *C*)))
          (case (string->symbol s)
            ((UNICODE) "UTF8")
            ((ALT) "WIN866")
            ((WIN) "WIN1251")
            ((TCVN) "WIN1258")
            (else s))))
      (let ((host (pg-get-host *C*))
            (pid (pg-backend-pid *C*))
            (enc (normalized-cenc)))
        (and (or (not host)
                 (and (string? host)
                      (not (string-null? host))))
             (integer? pid)
             (string? enc)
             (not (string-null? enc))
             ;; try something different from the original
             (let ((new-enc (if (string=? "UTF8" enc)
                                "SQL_ASCII"
                                "UTF8")))
               (and (pg-set-client-encoding! *C* new-enc)
                    (let ((check (normalized-cenc)))
                      (pg-set-client-encoding! *C* enc)
                      (and (string=? check new-enc)
                           (string=? enc (normalized-cenc)))))))))))

(define test:mblen
  (add-test #t
    (lambda ()
      (and (zero? (pg-mblen 'LATIN1 "" 0))
           (let* ((text "☡ Guile ∘ PostgreSQL ∞")
                  (len (string-length text)))
             (let loop ((start 0) (good '(3 1 1 1 1 1 1 1
                                            3 1 1 1 1 1 1 1 1 1 1 1 1
                                            3)))
               (or (= len start)
                   (let ((x (pg-mblen 'UTF8 text start)))
                     (and (positive? x)
                          (not (null? good))
                          (= x (car good))
                          (loop (+ start x) (cdr good)))))))))))

(define test:known-bad-command
  (add-test #f
    (lambda ()
      (command-ok? (cexec "LIKELY-TO-BE-INVALID-COMMAND;")))))

(define test:transaction-status
  (add-test #t
    (let* ((st (lambda () (pg-transaction-status *C*)))
           (st-ok? (lambda (expected) (eq? expected (st)))))
      (lambda ()
        (and (st-ok? #:idle)
             (command-ok? (cexec "START TRANSACTION"))
             (st-ok? #:intrans)
             (eq? 'PGRES_FATAL_ERROR
                  (pg-result-status
                   (cexec "INSERT INTO nonexistent VALUES (42)")))
             (st-ok? #:inerror)
             (command-ok? (cexec "ROLLBACK"))
             (st-ok? #:idle))))))

(define test:parameter-status
  (add-test #t
    (lambda ()
      (or (= 2 (pg-protocol-version *C*))
          (and-map (lambda (k)
                     (let ((v (pg-parameter-status *C* k)))
                       (format #t "INFO: parameter ~S => ~S\n" k v)
                       (string? v)))
                   '(server_version
                     client_encoding
                     is_superuser
                     session_authorization
                     DateStyle))))))

(define test:server-version
  (add-test #t
    (lambda ()
      (let ((s (pg-parameter-status *C* 'server_version))
            (v (pg-server-version *C*)))
        (format #t "INFO: server version => ~S\n" v)
        (and s
             (string? s)
             (not (string-null? s))
             v
             (not (zero? v))
             (let ((L (string-index s #\.))
                   (R (string-rindex s #\.)))
               (define (n<- b e)
                 (string->number (substring s b e)))
               (and L R (< L R)
                    (= v (+ (* 10000 (n<- 0 L))
                            (*   100 (n<- (1+ L) R))
                            (*     1 (n<- (1+ R) (string-length s))))))))))))

(define test:set-error-verbosity
  (add-test #:default
    (lambda ()
      (and (pg-set-error-verbosity *C* #:terse)
           (eq? (if (< 2 (pg-protocol-version *C*))
                    #:terse
                    #:default)
                (pg-set-error-verbosity *C* #:default))
           (pg-set-error-verbosity *C* #:default)))))

(define test:set-notice-out!-1
  (add-test #t
    (lambda ()
      (let ((n (call-with-output-string
                (lambda (port)
                  (pg-set-notice-out! *C* port)
                  (cexec "CREATE TABLE unused (ser serial, a int)")))))
        (and (string? n)
             (regexp-exec
              ;; The full message begins with:
              ;; "NOTICE:  CREATE TABLE will create implicit sequence "
              ;; and ends differently depending on PostgreSQL version.
              ;; Here are some format strings that match those versions:
              ;; - 6.x    -- "'~A' for SERIAL column '~A'"
              ;; - 7.4.5  -- "~S for \"serial\" column ~S"
              ;; - 8.1.0  -- "~S for serial column ~S"
              (make-regexp
               "^NOTICE.+implicit.+unused_ser_seq.+unused.ser")
              n)
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
                  (cexec "CREATE TABLE unused2 (ser serial, a int)")))))
        (and (string? n) (string-null? n))))))

(define test:set-client-data
  (add-test 'defunct
    (lambda ()
      ;; ‘pg-set-client-data!’ was removed in Guile-PG 0.39.
      'defunct)))

(define test:get-client-data
  (add-test 'defunct
    (lambda ()
      ;; ‘pg-get-client-data’ was removed in Guile-PG 0.39.
      'defunct)))

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
  (add-test 'defunct
    (lambda ()
      ;; Although ‘pg-get-connection’ was removed for the "more modesty"
      ;; release (pre-announced in Guile-PG 0.37), we leave this test here.
      'defunct)))

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

(define test:fnumber-and-friends
  (add-test #t
    (lambda ()
      (let ((res (cexec "SELECT col1, col2 FROM test WHERE col1 <= 100")))
        (and (tuples-ok? res)
             (eq? (pg-fnumber res "col1") 0)
             (eq? (pg-fnumber res "col2") 1)
             (eq? (pg-fnumber res "invalid_column_name") -1)
             (or (= 2 (pg-protocol-version *C*))
                 (and (integer? (pg-ftable res 0))
                      (integer? (pg-ftablecol res 0))
                      (= 0 (pg-fformat res 0)))))))))

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

(define test:pg-exec-params
  (add-test #t
    (lambda ()
      (define (single res)
        (if (tuples-ok? res)
            (pg-getvalue res 0 0)
            (let ((msg (pg-result-error-message res)))
              (format #t "EWHY: pg-exec-params ~A\n" msg)
              msg)))
      (define (spin spec)
        (define (sel pos)
          (format #f "SELECT ~A;" (list-ref spec pos)))
        (string=? (single (cexec (sel 0)))
                  (single (pg-exec-params
                           *C* (sel 1)
                           (apply vector (cddr spec))))))
      (or (= 2 (pg-protocol-version *C*))
          (and-map spin
                   '(("42"     "$1::integer" "42")
                     ("'foo'"  "$1::text"    "foo")
                     ("'a''b'" "$1::text"    "a'b")
                     ("6 * 6 + 6"
                      "$1::integer * $2::integer + $3::integer"
                      "6" "6" "6")
                     ("4 ^ 2"
                      "CAST ($1 AS integer) ^ CAST ($2 AS integer)"
                      "4" "2")))))))

(define test:pg-exec-prepared
  (add-test #t
    (lambda ()
      (define (single res)
        (if (tuples-ok? res)
            (pg-getvalue res 0 0)
            (let ((msg (pg-result-error-message res)))
              (format #t "EWHY: pg-exec-prepared ~A\n" msg)
              msg)))
      (define (spin spec)
        (define (sel pos)
          (format #f "SELECT ~A;" (list-ref spec pos)))
        (cexec "DEALLOCATE plan;")
        (and (command-ok? (cexec (format #f "PREPARE plan (~A) AS ~A"
                                         (list-ref spec 2) ; blech
                                         (sel 1))))
             (string=? (single (cexec (sel 0)))
                       (single (pg-exec-prepared
                                *C* "plan"
                                (apply vector (cdddr spec)))))))
      (or (= 2 (pg-protocol-version *C*))
          (and-map spin
                   '(("42"     "$1" "integer" "42")
                     ("'foo'"  "$1" "text"    "foo")
                     ("'a''b'" "$1" "text"    "a'b")
                     ("6 * 6 + 6"
                      "$1 * $2 + $3"
                      "integer, integer, integer"
                      "6" "6" "6")
                     ("4 ^ 2"
                      "CAST ($1 AS integer) ^ CAST ($2 AS integer)"
                      "integer, integer"
                      "4" "2")))))))

(define test:get-copy-data
  (add-test #t
    (lambda ()
      (or (= 2 (pg-protocol-version *C*))
          (and (command-ok?
                (cexec "SELECT * INTO t1 FROM test WHERE col1 = 1"))
               (let ((res (cexec "COPY t1 TO STDOUT"))
                     (box (list #f)))
                 (and ((ok? 'PGRES_COPY_OUT) res)
                      (< 0 (pg-get-copy-data *C* box #f))
                      (car box)
                      (= -1 (pg-get-copy-data *C* box #f))
                      (car box)
                      (string=? "1\tColumn 1\n" (car box)))))))))

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

(define test:put-copy-data/end
  (add-test #t
    (lambda ()
      (let ((res (cexec "COPY t1 FROM STDIN")))
        (and ((ok? 'PGRES_COPY_IN) res)
             (begin
               (for-each (lambda (s) (pg-put-copy-data *C* s))
                         '("2\tColumn 2" "\n" "\\." "\n"))
               (and (= 1 (pg-put-copy-end *C*))
                    (let ((res (cexec "SELECT * FROM t1 WHERE col1 = 2")))
                      (and res (tuples-ok? res)
                           (equal? (list (pg-getvalue res 0 0)
                                         (pg-getvalue res 0 1))
                                   '("2" "Column 2")))))))))))

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

(define test:set-nonblocking!
  (add-test #t
    (lambda ()
      (or (not (memq 'PQSETNONBLOCKING (pg-guile-pg-loaded)))
          (pg-set-nonblocking! *C* #t)))))

(define test:is-nonblocking?
  (add-test #t
    (lambda ()
      (or (not (memq 'PQISNONBLOCKING (pg-guile-pg-loaded)))
          (pg-is-nonblocking? *C*)))))

(define test:asynchronous-notification
  (add-test #t
    (lambda ()
      (and
       ;; Start listening for the sooper-sikret message!
       (command-ok? (cexec "LISTEN \"sooper-sikret message\""))
       ;; Another request is harmless.
       (command-ok? (cexec "LISTEN \"sooper-sikret message\""))
       ;; Pre-checks: No word yet, even if we tickle.
       (not (pg-notifies *C*))
       (not (pg-notifies *C* #t))
       ;; Methodology Note: Ideally, the notification should be signalled by
       ;; another process and/or backend entirely; the approach used here may
       ;; mask bugs.
       (command-ok? (cexec "NOTIFY \"sooper-sikret message\""))
       ;; Post-checks: Although one notification is all we want, we need to
       ;; use short-circuit ‘or’ for the "first" in this post-checks series.
       ;; Subsequent checks are invalid.
       (let ((ans (or (pg-notifies *C*) (pg-notifies *C* #t))))
         (and (pair? ans)
              (string? (car ans))
              (string=? "sooper-sikret message" (car ans))
              (integer? (cdr ans))
              (let ((bpid (pg-backend-pid *C*)))
                (or (= -1 bpid) (= bpid (cdr ans))))))
       (not (or (pg-notifies *C*) (pg-notifies *C* #t)))))))

(define test:send-query-param-variants  ; cleanup?
  (add-test #t
    (lambda ()
      (or (= 2 (pg-protocol-version *C*))
          (let ((v (vector "42")))
            (and (pg-send-query-params *C* "SELECT $1::integer;" v)
                 (begin (cexec "DEALLOCATE plan;")
                        (command-ok?
                         (cexec "PREPARE plan (integer) AS SELECT $1;")))
                 (pg-send-query-prepared *C* "plan" v)))))))

(define test:asynchronous-retrieval
  (add-test #t
    (lambda ()
      (and
       ;; Create a table.
       (command-ok? (cexec "CREATE TABLE async (a numeric (20, 10))"))
       ((ok? 'PGRES_COPY_IN) (cexec "COPY async FROM STDIN"))
       (begin
         (and (< 2 (pg-protocol-version *C*))
              ;; Test fails for PostgreSQL 7.4.12 at ‘pg-endcopy’ if omitted.
              ;; This needs to be done prior to ‘pg-putline’, as well.
              (pg-set-nonblocking! *C* #f))
         (do ((i 4224 (1- i)))
             ((= i 0))
           (pg-putline *C* (format #f "~A.~A\n" i i)))
         (pg-putline *C* "\\.\n")
         (pg-endcopy *C*))
       ;; Flush until we're sure everything is sent.
       (let loop ((rv (pg-flush *C*)))
         (case rv
           ((-1) #f)
           ((0) #t)
           ((1) (begin (sleep 1) (loop (pg-flush *C*))))
           (else #f)))
       ;; Perhaps usage protocol does not absolutely require this check, but
       ;; removing it causes the subsequent ‘pg-send-query’ to return #f, with
       ;; error message "another command is already in progress".
       (not (pg-is-busy? *C*))
       ;; Register a query.
       (pg-send-query *C* "SELECT * FROM async WHERE sqrt(a) * sqrt(a) = a;")
       (begin
         (display "INFO: (async checks) ")
         ;; Wait for quiescence.
         (let loop ((checks 0))
           (cond ((pg-is-busy? *C*)
                  (pg-consume-input *C*)
                  (sleep 1)
                  (display ".")
                  (loop (1+ checks)))
                 (else
                  (display checks)
                  (newline))))
         ;; Sync up.
         (tuples-ok? (pg-get-result *C*)))))))

(define test:request-cancel
  (add-test #t
    (lambda ()
      (and
       (not (pg-is-busy? *C*))
       ;; Start a transaction that's easy to cancel.
       ;; [Insert cynical comparison to inept manglement/politicos here.]
       (pg-send-query *C* "BEGIN; UPDATE async SET a = a * sqrt (a);")
       ;; Dispatch goes through.
       (pg-request-cancel *C*)
       ;; Result handling, however, may finish, or may signal error.
       (begin
         (display "INFO: (async checks) ")
         ;; Wait for quiescence.
         (let loop ((checks 0))
           (cond ((pg-is-busy? *C*)
                  (pg-consume-input *C*)
                  (sleep 1)
                  (display ".")
                  (loop (1+ checks)))
                 (else
                  (display checks)
                  (newline))))
         ;; Sync up with the cancellation.
         (and (not (tuples-ok? (pg-get-result *C*)))
              (begin (format #t "INFO: (~A)\n"
                             (let ((reason (pg-error-message *C*)))
                               (if (string-null? reason)
                                   "cancellation"
                                   reason)))
                     #t)))))))

(define test:close
  (add-test #t
    (lambda ()
      (pg-finish *C*)
      (pg-finish *C*)                   ; expect: no error
      #t)))

(define (main)
  (set! verbose #t)
  (test-init "basic" 59)
  (test! test:pg-guile-pg-loaded
         test:pg-conndefaults
         test:protocol-version/bad-connection
         test:make-connection
         test:protocol-version
         test:tracing-traced-connection
         test:untracing-untraced-connection
         test:various-connection-info
         test:mblen
         test:known-bad-command
         test:transaction-status
         test:parameter-status
         test:server-version
         test:set-error-verbosity
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
         test:fnumber-and-friends
         test:getvalue
         test:getisnull
         test:fsize
         test:getlength
         test:pg-exec-params
         test:pg-exec-prepared
         test:get-copy-data
         test:getline
         test:getlineasync
         test:put-copy-data/end
         test:putline
         test:get-proc:pg-get-db
         test:get-proc:pg-get-user
         test:get-proc:pg-get-pass
         test:get-proc:pg-get-tty
         test:get-proc:pg-get-port
         test:get-proc:pg-get-options
         test:asynchronous-notification ; once before test:set-nonblocking!
         test:set-nonblocking!
         test:asynchronous-notification ; once after
         test:is-nonblocking?           ; must be after test:set-nonblocking!
         test:send-query-param-variants
         test:asynchronous-retrieval
         test:request-cancel
         test:close)
  (set! *C* #f)
  (drop!)
  (test-report))

(exit (main))

;;; basic.scm ends here
