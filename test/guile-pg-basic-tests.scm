;;    Guile-pg - A Guile interface to PostgreSQL
;;    Copyright (C) 1999-2000, 2002 Free Software Foundation, Inc.
;;
;;    This program is free software; you can redistribute it and/or modify
;;    it under the terms of the GNU General Public License as published by
;;    the Free Software Foundation; either version 2 of the License, or
;;    (at your option) any later version.
;;
;;    This program is distributed in the hope that it will be useful,
;;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;    GNU General Public License for more details.
;;
;;    You should have received a copy of the GNU General Public License
;;    along with this program; if not, write to the Free Software
;;    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
;;
;;    Guile-pg was written by Ian Grant <Ian.Grant@cl.cam.ac.uk>

(use-modules (database postgres))

(define (ok? expected-status)
  (lambda (result)
    (and result (eq? expected-status (pg-result-status result)))))

(define tuples-ok? (ok? 'PGRES_TUPLES_OK))
(define command-ok? (ok? 'PGRES_COMMAND_OK))

(define (field-info access)
  (lambda (result)
    (map (lambda (fn)
           (access result fn))
         (iota (pg-nfields result)))))

(define pg-fnames (field-info pg-fname))

(define (pg-ftype-name result fnum)
  (let ((result (pg-exec (pg-get-connection result)
                         (format #f "SELECT typname FROM pg_type WHERE oid = ~A"
                                 (pg-ftype result fnum)))))
    (and (tuples-ok? result)
         (> (pg-ntuples result) 0)
         (pg-getvalue result 0 0))))

(define pg-ftype-names (field-info pg-ftype-name))

(define (pg-getvalues result tuple)
  (map (lambda (fn)
         (pg-getvalue result tuple fn))
       (iota (pg-nfields result))))

(define (pg-gettuple result tuple)
  (map (lambda (n v)
         (cons (string->symbol n) v))
       (pg-fnames result)
       (pg-getvalues result tuple)))

(define (pg-for-each proc result)
  (let ((ntuples (pg-ntuples result)))
    (do ((i 0 (1+ i)))
        ((= i ntuples) ntuples)         ; why return this? -ttn
      (proc (pg-gettuple result i)))))

(define (pg-tuples result)
  (map (lambda (i)
         (pg-gettuple result i))
       (iota (pg-ntuples result))))

(define (run-cmd n sql-proc)
  (define (iter n)
    (let ((res (pg-exec conn (sql-proc n))))
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

(define (make-table! conn tablename fields)
  (let ((sql-string (string-append
                     "CREATE TABLE " tablename " "
                     (interpolate-list "(" ", " ")"
                                       (lambda (p)
                                         (string-append
                                          (as-string (car p)) " "
                                          (as-string (cdr p))))
                                       fields))))
    (pg-exec conn sql-string)))

(define (insert-record-sql conn tablename record)
  (string-append "INSERT INTO " tablename " "
                 (interpolate-list "(" ", " ")"
                                   (lambda (p)
                                     (string-append (as-string (car p)) " "))
                                   record)
                 " VALUES "
                 (interpolate-list "(" ", " ")"
                                   (lambda (p)
                                     (string-append (as-string (cdr p)) " "))
                                   record)))

;; Here we define procedures to carry out the tests

;; We want to keep one connection hanging around for the duration of all the
;; tests

(define conn #f)

;; Test pg-connectdb
;; expect #t
(define (test:make-connection)
  (let ((new (false-if-exception (pg-connectdb ""))))
    (and (pg-connection? new)
         (begin (set! conn new)
                #t))))

;; Test pg-reset
;; expect #t
(define (test:reset)
  (pg-reset conn)
  #t)

;; Test pg-set-client-data
;; expect #t
(define (test:set-client-data)
  (equal? (pg-set-client-data! conn '(elt1 elt2 elt3))
          '(elt1 elt2 elt3)))

;; Test pg-get-client-data
;; expect #t
(define (test:get-client-data)
  (equal? (pg-get-client-data conn) '(elt1 elt2 elt3)))

;; Test pg-exec
;; expect #f
(define (test:make-table)
  (command-ok?
   (make-table! conn "test" '(("col1" . "int4") ("col2" . "text")))))

;; Test pg-exec
;; expect #t
(define (test:load-records)
  (run-cmd 100 (lambda (n) (string-append "INSERT INTO test VALUES ("
                                          (number->string n) ", 'Column "
                                          (number->string n) "')"))))
;; Test pg-getvalue
;; expect 100
(define (test:count-records)
  (let ((res (pg-exec conn "SELECT COUNT(*) FROM test WHERE col1 <= 100")))
    (and (tuples-ok? res)
         (string->number (pg-getvalue res 0 0)))))

;; Test pg-get-xxx
;; expect #t
(define (test:get-proc proc)
  (and (string? (proc conn))
       (not (false-if-exception (proc #f)))))

;; Test pg-cmdtuples
;; expect 50
(define (test:delete-some-records)
  (let ((res (pg-exec conn "DELETE FROM test WHERE col1 > 50")))
    (if (command-ok? res)
        (string->number (pg-cmdtuples res))
        #f)))

;; Test pg-fname
;; expect ("col1" "col2")
(define (test:fname)
  (let ((res (pg-exec conn "SELECT * FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (pg-fnames res))))

;; Test pg-ftype
;; expect ("int4" "text")
(define (test:ftypes)
  (let ((res (pg-exec conn "SELECT * FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (pg-ftype-names res))))

;; Test pg-get-connection
;; expect conn
(define (test:get-connection)
  (let ((res (pg-exec conn "SELECT * FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (eq? conn (pg-get-connection res)))))

;; Test pg-ntuples
;; expect #t
(define (test:ntuples)
  (let ((res (pg-exec conn "SELECT col1 FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (eq? 1 (pg-ntuples res)))))

;; Test pg-nfields
;; expect #t
(define (test:nfields)
  (let ((res (pg-exec conn "SELECT col1 FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (eq? 1 (pg-nfields res)))))

;; Test pg-oid-status, pg-cmdtuples
;; expect #t
(define (test:oid-status)
  (let ((res (pg-exec conn "INSERT INTO test VALUES (10000, 'Column 10000')"))
        (oid #f))
    (and (command-ok? res)
         (begin
           (set! oid (pg-oid-status res))
           (set! res (pg-exec conn "DELETE FROM test WHERE col1 = 10000"))
           (and (command-ok? res)
                (string? oid)
                (> (string->number oid) 0)
                (eq? (string->number (pg-cmdtuples res)) 1))))))

;; Test pg-oid-value, pg-cmdtuples
;; expect #t
(define (test:oid-value)
  (let ((res (pg-exec conn "INSERT INTO test VALUES (10000, 'Column 10000')"))
        (oid #f))
    (and (command-ok? res)
         (begin
           (set! oid (pg-oid-value res))
           (set! res (pg-exec conn "DELETE FROM test WHERE col1 = 10000"))
           (and (command-ok? res)
                oid
                (> oid 0)
                (eq? (string->number (pg-cmdtuples res)) 1))))))

;; Test pg-fnumber
;; expect #t
(define (test:fnumber)
  (let ((res (pg-exec conn "SELECT col1, col2 FROM test WHERE col1 <= 100")))
    (and (tuples-ok? res)
         (eq? (pg-fnumber res "col1") 0)
         (eq? (pg-fnumber res "col2") 1)
         (eq? (pg-fnumber res "invalid_column_name") -1))))

;; Test pg-getvalue
;; expect #t
(define (test:getvalue)
  (let ((res (pg-exec conn "SELECT col1, col2 FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (string=? (pg-getvalue res 0 0) "1")
         (string=? (pg-getvalue res 0 1) "Column 1")
         (not (false-if-exception (pg-getvalue res 0 2)))
         (not (false-if-exception (pg-getvalue res -1 0))))))

;; Test pg-isnull
;; expect #t
(define (test:getisnull)
  (let ((res (pg-exec conn "INSERT INTO test VALUES (10000)"))
        (res2 #f))
    (and (command-ok? res)
         (begin
           (set! res (pg-exec conn "SELECT * FROM test WHERE col1 = 10000"))
           (set! res2 (pg-exec conn "DELETE FROM test WHERE col1 = 10000"))
           (and (tuples-ok? res)
                (not (pg-getisnull res 0 0))
                (pg-getisnull res 0 1)
                (not (false-if-exception (pg-getisnull res 0 2)))
                (not (false-if-exception (pg-getisnull res -1 0)))
                (command-ok? res2))))))

;; Test pg-fsize
;; expect #t
(define (test:fsize)
  (let ((res (pg-exec conn "SELECT col1, col2 FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (eq? (pg-fsize res 0) 4)
         (eq? (pg-fsize res 1) -1)
         (not (false-if-exception (pg-fsize res 2))))))

;; Test pg-getlength
;; expect #t
(define (test:getlength)
  (let ((res (pg-exec conn "SELECT col1, col2 FROM test WHERE col1 = 1")))
    (and (tuples-ok? res)
         (eq? (pg-getlength res 0 0) 1)
         (eq? (pg-getlength res 0 1) (string-length "Column 1"))
         (not (false-if-exception (pg-getlength res 0 2)))
         (not (false-if-exception (pg-getlength res 1 0))))))

;; Test pg-getline
;; expect #t
(define (test:getline)
  (let ((res (pg-exec conn "SELECT * INTO test2 FROM test WHERE col1 = 1")))
    (and (command-ok? res)
         (let ((res (pg-exec conn "COPY test2 TO STDOUT")))
           (and ((ok? 'PGRES_COPY_OUT) res)
                (let ((line (pg-getline conn)))
                  (and (string=? (pg-getline conn) "\\.")
                       (eq? (pg-endcopy conn) 0)
                       (string=? line "1\tColumn 1"))))))))

;; Test pg-putline
;; expect #t
(define (test:putline)
  (let ((res (pg-exec conn "COPY test2 FROM STDIN")))
    (and ((ok? 'PGRES_COPY_IN) res)
         (begin
           (for-each (lambda (s) (pg-putline conn s))
                     '("2\tColumn 2" "\n" "\\." "\n"))
           (and (eq? (pg-endcopy conn) 0)
                (let ((res (pg-exec conn "SELECT * FROM test2 WHERE col1 = 2")))
                  (and res (tuples-ok? res)
                       (string=? (pg-getvalue res 0 0) "2")
                       (string=? (pg-getvalue res 0 1) "Column 2"))))))))

(load-from-path (string-append *srcdir* "/testing.scm"))
(set! verbose #t)
(test-init "basic-tests" 32)
(test *VERSION* pg-guile-pg-version)
(test #t pg-guile-pg-loaded)
(test #t test:make-connection)
(test #t test:set-client-data)
(test #t test:make-table)
(test #t test:load-records)
(test 100 test:count-records)
(test #t test:reset)
(test 100 test:count-records)
(test 50 test:delete-some-records)
(test 50 test:count-records)
(test '("col1" "col2") test:fname)
(test '("int4" "text") test:ftypes)
(test #t test:get-client-data)
(test #t test:get-connection)
(test #t test:ntuples)
(test #t test:nfields)
(test #t test:oid-status)
(test #t test:oid-value)
(test #t test:fnumber)
(test #t test:getvalue)
(test #t test:getisnull)
(test #t test:fsize)
(test #t test:getlength)
(test #t test:getline)
(test #t test:putline)
(test #t test:get-proc pg-get-db)
(test #t test:get-proc pg-get-user)
(test #t test:get-proc pg-get-pass)
(test #t test:get-proc pg-get-tty)
(test #t test:get-proc pg-get-port)
(test #t test:get-proc pg-get-options)
(set! conn #f)
(test-report)

;;; guile-pg-basic-tests.scm ends here
