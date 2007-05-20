;;; guile-pg-abstraction-scm-tests.scm

;; Copyright (C) 2002,2003,2004,2005,2006,2007 Thien-Thi Nguyen
;;
;; This file is part of Guile-PG.
;;
;; Guile-PG is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 2 of the License, or
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

;;; Code:

(load-from-path (in-vicinity *srcdir* "testing.scm"))

(use-modules (database postgres)
             (database postgres-types)
             (database postgres-table)
             (ice-9 common-list))

(define db-name (or (getenv "PGDATABASE")
                    (error "don't know what database to use")))

(define (mgr . args)
  (let ((rv (apply pgtable-manager args)))
    (and rv (procedure? rv) (getenv "DEBUG")
         ((rv #:trace-exec) *log-file*))
    rv))

;; The table manager and some global commands.
(define m #f)
(define drop #f)
(define create #f)
(define insert #f)
(define select #f)

;; support

(define (command-ok? raw)
  (eq? 'PGRES_COMMAND_OK (pg-result-status raw)))

(define-macro (pass-if explanation exp)
  `(let ((proc (lambda () ,exp)))
     (set-procedure-property! proc 'name ,explanation)
     (test #t proc)))

(define-macro (sel/check sel . body)
  `(let ((tupres ,sel))
     (define (check-dim tn fn)
       (and (= tn (pg-ntuples tupres))
            (= fn (pg-nfields tupres))))
     (define (tref tn fn)
       (pg-getvalue tupres tn fn))
     (pass-if "result status"
       (let ((s (pg-result-status tupres)))
         (cond ((eq? 'PGRES_TUPLES_OK s))
               (else (format #t "EWHY: result status: ~A\n      ~A\n"
                             s (pg-result-error-message tupres))
                     #f))))
     ,@body))

;;; Tests

;; Test types cache
;; expect #t
;;
(define (test:query-oid/type-name)
  (->bool (oid-type-name-cache
           (pg-connectdb (simple-format #f "dbname=~A" db-name)))))

;; Test pgtable-manager
;; expect #t
;;
(define (test:set!-m)
  (set! m (false-if-exception
           (mgr
            ;; database
            db-name
            ;; table
            "abstrAction"
            ;; defs (historical note: this testing originated w/ an
            ;;                        application that wrapped rsync)
            '((time            timestamp)
              (error_condition text)
              (files           text[])
              (wrote           int4)
              (read            text[][])
              (rate            float4)
              (total           int4)
              (speedup         float4)
              (etc             int4[])))))
  (procedure? m))

;; Test pgtable-manager procs
;; expect #t
;;
(define (test:m-procs)
  (cond (m (set! drop    (m 'drop))
           (set! create  (m 'create))
           (set! insert  (m 'insert-values))
           (set! select  (m 'select))))
  (every procedure? (list drop create insert select)))

;; Test pgtable-manager create
;; expect #t
;;
(define (test:m-create)
  (command-ok? (create)))

;; Test pgtable-manager insert 1
;; expect #t
;;
(define (test:m-insert-1)
  (command-ok? (insert (current-time) "ugh" '("a" "b" "c") 5
                       '((",b" "") ("," "c") ("" "d,"))
                       15.20 25 30.35 '(-1 2))))

;; Test pgtable-manager insert 2
;; expect #t
;;
(define (test:m-insert-2)
  (command-ok? (insert (current-time) "foo" '("d" "e" "f") 105
                       '((",x" "") ("y" ",") ("" "z,"))
                       115.120 125 130.135
                       '(3 -4))))

;; Test pgtable-manager insert 3
;; expect #t
;;
(define (test:m-insert-3)
  (command-ok? (insert (current-time) "" '() 0
                       '()
                       2 3 4 '())))

;;; multiple tests rolled into each

(define (mtest:select-*)                ; 5
  (sel/check (select #t)
             (pass-if "dim" (check-dim 3 9))
             (pass-if "ugh" (string=? "ugh" (tref 0 1)))
             (pass-if "5" (string=? "5" (tref 0 3)))
             (pass-if "130.135" (string=? "130.135" (tref 1 7)))
             (pass-if "{}" (string=? "{}" (tref 2 2)))))

(define (mtest:select-*-error_condition) ; 3
  (sel/check (select #t #:where '(= error_condition "foo"))
             (pass-if "size" (check-dim 1 9))
             (pass-if "files"
               (let ((val (tref 0 2)))
                 (or
                  ;; before postgresql 7.2
                  (string=? val "{\"d\",\"e\",\"f\"}")
                  ;; postgresql 7.2 and later
                  (string=? val "{d,e,f}"))))
             (pass-if "etc" (string=? "{3,-4}" (tref 0 8)))))

(define (mtest:select-*-read)           ; 3
  (sel/check (select #t #:where '(= read[2][1] ","))
             (pass-if "size" (check-dim 1 9))
             (pass-if "25" (string=? "25" (tref 0 6)))
             (pass-if "read"
               (let ((val (tref 0 4)))
                 (or
                  ;; before postgresql 7.2
                  (string=? val "{{\",b\",\"\"},{\",\",\"c\"},{\"\",\"d,\"}}")
                  ;; postgresql 7.2 and later
                  (string=? val "{{\",b\",\"\"},{\",\",c},{\"\",\"d,\"}}"))))))

(define (mtest:select-count)            ; 2
  (sel/check (select '((int4 #f (count *))))
             (pass-if "size" (check-dim 1 1))
             (pass-if "3" (string=? "3" (tref 0 0)))))

(define (mtest:select-*-read<>)         ; 2
  (sel/check (select #t #:where '(<> read[2][1] ","))
             (pass-if "size" (check-dim 1 9))
             (pass-if "115.12" (string=? "115.12" (tref 0 5)))))

(define (mtest:select-files/etc)        ; 3
  (sel/check (select '(files etc) #:where '(< etc[2] 0))
             (pass-if "size" (check-dim 1 2))
             (pass-if "files"
               (let ((val (tref 0 0)))
                 (or
                  ;; before postgresql 7.2
                  (string=? val "{\"d\",\"e\",\"f\"}")
                  ;; postgresql 7.2 and later
                  (string=? val "{d,e,f}"))))
             (pass-if "etc" (string=? "{3,-4}" (tref 0 1)))))

(define (test:mtest-cleanup)
  (command-ok? (car (drop))))           ; use CAR because `drop' => a list

;;; now do some specific checks

;;+ (define pgtable:exception:malformed-defs
;;+   (cons 'misc-error "malformed def"))
;;+
;;+ (define-macro (expect-bad-defs defs)
;;+   `(pass-if-exception (format #f "~S" ,defs)
;;+        pgtable:exception:malformed-defs
;;+      (pgtable-manager db-name "dontcare" ,defs)))
;;+
;;+ (with-test-prefix "bad defs"
;;+   (expect-bad-defs #f)
;;+   (expect-bad-defs #t)
;;+   (expect-bad-defs 1)
;;+   (expect-bad-defs 'symbol)
;;+   (expect-bad-defs "string")
;;+   (expect-bad-defs '())
;;+   (expect-bad-defs '(bad))
;;+   (expect-bad-defs '(text))
;;+   (expect-bad-defs '(bad defs bad defs bad defs))
;;+   (expect-bad-defs '(a text b text c text))
;;+   (expect-bad-defs '((text a) (text b) (text c)))
;;+   (expect-bad-defs '((text . a) (text . b) (text . c))))

;;; ok, now do some real checks (although still simple)

(define (test-m2)

  (let ((m2 (mgr db-name "abstrActions_2" '((n int4 "primary key")
                                            (desc text)))))

    (pass-if "m2" (procedure? m2))

    ((m2 'drop))                        ; no check

    (pass-if "(m2 'create)" (command-ok? ((m2 'create))))
    (pass-if "(m2 'drop)"   (command-ok? (car ((m2 'drop)))))
    (pass-if "(m2 'create)" (command-ok? ((m2 'create))))

    (pass-if "m2 insert 1"
      (command-ok? ((m2 'insert-values)
                    42 "the answer to the big question")))

    (pass-if "m2 insert 2"
      (command-ok? ((m2 'insert-values)
                    13 "average age of kid first trying pot")))

    (pass-if "m2 is 2x2"
      (let ((res ((m2 'select) #t)))
        (and (= 2 (pg-ntuples res))
             (= 2 (pg-nfields res)))))

    (pass-if "m2 insert-col-values 1"
      (command-ok? ((m2 'insert-col-values) '(n) 31)))

    (pass-if "m2 insert-col-values 2"
      (not (command-ok? ((m2 'insert-col-values)
                         '(desc) "lost in space")))) ; no p key

    (pass-if "m2 insert-col-values 3"
      (command-ok? ((m2 'insert-col-values)
                    '(n desc) 343 "a nice prime")))

    (pass-if "m2 delete-rows"
      (command-ok? ((m2 'delete-rows) '(= 13 n))))

    (pass-if "m2 update-col"
      (command-ok? ((m2 'update-col) '(desc) '("almost 2^5, eh?") '(= 31 n))))

    (let ((res ((m2 'select) '(desc))))
      (pass-if "m2 select"
        (eq? 'PGRES_TUPLES_OK (pg-result-status res))))

    (let ((res ((m2 'select) '(desc n))))
      (pass-if "objectifier-hints: list of col names"
        (equal? ((m2 'tuples-result->object-alist) res)
                '((desc "the answer to the big question"
                        "a nice prime"
                        "almost 2^5, eh?")
                  (n 42 343 31))))
      (pass-if "m2 result as rows"
        (equal? ((m2 'tuples-result->rows) res)
                '(("the answer to the big question" 42)
                  ("a nice prime" 343)
                  ("almost 2^5, eh?" 31)))))

    (pass-if "m2 final drop"
      (command-ok? (car ((m2 'drop))))))) ; use CAR because `drop' => a list

(define (test-m3)
  (let ((all (apply string (map integer->char (iota 256))))
        (m3 (mgr db-name "abstrActions_3" '((b bytea)))))

    (define (check-1 name expected expr)
      (sel/check ((m3 #:select) `((#f #f ,expr)))
                 (pass-if (simple-format #f "~A ~A" name expected)
                   (and (check-dim 1 1)
                        (string=? expected (tref 0 0))))))

    ((m3 #:drop))                       ; no check

    (pass-if "(m3 #:create)"
      (command-ok? ((m3 #:create))))

    (pass-if "(m3 #:insert-values all)"
      (command-ok? ((m3 #:insert-values) all)))

    (pass-if "m3 round-trip"
      (equal? all (let* ((res ((m3 #:select) #t))
                         (status (pg-result-status res))
                         (s (and (eq? 'PGRES_TUPLES_OK status)
                                 (caar ((m3 #:tuples-result->rows) res)))))
                    (simple-format
                     *log-file* "m3 round-trip: ~S\n~S ~S\n"
                     (pg-getvalue res 0 0)
                     (string-length s)
                     (map char->integer (string->list s)))
                    s)))

    (check-1 "in/set" "f" '(in/set 0 1 2 3))
    (check-1 "in/set" "t" '(in/set (+ 6 (* 6 6)) 41 42 43))
    (check-1 "between" "t" '(between 4 (+ 1 3) (- 5 1)))
    (check-1 "between" "f" '(between 1 3 5))

    ((m3 #:drop))))                     ; no check


(define (cleanup!)
  (set! m #f)
  (set! drop #f)
  (set! create #f)
  (set! insert #f)
  (set! select #f))

(define (main)
  (set! verbose #t)
  (test-init "abstraction-scm-tests"    ; manularity sucks
             (+ 1
                6
                (let ((count (list 5 3 3 2 2 3))) ; multiples
                  (+ (length count)
                     (apply + count)))
                1
                (+ 6 1 9)               ; m2
                (+ 3 (* 2 4))))         ; m3
  (test #t test:query-oid/type-name)
  (test #t test:set!-m)
  (test #t test:m-procs)
  (test #t test:m-create)
  (test #t test:m-insert-1)
  (test #t test:m-insert-2)
  (test #t test:m-insert-3)
  (mtest:select-*)
  (mtest:select-*-error_condition)
  (mtest:select-*-read)
  (mtest:select-count)
  (mtest:select-*-read<>)
  (mtest:select-files/etc)
  (test #t test:mtest-cleanup)
  (test-m2)
  (test-m3)
  (cleanup!)
  (test-report))

(exit (main))

;;; guile-pg-abstraction-scm-tests.scm ends here
