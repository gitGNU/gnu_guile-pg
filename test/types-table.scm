;;; types-table.scm                             -*- coding: utf-8 -*-

;; Copyright (C) 2002, 2003, 2004, 2005, 2006, 2007,
;;   2008, 2009, 2011, 2012, 2014 Thien-Thi Nguyen
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

(or (not *all-tests*)
    (begin (display "Bad env.")
           (newline)
           (exit #f)))

(use-modules (database postgres)
             (database postgres-qcons)
             (database postgres-types)
             (database postgres-table)
             (srfi srfi-13)
             (srfi srfi-14)
             (ice-9 regex)
             (ice-9 common-list))

(fresh!)

(define db-name (or (getenv "PGDATABASE")
                    (error "don't know what database to use")))

(define (mgr . args)
  (let ((rv (apply pgtable-manager args)))
    (cond ((procedure? rv)
           (and (getenv "DEBUG")
                ((rv #:trace-exec) *log-file*))
           (let ((scs 'standard_conforming_strings)
                 (conn ((rv #:k) #:connection)))
             (define (try! action value)
               (or value
                   (begin (fso "while trying to ~A, got error:~%~A~%"
                               action (pg-error-message conn))
                          (exit #f))))
             (try! "set client encoding"
                   (pg-set-client-encoding! conn "UTF8"))
             (try! "verify client encoding is UTF8"
                   (member (pg-client-encoding conn)
                           (list
                            ;; before postgresql 8.1
                            "UNICODE"
                            ;; postgresql 8.1 and later
                            "UTF8")))
             (cond ((pg-parameter-status conn scs)
                    => (lambda (v)
                         (fso "INFO: ~S => ~S~%" scs v)
                         (fluid-set! sql-quote-auto-E? #t)))))))
    rv))

;; The table manager and some global commands.
(define m #f)
(define drop #f)
(define create #f)
(define insert #f)
(define select #f)

;; support

(define (command-ok? raw)
  (let ((sym (pg-result-status raw)))
    (case sym
      ((PGRES_COMMAND_OK) #t)
      (else (and (equal? "1" (getenv "DEBUG"))
                 (fso "~A: ~A~%" sym (pg-result-error-message raw)))
            #f))))

(define-macro (pass-if explanation exp)
  `(let ((proc (lambda () ,exp)))
     (set-procedure-property! proc 'name ,explanation)
     (test #t proc)))

(define (handle-exception-proc why)
  (lambda (key . rest)
    (case key
      ((misc-error)
       (and (string-match why (apply fs (cdr rest)))
            why))
      (else
       (cons key rest)))))

(define-macro (pass-if-exception explanation weirdness exp)
  `(let* ((key (car ,weirdness))
          (why (cdr ,weirdness))
          (proc (lambda ()
                  (catch key
                         (lambda () ,exp #t)
                         (handle-exception-proc why)))))
     (set-procedure-property! proc 'name ,explanation)
     (test why proc)))

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
               (else (fso "EWHY: result status: ~A~%      ~A~%"
                          s (pg-result-error-message tupres))
                     #f))))
     ,@body))

;;; Tests

;; Test types cache
;; expect #t
;;
(define (test:query-oid/type-name)
  (->bool (oid-type-name-cache
           (pg-connectdb (fs "dbname=~A" db-name)))))

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

(define PESKY '((1 1 . "f")
                (8 8 . "\\\"\a\f\n\r\t\v")
                (4 6 . "⌬ ")        ; U+232C / UTF-8: E2 8C AC
                (4 4 . "\\\\\\\"")
                (6 6 . "\\a\\f\\n")
                (6 6 . "\\r\\t\\v")
                (2 2 . "oo")))

(define POISON (apply string-append
                      (with-output-to-string
                        (lambda ()
                          (display "(")
                          (let loop ((ls (map cddr PESKY)))
                            (or (null? ls)
                                (let ((next (cdr ls)))
                                  (display (string-xrep (car ls)))
                                  (or (null? next)
                                      (display " "))
                                  (loop next))))
                          (display ")")))
                      (map cddr PESKY)))

(define POISON-COUNTS (let ((adj (+ (string-length
                                     (object->string
                                      (make-list (length PESKY)
                                                 "")))
                                    (string-length
                                     (string-filter
                                      POISON
                                      (char-set #\\ #\")
                                      (string-index POISON #\)))))))
                        (define (sum sel)
                          (+ adj (* 2 (apply + (map sel PESKY)))))
                        (fs "~A ~A" (sum car) (sum cadr))))

(and (getenv "DEBUG")
     (fso "POISON: ~A |~A|~%" POISON-COUNTS POISON))

;; Test pgtable-manager insert 2
;; expect #t
;;
(define (test:m-insert-2)
  (command-ok? (insert (current-time) POISON '("d" "e" "f") 105
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

(define (mtest:select-*-error_condition) ; 4
  (sel/check (select '(time
                       error_condition files
                       wrote read rate total speedup etc
                       (#f "ec counts" (|| (char_length error_condition)
                                           " "
                                           (octet_length error_condition))))
                     #:where `(= error_condition ,POISON))
             (pass-if "size" (check-dim 1 10))
             (pass-if "ec" (let ((counts (tref 0 9))
                                 (string (tref 0 1)))
                             (and (getenv "DEBUG")
                                  (fso "ec: ~A |~A|~%" counts string))
                             (and (string=? POISON-COUNTS counts)
                                  (string=? POISON string))))
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

(define (mtest:any/all--OP)             ; 1 + 2
  ((m #:insert-col-values) '(files) '("" "d" "dd" "ddd"))
  (sel/check (select #t #:where '(all--<> "no how no way" files))
             (pass-if "size" (check-dim 4 9)))
  (sel/check (select '((#f #f (count *))) #:where '(any--= "d" files))
             (pass-if "size" (check-dim 1 1))
             (pass-if "value" (string=? "2" (tref 0 0)))))

(define (test:mtest-cleanup)
  (command-ok? (car (drop))))           ; use CAR because ‘drop’ => a list

;;; now do some specific checks

(define pgtable:exception:malformed-defs
  (cons 'misc-error "malformed def"))

(define-macro (expect-bad-defs defs)
  `(pass-if-exception (fs "~S" ,defs)
       pgtable:exception:malformed-defs
     (pgtable-manager db-name "dontcare" ,defs)))

(define (test-bad-defs)
  (expect-bad-defs #f)
  (expect-bad-defs #t)
  (expect-bad-defs 1)
  (expect-bad-defs 'symbol)
  (expect-bad-defs "string")
  (expect-bad-defs '())
  (expect-bad-defs '(bad))
  (expect-bad-defs '(text))
  (expect-bad-defs '(bad defs bad defs bad defs))
  (expect-bad-defs '(a text b text c text))
  (expect-bad-defs '((text a) (text b) (text c)))
  (expect-bad-defs '((text . a) (text . b) (text . c))))

;;; ok, now do some real checks (although still simple)

(define (test-m2)

  (let ((m2 (mgr db-name "abstr∀ctions-2" '((∑╌ int4 "primary key")
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
      (command-ok? ((m2 'insert-col-values) '(∑╌) 31)))

    (pass-if "m2 insert-col-values 2"
      (not (command-ok? ((m2 'insert-col-values)
                         '(desc) "lost in space")))) ; no p key

    (pass-if "m2 insert-col-values 3"
      (command-ok? ((m2 'insert-col-values)
                    '(∑╌ desc) 343 "a nice prime")))

    (pass-if "m2 delete-rows"
      (command-ok? ((m2 'delete-rows) '(= 13 ∑╌))))

    (pass-if "m2 update-col"
      (command-ok? ((m2 'update-col) '(desc) '("almost 2^5, eh?") '(= 31 ∑╌))))

    (let ((res ((m2 'select) '(desc))))
      (pass-if "m2 select"
        (eq? 'PGRES_TUPLES_OK (pg-result-status res))))

    (let ((res ((m2 'select) '(desc ∑╌))))
      (pass-if "objectifier-hints: list of col names"
        (equal? ((m2 'tuples-result->object-alist) res)
                '((desc "the answer to the big question"
                        "a nice prime"
                        "almost 2^5, eh?")
                  (∑╌ 42 343 31))))
      (pass-if "m2 result as rows"
        (equal? ((m2 'tuples-result->rows) res)
                '(("the answer to the big question" 42)
                  ("a nice prime" 343)
                  ("almost 2^5, eh?" 31)))))

    (pass-if "m2 final drop"
      (command-ok? (car ((m2 'drop)))))   ; use CAR because ‘drop’ => a list
    ((m2 'finish))))                      ; no check

(define (test-m3)
  (let ((all (apply string (map integer->char (iota 256))))
        (m3 (mgr db-name "abstrActions_3" '((b bytea)))))

    (define (check-1 name expected expr)
      (sel/check ((m3 #:select) `((#f #f ,expr)))
                 (pass-if (fs "~A ~A" name expected)
                   (and (check-dim 1 1)
                        (string=? expected (tref 0 0))))))

    ((m3 #:drop))                       ; no check

    (pass-if "(m3 #:create)"
      (command-ok? ((m3 #:create))))

    (pass-if "(m3 #:insert-values all)"
      (command-ok? ((m3 #:insert-values) all)))

    (check-1 "m3 inserted octet-length"
             (number->string (string-length all))
             '(octet_length b))

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

    ((m3 #:drop))                       ; no check
    ((m3 #:finish))))                   ; no check

(define (cleanup!)
  ((m #:finish))
  (set! m #f)
  (set! drop #f)
  (set! create #f)
  (set! insert #f)
  (set! select #f))

(define (main)
  (set! verbose #t)
  (test-init "types-table"
             ;; manularity sucks
             (+ 1
                6
                (let ((count '(5 4 3 2 2 3 1 2))) ; multiples
                  (+ (length count)
                     (apply + count)))
                1
                12                        ; bad defs
                (+ 6 1 9)                 ; m2
                (+ 1 (* 2 1) 2 (* 2 4)))) ; m3
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
  (mtest:any/all--OP)
  (test #t test:mtest-cleanup)
  (test-bad-defs)
  (test-m2)
  (test-m3)
  (cleanup!)
  (drop!)
  (test-report))

(exit (main))

;;; types-table.scm ends here
