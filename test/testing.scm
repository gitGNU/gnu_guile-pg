;; Copyright (C) 2004, 2006, 2008, 2011 Thien-Thi Nguyen
;; Copyright (C) 1996, 1997, 1998 Per Bothner.
;;
;; Usage:
;; (load "testing.scm")
;; (test-init "Miscellaneous" 2)
;; (test '(3 4 5 6) (lambda x x) 3 4 5 6)
;; (test '(a b c . d) 'dot '(a . (b . (c . d))))
;; (exit (test-report))
;;
;; test-init:  The first argument is the name of the test.
;; A log is written to (string-append NAME ".log").
;; The second (optional) argument is the total number of tests;
;; at the end an error is written if the actual count does not match.
;;
;; test:  The first argument is the expected result.
;; The second argument is either a procecure applied to the remaining
;; arguments;  or it is a symbol (used when reporting), in which case
;; the third argument is matched against the first.
;; The resulting values are matched using equal?.
;;
;; section:  You can divide your tests into "sections" with the section
;; procedure.  The arguments of the previous section are displayed if any
;; errors are reported.
;;
;; test-report:  Called at end to print a summary.  Return value #f
;; means either total-expected-count was not the same as actual, or
;; there was at least one unexpected success or unexpected failure.
;;
;; fail-unexpected:  If non-null, if means the following test is
;; expected to fail.  The actual value should be string explaining
;; the failure.  For example:
;; (set! fail-expected "sqrt of negative number not supported")
;; (test "+2.0i" number->string (sqrt -4))
;;
;; verbose:  If true, all tests are wroitten to standard output,
;; not just to the log file.

(define verbose #f)

(define pass-count 0)
(define fail-count 0)
(define xfail-count 0)
(define xpass-count 0)

(define *log-file* #f)

(define test-name "<unknown>")

;;; Set this (to an explanatory string) if the next test is known to fail.
(define fail-expected #f)

;;; The current section.
(define cur-section #f)
;;; The section when we last emitted a message.
(define last-section #f)

(define total-expected-count #f)

(define (test-init name . opt-args)
  (set! total-expected-count (if (null? opt-args) #f (car opt-args)))
  (set! test-name name)
  (set! *log-file* (open-output-file (string-append name ".log")))
  (display (string-append "%%%% Starting test " name) *log-file*)
  (newline *log-file*)
  (display (string-append "%%%% Starting test " name
			  "  (Writing full log to \"" name ".log\")"))
  (newline)
  (set! pass-count 0)
  (set! xpass-count 0)
  (set! fail-count 0)
  (set! xfail-count 0))

(define (display-section port)
  (display "SECTION" port)
  (do ((l cur-section (cdr l)))
      ((null? l) #f)
    (write-char #\Space port)
    (display (car l) port))
  (newline port))

(define (maybe-report-section)
  (and cur-section *log-file* (not (eq? cur-section last-section))
       (begin (display-section (current-output-port))
	      (set! last-section cur-section))))

(define (section . args)
  (set! cur-section args)
  (display-section (or *log-file* (current-output-port)))
  (set! last-section #f)
  #t)
(define record-error (lambda (e) (set! errs (cons (list cur-section e) errs))))

(define (report-pass port fun args res)
  (display (if fail-expected "XPASS:" "PASS: ") port)
  (write (cons fun args) port)
  (display "  ==> " port)
  (write res port)
  (newline port))

(define (report-fail port fun args res expect)
  (display (if fail-expected (string-append "XFAIL (" fail-expected "): ")
	       "FAIL: ") port)
  (write (cons fun args) port)
  (display "  ==> " port)
  (write res port)
  (display " BUT EXPECTED " port)
  (write expect port)
  (newline port))

(define (test expect fun . args)
  ((lambda (res)
     (cond ((procedure-name fun) => (lambda (name) (set! fun name))))
     (cond ((equal? expect res)
	    (if fail-expected
		(set! xpass-count (+ xpass-count 1))
		(set! pass-count (+ pass-count 1)))
	    (if *log-file*
		(report-pass *log-file* fun args res))
	    (cond ((or verbose fail-expected)
		   (maybe-report-section)
		   (report-pass (current-output-port) fun args res))))
	   (#t
	    (if fail-expected
		(set! xfail-count (+ xfail-count 1))
		(set! fail-count (+ fail-count 1)))
	    (if *log-file*
		(report-fail *log-file* fun args res expect))
	    (cond ((or verbose (not fail-expected))
		   (maybe-report-section)
		   (report-fail (current-output-port) fun args res expect)))))
     (set! fail-expected #f))
   (if (procedure? fun) (apply fun args) (car args))))

(define (report-display value)
  (display value)
  (and *log-file* (display value *log-file*)))

(define (report-newline)
  (newline)
  (and *log-file* (newline *log-file*)))

(define (report1 value string)
  (cond ((> value 0)
	 (report-display string)
	 (report-display value)
	 (report-newline))))

(define (test-report)
  (let ((rv #t))                        ; feeling optimistic
    (report1 pass-count  "# of expected passes      ")
    (report1 xfail-count "# of expected failures    ")
    (report1 xpass-count "# of unexpected successes ")
    (report1 fail-count  "# of unexpected failures  ")
    (if (and total-expected-count
             (not (= total-expected-count
                     (+ pass-count xfail-count xpass-count fail-count))))
        (begin
          (report-display "*** Total number of tests should be: ")
          (report-display total-expected-count)
          (report-display ". ***")
          (report-newline)
          (report-display
           "*** Discrepancy indicates testsuite error or exceptions. ***")
          (report-newline)
          (set! rv #f)))
    (cond (*log-file*
           (close-output-port *log-file*)
           (set! *log-file* #f)))
    (and rv (zero? xpass-count) (zero? fail-count))))


;;; Accountants eventually rule the world,
;;; so we mimic their good habits.  --ttn

(define *all-tests* #f)
(define (reset-all-tests!) (set! *all-tests* '()))
(define (add-test exp p) (set! *all-tests* (acons p exp *all-tests*)) p)
(define (test-one! p) (test (assq-ref *all-tests* p) p))
(define (test! . ls) (for-each test-one! ls))


;;; Common bits

(define (fs s . args)
  (apply simple-format #f s args))

(define (d/c action severity)
  (let* ((dbname (or (getenv "PGDATABASE")
                     (error "Env var PGDATABASE not set.")))
         (conn (pg-connectdb "dbname=template1"))
         (res (pg-exec conn (fs "~A DATABASE ~A~A;"
                                action dbname
                                (if (equal? "CREATE" action)
                                    " WITH ENCODING = 'UTF8'"
                                    ""))))
         (ok? (eq? 'PGRES_COMMAND_OK (pg-result-status res))))
    (and (equal? "1" (getenv "DEBUG"))
         (display (fs "~A: ~A ~A\n"
                      (if ok? "INFO" severity)
                      action
                      (if ok? "ok" (pg-result-error-message res)))))
    (set! res #f)
    (pg-finish conn)
    (set! conn #f)
    (gc)
    ok?))

(define (drop! . no-worries)
  ;; Give PostgreSQL some time to finish accessing (internally) template1.
  (usleep 100000)
  (d/c "DROP" "NO-WORRIES")
  #t)

(define (fresh!)
  (drop!)
  ;; If PostgreSQL has not yet finished accessing (internally) template1
  ;; (even though it should have), wait a little bit and try again.
  (cond (                       (d/c "CREATE" "TOO-EAGER"))
        ((begin (usleep 100000) (d/c "CREATE" "FATAL")))
        (else (display "ERROR: fresh! failed. Giving up.\n")
              (exit #f))))


;;; load-time actions

(cond ((getenv "DEBUG")
       (set! %load-verbosely #t)))

;;; testing.scm ends here
