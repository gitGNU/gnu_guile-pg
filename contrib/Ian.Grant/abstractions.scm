;; Ideally postgres.scm should contain higher-level functions to make using
;; databases easier and prevent everyone from having to reimplement the
;; same set of prinitives again and again.  These are just some rough ideas
;; of functions that could be implemented.  They are not very efficient or
;; safe though so they're not being distributed.

(define (list->sqlcmd lst)
  (define (cmd->string cmd)
    (cond ((string? cmd) (sqlquote cmd))
          ((number? cmd) (number->string cmd))
          ((symbol? cmd) (symbol->string cmd))))
    (if (null? lst) ""
      (let ((cmd (car lst)))
        (string-append
          (cond ((pair? cmd) (string-append "(" (list->sqlcmd cmd) ")"))
                (else (string-append (cmd->string cmd))))
	  (let ((tail (cdr lst)))
	    (if (not (null? tail))
	        (string-append " " (list->sqlcmd tail)) ""))))))

(define (sqlquote str)
  (string-append "'" (sqlescape str) "'"))

(define (sqlescape str)
  (let* ((retlst '())
         (push (lambda (c) (set! retlst (cons c retlst)))))
    (for-each (lambda (chr)
                (if (or (eqv? chr #\\) (eqv? chr #\')) (push #\\) ())
                (push chr))
      (string->list str))
    (list->string (reverse retlst))))

(define (displaysql lst)
  (display (list->sqlcmd lst))
  (newline))
(define (as-string obj)
  (cond ((string? obj) obj)
        ((char? obj) (make-string 1 obj))
        ((symbol? obj) (symbol->string obj))
        ((number? obj) (number->string obj))
        ((procedure? obj) (as-string (obj)))
        ((pair? obj) (interpolate-list "(" " " ")" 
                       (lambda (x) (as-string x)) obj))
        (else "?as-string")))

(define (interpolate-list afore twixt aft proc lst)
  (define (iter lst)
    (let ((elt (car lst))
          (tail (cdr lst)))
       (string-append (proc elt)
         (if (not (null? tail))
           (string-append (as-string twixt) (iter tail))
           (as-string aft)))))
  (string-append (as-string afore) (iter lst)))


(define-public (pg-make-table! conn tablename fields)
  (let ((sql-string (string-append
                      "CREATE TABLE " tablename " "
                      (interpolate-list "(" ", " ")" 
                        (lambda (p)
                          (string-append 
                            (as-string (car p)) " " (as-string (cdr p))))
                        fields))))
       (pg:exec conn sql-string)))

(define-public (pg-insert-record! conn tablename record)
  (let ((sql-string (string-append
                      "INSERT INTO " tablename " "
                      (interpolate-list "(" ", " ")" 
                        (lambda (p)
                          (string-append (as-string (car p)) " "))
                        record)
                      " VALUES "
                      (interpolate-list "(" ", " ")" 
                        (lambda (p)
                          (string-append (as-string (cdr p)) " "))
                        record))))
       (pg:exec conn sql-string)))

(define-public (pg-fnames result)
  (let ((nfields (pg:nfields result)))
    (define (iter i)
      (if (= i nfields) '()
          (cons (pg:fname result i) (iter (+ i 1)))))
    (iter 0)))

(define-public (pg-getvalues result tuple)
  (let ((nfields (pg:nfields result)))
    (define (iter i)
      (if (= i nfields) '()
          (cons (pg:getvalue result tuple i) (iter (+ i 1)))))
    (iter 0)))

(define-public (pg-gettuple result tuple)
  (map (lambda (n v) (cons (string->symbol n) v))
       (pg:fnames result)
       (pg:getvalues result tuple)))

(define-public (pg-for-each proc result)
  (let ((ntuples (pg:ntuples result)))
    (define (iter i)
      (cond ((= i ntuples) ntuples)
            (else (proc (pg:gettuple result i))
                  (iter (+ i 1)))))
    (iter 0)))

(define-public (pg-tuples result)
  (let ((ntuples (pg:ntuples result)))
    (define (iter i)
      (cond ((= i ntuples) '())
            (else (cons (pg:gettuple result i)
                  (iter (+ i 1))))))
    (iter 0)))

