;;; (use-modules (database interface postgres))

(define (run-cmd n sql-proc)
  (define (iter n)
    (let ((res (pg:exec conn (sql-proc n))))
      (if (eq? (modulo n 10) 0) (display ".") #f)
      (if (eq? n 1)
          #t
          (if (not (eq? (pg:result-status res) 'PGRES_COMMAND_OK))
              #f
              (iter (- n 1))))))
  (iter n))

(define conn (pg:connectdb "host=127.0.0.1 dbname=test"))
(define (load-records) (display "Loading 1000 records ... ")
  (if (run-cmd 1000 (lambda (n) (string-append "INSERT INTO test VALUES ("
                                              (number->string n) ", 'Column "
                                              (number->string n) "')")))
      (begin (display "OK")(newline))
      (begin (display "Failed")(newline))))
