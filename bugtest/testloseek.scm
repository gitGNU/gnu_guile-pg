(define (pg-transaction conn proc . args)
  (and (pg-exec-cmd conn "BEGIN TRANSACTION")
       (apply proc (cons conn args))
       (pg-exec-cmd conn "END TRANSACTION")))

(define (pg-exec-cmd conn sql)
  (let ((res (pg-exec conn sql)))
    (and res (eq? (pg-result-status res) 'PGRES_COMMAND_OK))))

(define (write-chars n c lop)
  (define (iter n)
    (write-char c lop)
;;    (force-output lop)
    (if (eq? n 1)
        #t
        (iter (- n 1))))
  (iter n))

(use-modules (database postgres))
(define conn (pg-connectdb "dbname=test"))
(define oid #f)

(define trace-port (open-file "testloseek.scm.log" "w"))
(pg-trace conn trace-port)

(pg-transaction conn
    (lambda (conn)
      (let ((lo-port (pg-lo-creat conn "w")))
           (and lo-port
                (write-chars 6 #\a lo-port)
                (set! oid (pg-lo-get-oid lo-port))
                (close-port lo-port)))))

(pg-transaction conn
    (lambda (conn)
      (let ((lo-port (pg-lo-open conn oid "w")))
           (and lo-port
                (eq? (pg-lo-seek lo-port 1 SEEK_SET) 1)
                (write-char #\b lo-port)
                (eq? (pg-lo-seek lo-port 3 SEEK_SET) 3)
                (write-char #\b lo-port)
                (close-port lo-port)))))

(pg-untrace conn)
(close-port trace-port)

(pg-transaction conn
    (lambda (conn)
       (pg-lo-export conn oid "testloseek.scm.lobj")))
