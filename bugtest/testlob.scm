;;; (use-modules (database interface postgres))
(define (write-chars n char1 char2 lop)
  (define (iter n)
    (write-char char1 lop)
    (write-char char2 lop)
    (if (eq? n 1)
        #t
        (iter (- n 1))))
  (iter n))

(define conn (pg:connectdb "dbname=test"))
(pg:exec conn "BEGIN")
(define lop (pg:lo-creat conn "w"))
(define oid (pg:lo-get-oid lop))
(display "Created large object: ")(display oid)(newline)
(write-chars 5000 #\X #\Y lop)
(close-port lop)
(pg:exec conn "END")

(pg:exec conn "BEGIN")
(pg:lo-export conn oid "testlob.scm.out")
(pg:exec conn "END")

