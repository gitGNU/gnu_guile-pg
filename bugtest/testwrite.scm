(define (write-chars n char port)
  (define (iter n)
    (write-char char port)
    (if (eq? n 1)
        #t
        (iter (- n 1))))
  (iter n))

(define port (open-output-file "testwrite.out"))
(write-chars 10000 #\X port)
(close-port port)
