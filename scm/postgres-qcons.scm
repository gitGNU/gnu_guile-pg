;;; postgres-qcons.scm --- construct SELECT queries

;;	Copyright (C) 2005 Thien-Thi Nguyen
;;
;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 2 of the License, or
;; (at your option) any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program; if not, write to the Free Software
;; Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

;;; Commentary:

;; This module exports the procedure:
;;   (sql-quote string) => string

;;; Code:

(define-module (database postgres-qcons)
  #:export (sql-quote))

;; Return a new string made by preceding each single quote in string @var{s}
;; with a backslash, and prefixing and suffixing with single quote.
;; For example:
;;
;; @lisp
;; (define bef "ab'cd")
;; (define aft (sql-quote bef))
;; aft @result{} "'ab\\'cd'"
;; (map string-length (list bef aft)) @result{} (5 8)
;; @end lisp
;;
;; Note that in the external representation of a Scheme string,
;; the backslash appears twice (this is normal).
;;
(define (sql-quote s)                   ; also surrounds w/ single quote
  (let* ((olen (string-length s))
         (len (+ 2 olen))
         (cuts (let loop ((stop olen) (acc (list olen)))
                 (cond ((string-rindex s #\' 0 stop)
                        => (lambda (hit)
                             (set! len (1+ len))
                             (loop hit (cons hit (cons hit acc)))))
                       (else (cons 0 acc)))))
         (rv (make-string len)))
    (string-set! rv 0 #\')
    (string-set! rv (1- len) #\')
    (let loop ((put 1) (ls cuts))
      (if (null? ls)
          rv
          (let* ((one (car ls))
                 (two (cadr ls))
                 (end (+ put (- two one)))
                 (tail (cddr ls)))
            (substring-move! s one two rv put)
            (or (null? tail)
                (string-set! rv end #\\))
            (loop (1+ end) tail))))))

;;; postgres-qcons.scm ends here
