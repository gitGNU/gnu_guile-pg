;;; postgres-resdisp.scm --- display a query result in various ways

;; Copyright (C) 2005, 2006, 2007 Thien-Thi Nguyen
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

;;; Commentary:

;; This module exports the proc:
;;   (display-result result [decor [flags...]])

;;; Code:

(define-module (database postgres-resdisp)
  #:use-module ((database postgres)
                #:select (pg-result?
                          pg-result-status
                          pg-ntuples
                          pg-nfields
                          pg-fname
                          pg-getlength
                          pg-getvalue))
  #:export (display-result))

(define (decor name)
  (case (if (keyword? name)
            (keyword->symbol name)
            name)
    ((space)      (lambda (x) (case x ((h) #\space) (else " "))))
    ((h-only)     (lambda (x) (case x ((h) #\-) ((v) " ") ((+) "-"))))
    ((v-only)     (lambda (x) (case x ((h) #\space) ((v) "|") ((+) "|"))))
    ((+-only)     (lambda (x) (case x ((h) #\space) ((v) " ") ((+) "+"))))
    ((no-h)       (lambda (x) (case x ((h) #\space) ((v) "|") ((+) "+"))))
    ((no-v)       (lambda (x) (case x ((h) #\-) ((v) " ") ((+) "+"))))
    ((no-+)       (lambda (x) (case x ((h) #\-) ((v) "|") ((+) " "))))
    ((fat-space)  (lambda (x) (case x ((h) #\space) (else "  "))))
    ((fat-no-v)   (lambda (x) (case x ((h) #\-) ((v) "   ") ((+) "-+-"))))
    ((fat-h-only) (lambda (x) (case x ((h) #\-) ((v) "  ") ((+) "--"))))
    (else         (error "bad decor:" name))))

(define vr vector-ref)
(define v! vector-set!)

(define (v-init-proc ftot)
  (lambda (init)
    (let ((v (make-vector ftot)))
      (do ((fn 0 (1+ fn))) ((= ftot fn) v)
        (v! v fn (init fn))))))

;; Display @var{result}, including header and ASCII decoration.
;; @var{result} is an object that satisfies @code{pg-result?}.
;; Optional second arg @var{decor} is a symbol, one of:
;;
;; @example
;; space
;; h-only v-only +-only
;; no-h no-v no-+
;; fat-space fat-no-v fat-h-only
;; @end example
;;
;; If omitted or @code{#f}, the default is to use "-", "|" and "+" for the
;; horizontal, vertical and intersection decorations, respectively.
;; The rest of the args, @var{flags}, are symbols that configure various
;; parts of the output.  Recognized flags:
;;
;; @example
;; no-top-hr no-mid-hr no-bot-hr
;; no-L no-R no-LR
;; @end example
;;
;; These inhibit the @dfn{horizonal rule} at the top of the output,
;; between the header and the table body, and at the bottom of the
;; output, respectively; as well as the left and right decorations.
;;
;;-sig: (result [decor [flags...]])
;;
(define (display-result result . opts)
  (or (and (pg-result? result)
           (eq? 'PGRES_TUPLES_OK (pg-result-status result)))
      (error "bad result:" result))
  (let* ((ttot (pg-ntuples result))
         (ftot (pg-nfields result))
         (deco (if (or (null? opts) (not (car opts)))
                   (lambda (x) (case x ((h) #\-) ((v) "|") ((+) "+")))
                   (let ((d (car opts)))
                     (cond ((procedure? d) d)
                           ((keyword? d) (decor d))
                           ((symbol? d) (decor d))
                           (else (error "bad decor:" d))))))
         (flags (if (null? opts)
                    opts
                    (let ((rest (cdr opts)))
                      (if (and (pair? rest) (pair? (car rest)))
                          (car rest)
                          rest))))
         (L? (not (or (memq 'no-L flags) (memq 'no-LR flags))))
         (R? (not (or (memq 'no-R flags) (memq 'no-LR flags))))
         (v-init (v-init-proc ftot))
         (names  (v-init (lambda (fn) (pg-fname result fn))))
         (widths (v-init (lambda (fn)
                           (let ((len (string-length (vr names fn))))
                             (do ((tn 0 (1+ tn))) ((= ttot tn) len)
                               (set! len (max (pg-getlength result tn fn)
                                              len))))))))

    (define (display-row sep producer padding)
      (do ((fn 0 (1+ fn)))
          ((= ftot fn))
        (and (if (= 0 fn) L? #t)
             (display sep))
        (let ((s (producer fn)))
          (display s)
          (display (make-string (- (vr widths fn) (string-length s)) padding))))
      (and R? (display sep))
      (newline))

    (define (hr inhibit)
      (or (memq inhibit flags)
          (display-row (deco '+)
                       (lambda (fn) "")
                       (deco 'h))))

    (define (row content)
      (display-row (deco 'v) content #\space))

    ;; do it!
    (hr 'no-top-hr)
    (row (lambda (fn) (vr names fn)))
    (hr 'no-mid-hr)
    (do ((tn 0 (1+ tn)))
        ((= ttot tn))
      (row (lambda (fn) (pg-getvalue result tn fn))))
    (hr 'no-bot-hr))

  (if #f #f))

;;; postgres-resdisp.scm ends here
