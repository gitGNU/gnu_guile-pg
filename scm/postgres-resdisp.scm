;;; postgres-resdisp.scm --- display a query result in various ways

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

;; This module exports the proc:
;;   (display-result result [intersection-style [flags...]])

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

(define (intersection-style name)
  (case name
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
    (else         (error "bad style:" style))))

(define vr vector-ref)
(define v! vector-set!)

(define (v-init-proc ftot)
  (lambda (init)
    (let ((v (make-vector ftot)))
      (do ((fn 0 (1+ fn))) ((= ftot fn) v)
        (v! v fn (init fn))))))

;; Display @var{result}, including header and ASCII decoration.
;; @var{result} is an object that satisfies @code{pg-result?}.
;; Optional second arg @var{intersection-style} is a symbol, one of:
;;
;; @example
;; space
;; h-only v-only +-only
;; no-h no-v no-+
;; fat-space fat-no-v fat-h-only
;; @end example
;;
;; If omitted or #f, the default is to use "-", "|" and "+" for the
;; horizontal, vertical and intersection decorations, respectively.
;; The rest of the args, @var{flags}, are symbols that configure various
;; parts of the output.  Recognized flags:
;;
;; @example
;; no-top-hr no-mid-hr no-bot-hr
;; @end example
;;
;; These inhibit the @dfn{horizonal rule} at the top of the output,
;; between the header and the table body, and at the bottom of the
;; output, respectively.
;;
;;-sig: (result [intersection-style [flags...]])
;;
(define (display-result result . opts)
  (or (and (pg-result? result)
           (eq? 'PGRES_TUPLES_OK (pg-result-status result)))
      (error "bad result:" result))
  (let* ((ttot   (pg-ntuples result))
         (ftot   (pg-nfields result))
         (style  (if (or (null? opts) (not (car opts)))
                     (lambda (x) (case x ((h) #\-) ((v) "|") ((+) "+")))
                     (let ((s (car opts)))
                       (cond ((procedure? s) s)
                             ((symbol? s) (intersection-style s))
                             (else (error "bad style:" s))))))
         (flags  (if (null? opts)
                     opts
                     (let ((rest (cdr opts)))
                       (if (and (pair? rest) (pair? (car rest)))
                           (car rest)
                           rest))))
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
        (display sep)
        (let ((s (producer fn)))
          (display s)
          (display (make-string (- (vr widths fn) (string-length s)) padding))))
      (display sep)
      (newline))

    (define (hr inhibit)
      (or (memq inhibit flags)
          (display-row (style '+)
                       (lambda (fn) "")
                       (style 'h))))

    (define (row content)
      (display-row (style 'v) content #\space))

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
