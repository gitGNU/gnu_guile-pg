;;; ttn/display-table.scm --- Display a table in various ways

;; $State$:$Name$
;;
;; Copyright (C) 2001-2002 Thien-Thi Nguyen
;; This file is part of ttn's personal scheme library, released under GNU
;; GPL with ABSOLUTELY NO WARRANTY.  See the file COPYING for details.

;;; Commentary:

;; This module exports the proc:
;;   (display-table TABLE . STYLE)
;;
;; Display table TABLE, including header and ASCII decoration.
;; TABLE is an array of dimension (row-count column-count).
;; Optional arg STYLE is a symbol, one of:
;;   space
;;   h-only
;;   v-only
;;   +-only
;;   no-h
;;   no-v
;;   no-+
;;   fat-space
;;   fat-no-v
;;   fat-h-only
;; If omitted, the default is to use "-", "|" and "+" for the
;; horizontal, vertical and intersection decorations, respectively.

;;; Code:

(define-module (ttn display-table))

(define (table-style name)
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

(define (display-table table . style)
  (let* ((styler table-style)
         (style  (if (null? style)
                     (lambda (x) (case x ((h) #\-) ((v) "|") ((+) "+")))
                     (let ((style (car style)))
                       (cond ((procedure? style) style)
                             ((symbol? style) (styler style))
                             (else (error "bad style:" style))))))
         (names  (object-property table 'names))
         (widths (object-property table 'widths))
         (tuples (iota (car  (array-dimensions table))))
         (fields (iota (cadr (array-dimensions table)))))
    (letrec ((display-row (lambda (sep producer padding)
                            (for-each (lambda (fn)
                                        (display sep)
                                        (let ((s (producer fn)))
                                          (display s)
                                          (display (make-string
                                                    (- (array-ref widths fn)
                                                       (string-length s))
                                                    padding))))
                                      fields)
                            (display sep) (newline)))
             (hr (lambda () (display-row (style '+) (lambda (fn) "")
                                         (style 'h)))))
      (hr)
      (display-row (style 'v) (lambda (fn) (array-ref names fn)) #\space)
      (hr)
      (for-each (lambda (tn)
                  (display-row (style 'v)
                               (lambda (fn) (array-ref table tn fn))
                               #\space))
                tuples)
      (hr))))

(export display-table)

;;; ttn/display-table.scm ends here
