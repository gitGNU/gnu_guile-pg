;;; postgres-resx.scm --- query-result transforms

;;	Copyright (C) 2002, 2003, 2004 Free Software Foundation, Inc.
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

;; This module provides various procedures to translate/transform tuple data
;; resulting from a query against a PostgreSQL database:
;;
;;  (for-each-tuple PROC RESULT) => bool
;;  (result-field->object-list RESULT FN OBJECTIFIER) => list
;;  (result->object-alist RESULT OBJECTIFIERS) => alist
;;  (result->object-alists RESULT OBJECTIFIERS) => list of alists

;;; Code:

(define-module (database postgres-resx)
  #:use-module (database postgres)
  #:use-module (database postgres-types)
  #:use-module ((database postgres-col-defs)
                #:renamer (symbol-prefix-proc 'def:))
  #:export (for-each-tuple
            result-field->object-list
            result->object-alist
            result->object-alists))

;; Apply @var{proc} to each tuple in @var{result}.  Return #t to indicate
;; success, or #f if either the tuple count or the field count is zero.
;; The tuple is the list formed by mapping @code{pg-getvalue} over the fields.
;;
(define (for-each-tuple proc result)
  (let* ((nfields (pg-nfields result))
         (ntuples (pg-ntuples result))
         (field-range (iota nfields)))
    (and (< 0 nfields) (< 0 ntuples)
         (do ((tn 0 (1+ tn)))
             ((= tn ntuples) #t)        ; retval
           (apply proc (map (lambda (fn)
                              (pg-getvalue result tn fn))
                            field-range))))))

;; For @var{result} field number @var{fn}, map @var{objectifier}.
;; Return a list whose length is the number of tuples in @var{result}.
;;
(define (result-field->object-list result fn objectifier)
  (let loop ((tn (1- (pg-ntuples result))) (acc '()))
    (if (> 0 tn)
        acc                             ; retval
        (loop (1- tn)
              (cons (objectifier (pg-getvalue result tn fn))
                    acc)))))

;; Return an alist from extracting @var{result} using @var{objectifiers}.
;; Each key (a symbol) is a field name obtained by @code{pg-fname}, and the
;; associated value is a list of objects coverted by one of the objectifier
;; procedures from the list @var{objectifiers}.
;;
(define (result->object-alist result objectifiers)
  (let ((fn -1))
    (map (lambda (objectifier)
           (set! fn (1+ fn))
           (cons (string->symbol (pg-fname result fn))
                 (result-field->object-list result fn objectifier)))
         objectifiers)))

;; Process @var{result} using @var{objectifiers} like
;; @code{result->object-alist}, but return a list of alists instead,
;; each corresponding to a tuple in @var{result}.
;;
(define (result->object-alists result objectifiers)
  (let ((oa (result->object-alist result objectifiers)))
    (let ((names (map car oa)))
      (apply map (lambda slice
                   (map cons names slice))
             (map cdr oa)))))

;;; postgres-resx.scm ends here
