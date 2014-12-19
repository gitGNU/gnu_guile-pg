;;; postgres-col-defs.scm --- column definitions

;; Copyright (C) 2002, 2003, 2004, 2005, 2006,
;;   2007, 2008, 2009, 2011, 2012 Thien-Thi Nguyen
;;
;; This file is part of Guile-PG.
;;
;; Guile-PG is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3 of the License, or
;; (at your option) any later version.
;;
;; Guile-PG is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with Guile-PG.  If not, see <http://www.gnu.org/licenses/>.

;;; Code:

(define-module (database postgres-col-defs)
  #:export (column-name
            type-name
            type-options
            validate-def
            objectifiers
            stringifiers)
  #:use-module ((database postgres-types)
                #:select (type-objectifier
                          type-stringifier)))

;; Extract column name, a symbol, from @var{def}.
;;
(define (column-name def)
  (car def))

;; Extract type name, a symbol, from @var{def}.
;;
(define (type-name def)
  (let ((type-info (cdr def)))
    (if (pair? type-info)
        (car type-info)
        type-info)))

;; Extract type options, a list, from @var{def}.
;; Each option element is either a string (possibly with embedded spaces),
;; or a sub-list of numbers and/or symbols.  Typically the sub-list, if any,
;; will be the first option element.
;;
(define (type-options def)
  (let ((type-info (cdr def)))
    (if (pair? type-info)
        (cdr type-info)
        '())))

;; Check @var{obj} and signal error if it does not appear to be a well-formed
;; def.  Check that @var{obj} has a structure amenable to extraction of
;; components using @code{column-name} and @code{type-name}: The name must be
;; a symbol using only non-whitespace characters; the type must
;; be a symbol.  Optional second arg @var{typecheck} is a procedure that takes
;; the type (a symbol) and can do further checks on it.  It should return
;; non-@code{#f} to indicate success.
;;
;;-args: (- 1 0)
;;
(define (validate-def obj . typecheck)
  (or (and (pair? obj)
           (let ((col-name (column-name obj)))
             (and (symbol? col-name)
                  (let ((s (symbol->string col-name)))
                    (= (string-length s)
                       (apply + (map (lambda (c)
                                       (if (char-whitespace? c)
                                           0 1))
                                     (string->list s)))))))
           (pair? (cdr obj))
           (let ((col-type (type-name obj)))
             (and (symbol? col-type)
                  (or (null? typecheck)
                      ((car typecheck) col-type)))))
      (error "malformed def:" obj)))

;; Return a list of objectifiers associated with the types in @var{defs}.
;;
(define (objectifiers defs)
  (map type-objectifier
       (map type-name defs)))

;; Return a list of stringifiers associated with the types in @var{defs}.
;;
(define (stringifiers defs)
  (map type-stringifier
       (map type-name defs)))

;;; postgres-col-defs.scm ends here
