;;; postgres-col-defs.scm --- column definitions

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

;; This module exports the procs:
;;   (column-name DEF) => symbol
;;   (type-name DEF) => symbol
;;   (type-options DEF) => list of strings
;;   (objectifiers DEFS) => list of objectifier procedures
;;   (stringifiers DEFS) => list of stringifier procedures
;;
;; DEF is a single column definition.  DEFS is a list of column definitions.

;;; Code:

(define-module (database postgres-col-defs)
  #:use-module ((database postgres-types)
                #:select (dbcoltype-lookup
                          dbcoltype:objectifier
                          dbcoltype:stringifier))
  #:export (column-name
            type-name
            type-options
            objectifiers
            stringifiers))

;; Extract column name from @var{def}.
(define (column-name def)
  (car def))

;; Extract type name from @var{def}.
(define (type-name def)
  (let ((type-info (cdr def)))
    (if (pair? type-info)
        (car type-info)
        type-info)))

;; Extract type options from @var{def}.
(define (type-options def)
  (let ((type-info (cdr def)))
    (if (pair? type-info)
        (cdr type-info)
        '())))

;; Return a list of objectifiers associated with the types in @var{defs}.
(define (objectifiers defs)
  (map dbcoltype:objectifier
       (map dbcoltype-lookup
            (map type-name defs))))

;; Return a list of stringifiers associated with the types in @var{defs}.
(define (stringifiers defs)
  (map dbcoltype:stringifier
       (map dbcoltype-lookup
            (map type-name defs))))

;;; postgres-col-defs.scm ends here
