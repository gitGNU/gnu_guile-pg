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

;; This module exports the procedures:
;;   (qcons-declare! category x . extra)
;;   (sql-quote string) => string
;;   (make-WHERE-tree condition) => tree
;;   (make-GROUP-BY-tree expressions) => tree
;;   (make-HAVING-tree conditions) => tree
;;   (make-ORDER-BY-tree orderings) => tree
;;   (make-SELECT/OUT-tree outs) => tree
;;   (make-FROM-tree froms) => tree
;;   (make-SELECT/FROM/OUT-tree froms outs) => tree
;;   (sql<-trees . trees) => string
;;   (sql-command<-trees . trees) => string

;;; Code:

(define-module (database postgres-qcons)
  #:export (qcons-declare!
            sql-quote
            make-comma-separated-tree
            make-WHERE-tree
            make-GROUP-BY-tree
            make-HAVING-tree
            make-ORDER-BY-tree
            make-SELECT/OUT-tree
            make-FROM-tree
            make-SELECT/FROM/OUT-tree
            parse+make-SELECT/tail-tree
            sql<-trees
            sql-command<-trees))


;;; mirroring bootstrap

(define *conditional-operations*        ; entry: NAME
  '(= < <= > >= <> !=
      all any in like
      ALL ANY IN LIKE))

(define *infix-operations*              ; entry: NAME
  (map identity *conditional-operations*))

;; Declare as part of @var{category} (a keyword) the symbol @var{x}.
;; @var{extra} information may be required for the particular category.
;; Currently, these categories are recognized:
;;
;; @table @code
;; @item #:infix-op
;; Render @code{(x A B ...)} as @code{( A x B x ...)}.
;; @end table
;;
(define (qcons-declare! category x . extra)
  (or (symbol? x) (error "not a symbol:" x))
  (case category
    ((#:infix-op)
     (set! *infix-operations*
           (cons x (delq! x *infix-operations*))))
    (else
     (error "bad category:" category))))


;; Return a new string made by preceding each single quote in string
;; @var{s} with a backslash, and prefixing and suffixing with single quote.
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
(define (sql-quote s)
  (or (string? s) (error "not a string:" s))
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

(define (fs s . args)
  (apply simple-format #f s args))

(define (maybe-dq sym)
  (let ((s (symbol->string sym)))
    ;; double quote unless table name is included;
    ;; this is to protect against a column name
    ;; that happens to be a keyword (e.g., `desc')
    (if (string-index s #\.)
        sym
        (fs "~S" s))))

(define (list-sep-proc sep)
  (lambda (proc ls . more-ls)
    (if (null? ls)
        ls
        (let* ((ls (if (null? more-ls)  ; optimization; not strictly necessary
                       (map proc ls)
                       (apply map proc ls more-ls)))
               (rv (list (car ls))))
          (let loop ((tail (cdr ls)) (tp rv))
            (cond ((null? tail) rv)
                  (else (set-cdr! tp (list sep (car tail)))
                        (loop (cdr tail) (cddr tp)))))))))

(define andsep   (list-sep-proc #:AND))
(define orsep    (list-sep-proc #:OR))
(define commasep (list-sep-proc ","))

;; Return a tree made by mapping @var{proc} over list @var{ls},
;; w/ elements separated by commas.  Optional third arg @var{parens?}
;; non-#f includes surrounding parentheses.  The rest of the args
;; are more lists, whose @sc{car}s are passed as additional args
;; to @var{proc}.
;;
;;-sig: (proc ls [parens? [more-ls...]])
;;
(define (make-comma-separated-tree proc ls . opts)
  (let* ((parens? (and (not (null? opts)) (car opts)))
         (more-ls (and (not (null? opts)) (cdr opts)))
         (L (if parens? "(" ""))
         (R (if parens? ")" "")))
    (list L (apply commasep proc ls more-ls) R)))

(define (expr tree)

  (define (add-noise! op rest)

    (define (when-then-else branch)
      (let ((val (car branch))
            (res (cadr branch)))
        (if (eq? 'else val)
            (list #:ELSE (expr res))
            (list #:WHEN (expr val)
                  #:THEN (expr res)))))

    (case op
      ((#:q*)
       (letrec ((q (lambda (x)
                     (cond ((string? x) (sql-quote x))
                           ((pair? x) (if (eq? #:q* (car x))
                                          ;; neutralize meddlesome kids
                                          (q (cadr x))
                                          (map q x)))
                           (else x)))))
         (expr (q (car rest)))))
      ((and)
       (list "(" (andsep expr rest) ")"))
      ((or)
       (list "(" (orsep expr rest) ")"))
      ((case)
       (list #:CASE (expr (car rest))
             (map when-then-else (cdr rest))
             #:END))
      ((cond)
       (list #:CASE
             (map when-then-else rest)
             #:END))
      ((if)
       (list #:CASE (expr (car rest))
             (map when-then-else
                  `((#t ,(cadr  rest))
                    (#f ,(caddr rest))))
             #:END))
      (else
       (if (memq op *infix-operations*)
           (list "(" ((list-sep-proc op) expr rest) ")")
           (list op "(" (commasep expr rest) ")")))))

  ;; do it!
  (cond ((eq? #t tree) "'t'")
        ((eq? #f tree) "'f'")
        ((pair? tree) (add-noise! (car tree) (cdr tree)))
        (else tree)))

;; Return a @dfn{where clause} tree for @var{condition}.
;;
(define (make-WHERE-tree condition)
  (list #:WHERE (expr condition)))

;; Return a @dfn{group-by clause} tree for @var{expressions} (a list).
;;
(define (make-GROUP-BY-tree expressions)
  (list #:GROUP-BY (commasep expr expressions)))

;; Return a @dfn{having clause} tree for @var{conditions} (a list).
;;
(define (make-HAVING-tree conditions)
  (list #:HAVING (commasep expr conditions)))

;; Return a @dfn{order-by clause} tree for @var{orderings} (a list).
;; Each element of @var{orderings} has the form: @code{(ORDFUNC EXPR)}.
;; If @var{ordfunc} is the symbol @code{<} or the keyword @code{#:ASC},
;; it is taken as "ASC".  Likewise, @code{>} or @code{#:DESC} is taken
;; as "DESC".
;;
(define (make-ORDER-BY-tree orderings)
  (list #:ORDER-BY
        (commasep (lambda (ord)
                    (or (and (pair? ord)
                             (or (integer? (cadr ord))
                                 (symbol? (cadr ord))))
                        (error "bad ordering:" ord))
                    (list
                     (if (integer? (cadr ord))
                         (cadr ord)
                         (expr (maybe-dq (cadr ord))))
                     (case (car ord)
                       ((< #:ASC #:asc) #:ASC)
                       ((> #:DESC #:desc) #:DESC)
                       (else (list #:USING (car ord))))))
                  orderings)))

;; Return a @dfn{select-outs clause} tree for @var{outs} (a list).
;; Each element of @var{outs} can take one of several forms:
;;
;; @table @code
;; @item TITLE
;; TITLE is a symbol, a column name possibly qualified with the table name.
;; For example, @code{foo.bar} means table @code{foo}, column @code{bar}.
;;
;; @item (TITLE . EXPR)
;; TITLE is a string to be used to name the output for the column
;; described by prefix-style expression EXPR.
;;
;; @item EXPR
;; EXPR is a prefix-style expression.  The name of the output column
;; described by EXPR is usually EXPR's outermost function or operator.
;; @end table
;;
;; For the present (to ease migration in client modules), EXPR may also be a
;; string, in which case it is passed through opaquely w/o further processing.
;; This support WILL BE REMOVED after 2005-12-31; DO NOT rely on it.
;;
(define (make-SELECT/OUT-tree outs)
  (define (expr-nostring x)             ; todo: zonk after 2005-12-31
    (if (string? x)
        x
        (expr x)))
  (commasep (lambda (x)
              (cond ((symbol? x) (maybe-dq x))
                    ((string? x) x)     ; todo: zonk after 2005-12-31
                    ((and (pair? x) (string? (car x)))
                     (list (expr-nostring (cdr x)) #:AS (fs "~S" (car x))))
                    ((pair? x) (expr-nostring x))
                    (else (error "bad out spec:" x))))
            outs))

;; Return a @dfn{from clause} tree for @var{froms} (a list).
;; Each element of @var{froms} is either TABLE-NAME, or a pair
;; @code{(ALIAS . TABLE-NAME)} (both symbols).
;;
(define (make-FROM-tree froms)
  (list #:FROM
        (commasep (lambda (x)
                    (cond ((symbol? x) x)
                          ((and (pair? x) (not (pair? (cdr x))))
                           (list (cdr x) (car x)))
                          (else (error "bad from spec:" x))))
                  froms)))

;; Return a @dfn{select/from/out combination clause} tree for
;; @var{froms} and @var{outs} (both lists).  In addition to the
;; constituent processing done by @code{make-SELECT/OUT-tree}
;; and @code{make-FROM-tree} on @var{outs} and @var{froms},
;; respectively, prefix a "SELECT" token.
;;
(define (make-SELECT/FROM/OUT-tree froms outs)
  (list #:SELECT
        (make-SELECT/OUT-tree outs)
        (make-FROM-tree froms)))

;; Return a @dfn{select tail} tree for @var{plist}, a list of
;; alternating keywords and sexps.  Currently, these keywords
;; are recognized:
;;
;; @table @code
;; @item #:where
;; Pass the associated sexp to @code{make-WHERE-tree}.
;;
;; @item #:group-by
;; Pass the associated sexp to @code{make-GROUP-BY-tree}.
;;
;; @item #:having
;; Pass the associated sexp to @code{make-HAVING-tree}.
;;
;; @item #:order-by
;; Pass the associated sexp to @code{make-ORDER-BY-tree}.
;; @end table
;;
;; Optional second arg @var{quote?} non-#f means to arrange to use the
;; @code{#:q*} operator for the structured expressions.
;;
;;-sig: (plist [quote?])
;;
(define (parse+make-SELECT/tail-tree plist . opts)
  (define (q-ok? kw)
    (case kw
      ((#:order-by) #f)
      (else #t)))
  (let ((quote? (and (not (null? opts)) (car opts)))
        (acc (list '())))
    (let loop ((ls plist) (tp acc))
      (if (null? ls)
          (cdr acc)                     ; rv
          (begin
            (set-cdr! tp (list ((case (car ls)
                                  ((#:where) make-WHERE-tree)
                                  ((#:group-by) make-GROUP-BY-tree)
                                  ((#:having) make-HAVING-tree)
                                  ((#:order-by) make-ORDER-BY-tree))
                                ((if (and quote? (q-ok? (car ls)))
                                     (lambda (x) (list #:q* x))
                                     identity)
                                 (cadr ls)))))
            (loop (cddr ls) (cdr tp)))))))

;; Return a string made from flattening @var{trees} (a list).
;; Each element of @var{trees} is either a string, symbol, number,
;; or keyword; or a tree as returned by one of the @code{make-*-tree}
;; procedures.
;;
(define (sql<-trees . trees)
  (define (out x)
    (cond ((keyword? x)
           (display
            (case x
              ((#:ORDER-BY) "\nORDER BY")
              ((#:FROM #:WHERE) (fs "\n~A" (keyword->symbol x)))
              (else (keyword->symbol x)))))
          ((or (string? x) (symbol? x) (number? x))
           (display x))
          ((pair? x)
           (out (car x))
           (out " ")
           (out (cdr x)))
          ((null? x))
          (else
           (error "bad tree component:" x))))
  ;; do it!
  (with-output-to-string
    (lambda ()
      (out trees))))

;; Return a string made from flattening @var{trees} (a list).
;; See @code{sql<-trees} for a description of @var{trees}.
;; The returned string ends with a semicolon.
;;
(define (sql-command<-trees . trees)
  (apply sql<-trees trees (list ";")))


;;; load-time actions

(for-each (lambda (x)
            (qcons-declare! #:infix-op x))
          '(+ - ~ * ||
              ;; todo: add here.
              ))

;;; postgres-qcons.scm ends here
