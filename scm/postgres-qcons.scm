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
;;   (make-SELECT/COLS-tree cols) => tree
;;   (make-FROM-tree froms) => tree
;;   (make-SELECT/FROM/COLS-tree froms cols) => tree
;;   (sql<-trees . trees) => string
;;   (sql-command<-trees . trees) => string

;;; Code:

(define-module (database postgres-qcons)
  #:export (qcons-declare!
            sql-pre
            sql-pre?
            sql-unpre
            sql-quote
            make-comma-separated-tree
            make-WHERE-tree
            make-GROUP-BY-tree
            make-HAVING-tree
            make-ORDER-BY-tree
            make-SELECT/COLS-tree
            make-FROM-tree
            make-SELECT/FROM/COLS-tree
            parse+make-SELECT/tail-tree
            sql<-trees
            sql-command<-trees))


;;; mirroring bootstrap

(define *conditional-operations*        ; entry: NAME
  '(= < <= > >= <> !=
      all any in like))

(define *infix-operations*              ; entry: NAME
  (append! '(|| / * ~ - +)
           *conditional-operations*))

(define *display-aliases*               ; entry: (NAME . ALIAS)
  '((null?        . "IS NULL")
    (not-null?    . "IS NOT NULL")
    (true?        . "IS TRUE")
    (not-true?    . "IS NOT TRUE")
    (false?       . "IS FALSE")
    (not-false?   . "IS NOT FALSE")
    (unknown?     . "IS UNKNOWN")
    (not-unknown? . "IS NOT UNKNOWN")))

(define *postfix-operations*            ; entry: NAME
  (map car *display-aliases*))

;; Declare as part of @var{category} (a keyword) the symbol @var{x}.
;; @var{extra} information may be required for the particular category.
;; Currently, these categories are recognized:
;;
;; @table @code
;; @item #:infix
;; Render @code{(x A B ...)} as @code{( A x B x ...)}.
;;
;; @item #:postfix
;; Render @code{(x A)} as @code{( A x )}.
;;
;; @item #:display-alias
;; Render @code{x} as something else.  @var{extra} is a string
;; that specifies what to display instead of @var{x}.  For example,
;; @code{null?} and @code{not-null?} are pre-declared to render as
;; @code{IS NULL} and @code{IS NOT NULL}, respectively.
;; @end table
;;
(define (qcons-declare! category x . extra)
  (or (symbol? x) (error "not a symbol:" x))
  (case category
    ((#:infix)
     (set! *infix-operations*
           (cons x (delq! x *infix-operations*))))
    ((#:postfix)
     (set! *postfix-operations*
           (cons x (delq! x *postfix-operations*))))
    ((#:display-alias)
     (set! *display-aliases*
           (assq-set! *display-aliases* x (car extra))))
    (else
     (error "bad category:" category))))


(define --preformatted (make-object-property))

;; Return @var{string} marked as @dfn{preformatted}.
;; This inhibits certain types of processing when passed
;; through the other procedures defined in this module.
;; Repeated calls do not nest.
;;
(define (sql-pre string)
  (or (string? string) (error "not a string:" string))
  (set! (--preformatted string) #t)
  string)

;; Return #t if @var{string} is marked as preformatted.
;;
(define (sql-pre? string)
  (--preformatted string))

;; Return @var{string}, undoing the effect of @code{sql-pre}.
;;
(define (sql-unpre string)
  (or (string? string) (error "not a string:" string))
  (set! (--preformatted string) #f)
  string)

;; Return a new string made by preceding each single quote in string
;; @var{s} with a backslash, and prefixing and suffixing with single quote.
;; The returned string is marked by @code{sql-pre}.  For example:
;;
;; @lisp
;; (define bef "ab'cd")
;; (define aft (sql-quote bef))
;; aft @result{} "'ab\\'cd'"
;; (map string-length (list bef aft)) @result{} (5 8)
;; (map sql-pre? (list bef aft)) @result{} (#f #t)
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
          (sql-pre rv)                  ; rv
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
  (if (eq? '* sym)
      sym
      (let ((s (symbol->string sym)))
        ;; double quote unless table name is included;
        ;; this is to protect against a column name
        ;; that happens to be a keyword (e.g., `desc')
        (if (string-index s #\.)
            sym
            (sql-pre (fs "~S" s))))))

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
       (cond ((memq op *infix-operations*)
              (list "(" ((list-sep-proc op) expr rest) ")"))
             ((memq op *postfix-operations*)
              (list "(" (expr (car rest)) op ")"))
             (else
              (list op "(" (commasep expr rest) ")"))))))

  ;; do it!
  (cond ((eq? #t tree) (sql-pre "'t'"))
        ((eq? #f tree) (sql-pre "'f'"))
        ((string? tree) (if (--preformatted tree) tree (sql-quote tree)))
        ((symbol? tree) (maybe-dq tree))
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
                    (or (pair? ord)
                        (error "bad ordering:" ord))
                    (list
                     (let ((by (cadr ord)))
                       (cond ((integer? by) by)
                             ((symbol? by) (maybe-dq by))
                             (else (expr by))))
                     (case (car ord)
                       ((< #:ASC #:asc) #:ASC)
                       ((> #:DESC #:desc) #:DESC)
                       (else (list #:USING (car ord))))))
                  orderings)))

;; Return a @dfn{select-cols clause} tree for @var{cols} (a list).
;; Each element of @var{cols} can take one of several forms:
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
(define (make-SELECT/COLS-tree cols)
  (define (expr-nostring x)             ; todo: zonk after 2005-12-31
    (if (string? x)
        x
        (expr x)))
  (commasep (lambda (x)
              (cond ((number? x) x)
                    ((symbol? x) (maybe-dq x))
                    ((string? x) (sql-pre x)) ; todo: zonk after 2005-12-31
                    ((and (pair? x) (string? (car x)))
                     (list (expr-nostring (cdr x))
                           #:AS (sql-pre (fs "~S" (car x)))))
                    ((pair? x) (expr-nostring x))
                    (else (error "bad col spec:" x))))
            cols))

;; Return a @dfn{from clause} tree for @var{froms} (a list).
;; Each element of @var{froms} is either TABLE-NAME, or a pair
;; @code{(ALIAS . TABLE-NAME)} (both symbols).  IF @var{froms}
;; is #f, return a null tree (the empty list).
;;
(define (make-FROM-tree froms)
  (if froms
      (list #:FROM
            (commasep (lambda (x)
                        (cond ((symbol? x) (maybe-dq x))
                              ((and (pair? x) (not (pair? (cdr x))))
                               (list (maybe-dq (cdr x)) (maybe-dq (car x))))
                              (else (error "bad from spec:" x))))
                      froms))
      '()))

;; Return a @dfn{select/from/col combination clause} tree for
;; @var{froms} and @var{cols} (both lists).  In addition to the
;; constituent processing done by @code{make-SELECT/COLS-tree}
;; and @code{make-FROM-tree} on @var{cols} and @var{froms},
;; respectively, prefix a "SELECT" token.
;;
(define (make-SELECT/FROM/COLS-tree froms cols)
  (list #:SELECT
        (make-SELECT/COLS-tree cols)
        (make-FROM-tree froms)))

;; Return a @dfn{select tail} tree for @var{plist}, a list of
;; alternating keywords and sexps.  These subsequences are recognized:
;;
;; @table @code
;; @item #:where sexp
;; Pass @var{sexp} to @code{make-WHERE-tree}.
;;
;; @item #:group-by sexp
;; Pass @var{sexp} to @code{make-GROUP-BY-tree}.
;;
;; @item #:having sexp
;; Pass @var{sexp} to @code{make-HAVING-tree}.
;;
;; @item #:order-by sexp
;; Pass @var{sexp} to @code{make-ORDER-BY-tree}.
;;
;; @item #:limit n
;; Arrange for the tree to include @code{LIMIT n}.
;; @var{n} is an integer.
;; @end table
;;
(define (parse+make-SELECT/tail-tree plist)
  (let ((acc (list '())))
    (let loop ((ls plist) (tp acc))
      (if (null? ls)
          (cdr acc)                     ; rv
          (let* ((kw (car ls))
                 (mk (case kw
                       ((#:where)    make-WHERE-tree)
                       ((#:group-by) make-GROUP-BY-tree)
                       ((#:having)   make-HAVING-tree)
                       ((#:order-by) make-ORDER-BY-tree)
                       ((#:limit)    (lambda (n) (list #:LIMIT n)))
                       (else         (error "bad keyword:" kw)))))
            (set-cdr! tp (list (mk (if (null? (cdr ls))
                                       (error "lonely keyword:" kw)
                                       (cadr ls)))))
            (loop (cddr ls) (cdr tp)))))))

;; Return a string made from flattening @var{trees} (a list).
;; Each element of @var{trees} is either a string, symbol, number,
;; or keyword; or a tree as returned by one of the @code{make-*-tree}
;; procedures.  The returned string is marked by @code{sql-pre}.
;;
(define (sql<-trees . trees)
  (define (out x)
    (cond ((keyword? x)
           (display
            (case x
              ((#:ORDER-BY) "\nORDER BY")
              ((#:GROUP-BY) "\nGROUP BY")
              ((#:FROM #:WHERE) (fs "\n~A" (keyword->symbol x)))
              (else (keyword->symbol x)))))
          ((symbol? x)
           (display (or (assq-ref *display-aliases* x) x)))
          ((or (string? x) (number? x))
           (display x))
          ((pair? x)
           (out (car x))
           (out " ")
           (out (cdr x)))
          ((null? x))
          (else
           (error "bad tree component:" x))))
  ;; do it!
  (sql-pre
   (with-output-to-string
     (lambda ()
       (out trees)))))

;; Return a string made from flattening @var{trees} (a list).
;; See @code{sql<-trees} for a description of @var{trees}.
;; The returned string ends with a semicolon, and is marked
;; by @code{sql-pre}.
;;
(define (sql-command<-trees . trees)
  (apply sql<-trees trees (list ";")))

;;; postgres-qcons.scm ends here
