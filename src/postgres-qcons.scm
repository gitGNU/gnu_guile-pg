;;; postgres-qcons.scm --- construct SELECT queries

;; Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010, 2011 Thien-Thi Nguyen
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
;; along with Guile-PG; if not, write to the Free Software Foundation,
;; Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

;;; Commentary:

;; This module exports the procedures:
;;   (qcons-declare! category x . extra)
;;   (sql-pre string) => string
;;   (sql-pre? string) => boolean
;;   (sql-unpre string) => string
;;   (sql-quote string) => string
;;   (make-comma-separated-tree proc ls [parens? [more-ls...]]) => tree
;;   (make-WHERE-tree condition) => tree
;;   (make-GROUP-BY-tree expressions) => tree
;;   (make-HAVING-tree conditions) => tree
;;   (make-ORDER-BY-tree orderings) => tree
;;   (make-SELECT/COLS-tree cols) => tree
;;   (make-FROM-tree froms) => tree
;;   (make-SELECT/FROM/COLS-tree froms cols) => tree
;;   (parse+make-SELECT/tail-tree plist) => tree
;;   (parse+make-SELECT-tree composition cols/subs [tail...]) => tree
;;   (sql<-trees . trees) => string
;;   (sql-command<-trees . trees) => string

;;; Code:

(define-module (database postgres-qcons)
  #:export (qcons-declare!
            sql-pre
            sql-pre?
            sql-unpre
            sql-quote
            idquote
            make-comma-separated-tree
            make-WHERE-tree
            make-GROUP-BY-tree
            make-HAVING-tree
            make-ORDER-BY-tree
            make-SELECT/COLS-tree
            make-FROM-tree
            make-SELECT/FROM/COLS-tree
            parse+make-SELECT/tail-tree
            parse+make-SELECT-tree
            sql<-trees
            sql-command<-trees))


;;; mirroring bootstrap

;; Naming convention: Hash tables begin with "==".  They are
;; initialized on module load and updated by ‘qcons-declare!’.

(define-macro (define-hash name size pair? init)
  `(begin
     (define ,name (make-hash-table ,size))
     (for-each (lambda (x)
                 (hashq-set! ,name
                             ,(if pair? '(car x) 'x)
                             ,(if pair? '(cdr x) #t)))
               ,init)))

(define *conditional-operations*        ; entry: NAME
  '(= < <= > >= <> !=
      all any in
      like not-like ilike not-ilike
      ~~ !~~ ~~* !~~*
      similar not-similar
      ~ ~* !~ !~*))

(define *infix-operations*              ; entry: NAME
  (append! '(|| ||/ |/ / !! % ^ * - +
                     @ & | << >>
                       && &< &>
                       <-> <^ >^
                       ?- ?-| @-@ ?| ?||
                       @@ ~= <<= >>=)
           (map string->symbol '("#" "##" "?#"))
           *conditional-operations*))

(define-hash ==infix-operations 67 #f *infix-operations*)

(define *postfix-display-aliases*
  '((null?        . "IS NULL")
    (not-null?    . "IS NOT NULL")
    (true?        . "IS TRUE")
    (not-true?    . "IS NOT TRUE")
    (false?       . "IS FALSE")
    (not-false?   . "IS NOT FALSE")
    (unknown?     . "IS UNKNOWN")
    (not-unknown? . "IS NOT UNKNOWN")))

(define *display-aliases*               ; entry: (NAME . ALIAS)
  (append *postfix-display-aliases*
          '((not-like     . "NOT LIKE")
            (not-ilike    . "NOT ILIKE")
            (similar      . "SIMILAR TO")
            (not-similar  . "NOT SIMILAR TO"))))

(define-hash ==display-aliases 17 #t *display-aliases*)

(define *postfix-operations*            ; entry: NAME
  (append '(!)
          (map car *postfix-display-aliases*)))

(define-hash ==postfix-operations 11 #f *postfix-operations*)

(define-hash ==kw-over-commas 7 #f
  (append
   ;; string funcs
   '(convert overlay position substring trim)
   ;; date/time funcs
   '(extract)))

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
;;
;; @item #:keyword-args-ok
;; Render @code{(x A B ...)} as @code{x (A, B, ...)} if none
;; of @code{A}, @code{B}, @dots{} are keywords.  In the presence
;; of keywords, render it as @code{x (A B ...)}, without any commas
;; in the argument list.
;; @end table
;;
(define (qcons-declare! category x . extra)
  (or (symbol? x) (error "not a symbol:" x))
  (case category
    ((#:infix)
     (hashq-set! ==infix-operations x #t))
    ((#:postfix)
     (hashq-set! ==postfix-operations x #t))
    ((#:display-alias)
     (hashq-set! ==display-aliases x (car extra)))
    ((#:keyword-args-ok)
     (hashq-set! ==kw-over-commas x #t))
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

;; Return @code{#t} if @var{string} is marked as preformatted.
;;
(define (sql-pre? string)
  (--preformatted string))

;; Return @var{string}, undoing the effect of @code{sql-pre}.
;;
(define (sql-unpre string)
  (or (string? string) (error "not a string:" string))
  (set! (--preformatted string) #f)
  string)

;; Return a new string made by doubling each single-quote in string
;; @var{s}, and prefixing and suffixing with single-quote.
;; The returned string is marked by @code{sql-pre}.  For example:
;;
;; @lisp
;; (define bef "ab'cd")
;; (define aft (sql-quote bef))
;; aft @result{} "'ab''cd'"
;; (map string-length (list bef aft)) @result{} (5 8)
;; (map sql-pre? (list bef aft)) @result{} (#f #t)
;; @end lisp
;;
;; Note that this procedure used to return internal single-quote
;; characters prefixed with a backslash, which is acceptable by
;; PostgreSQL (given certain runtime parameter settings), but not
;; standards conforming.  The current (as of Guile-PG 0.38) behavior
;; is standards-conforming, upward compatible, and avoids futzing with
;; the runtime parameters.
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
                (string-set! rv end #\'))
            (loop (1+ end) tail))))))

(define (fs s . args)
  (apply simple-format #f s args))

;; Return the @dfn{quoted identifier} form of @var{id}, a string
;; or symbol.  The returned string is marked by @code{sql-pre}.
;; For example:
;;
;; @lisp
;; (define (try x)
;;   (display (idquote x))
;;   (newline))
;;
;; (try 'abcd)       @print{} "abcd"
;; (try 'ab.cd)      @print{} "ab"."cd"
;; (try 'abcd[xyz])  @print{} "abcd"[xyz]
;; (try 'ab.cd[xyz]) @print{} "ab"."cd"[xyz]
;;
;; ;; Special case: only * after dot.
;; (try 'ab.*)       @print{} "ab".*
;; @end lisp
;;
;; Note that PostgreSQL case-folding for non-quoted identifiers
;; is nonstandard.  The PostgreSQL documentation says:
;;
;; @quotation
;; If you want to write portable applications you are advised
;; to always quote a particular name or never quote it.
;; @cite{Section 4.1.1, Identifiers and Keywords}
;; @end quotation
;;
;; The qcons module (@pxref{Query Construction}) uses @code{idquote}
;; internally extensively.
;;
(define (idquote id)
  (sql-pre
   (let* ((s (if (symbol? id)
                 (symbol->string id)
                 id))
          (ra (string-index s #\[))
          (dot (string-index s #\.)))
     (cond
      ;; Fast path; no complications.
      ((not (or ra dot))
       (object->string s))
      ;; Just dot (ab.cd => "ab"."cd", but ab.* => "ab".*).
      ((and dot (not ra))
       (fs "~S.~S"
           (substring s 0 dot)
           (let ((after (substring s (1+ dot))))
             (if (string=? "*" after)
                 '*
                 after))))
      ;; Just array (abcd[xyz] => "abcd"[xyz]).
      ((and ra (not dot))
       (fs "~S~A"
           (substring s 0 ra)
           (substring s ra)))
      ;; Both dot and array (ab.cd[xyz] => "ab"."cd"[xyz]).
      (#t
       (fs "~S.~S~A"
           (substring s 0 dot)
           (substring s (1+ dot) ra)
           (substring s (ra))))))))

(define (maybe-dq sym)
  ;; Hmmm, why not use ‘idquote’ also for this?
  (if (eq? '* sym)
      sym
      (idquote sym)))

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
(define commasep (list-sep-proc #:%COMMA))

(define (as one two)
  (list one #:AS two))

(define (paren . x)
  `(#:%LPAREN ,@x #:%RPAREN))

;; Return a tree made by mapping @var{proc} over list @var{ls},
;; with elements separated by commas.  Optional third arg @var{parens?}
;; non-@code{#f} includes surrounding parentheses.  The rest of the args
;; are more lists, whose @sc{car}s are passed as additional args
;; to @var{proc}.
;;
;;-args: (- 0 2 parens? more-ls)
;;
(define (make-comma-separated-tree proc ls . opts)
  ((if (and (not (null? opts)) (car opts))
       paren
       identity)
   (if (and (not (null? opts)) (not (null? (cdr opts))))
       (apply commasep proc ls (cdr opts))
       (commasep proc ls))))

(define any/all-rx (make-regexp "^a(ny)|(ll)--"))

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
       (paren (andsep expr rest)))
      ((or)
       (paren (orsep expr rest)))
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
      ((::)
       (list #:CAST (paren (as (expr (cadr rest)) (car rest)))))
      ;; special constructs
      ((in/set)
       (list (expr (car rest)) #:IN
             (paren (commasep expr (cdr rest)))))
      ((between)
       (paren (expr (car rest)) #:BETWEEN
              (expr (cadr rest)) #:AND (expr (caddr rest))))
      (else
       (cond ((hashq-ref ==infix-operations op)
              (paren ((list-sep-proc op) expr rest)))
             ((hashq-ref ==postfix-operations op)
              (paren (expr (car rest)) op))
             ((and (hashq-ref ==kw-over-commas op)
                   (or-map keyword? rest))
              (list op (paren (map (lambda (x)
                                     (if (keyword? x)
                                         x
                                         (expr x)))
                                   rest))))
             ((regexp-exec any/all-rx (symbol->string op))
              => (lambda (m)
                   ;; FIXME: Debug :-/ and use ‘match:suffix’.
                   (let ((s (vector-ref m 0)))
                     (paren (expr (car rest))
                            (sql-pre (substring s 5))
                            (if (char=? #\n (string-ref s 1))
                                #:ANY
                                #:ALL)
                            (paren (cadr rest))))))
             (else
              (list op (paren (commasep expr rest))))))))

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
;; it is taken as @code{ASC}.  Likewise, @code{>} or @code{#:DESC} is taken
;; as @code{DESC}.
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
;; @item @var{title}
;; A symbol, a column name possibly qualified with the table name.
;; For example, @code{foo.bar} means table @code{foo}, column @code{bar}.
;;
;; @item (@var{title} . @var{expr})
;; @var{title} is a string to be used to name the output for the column
;; described by prefix-style expression @var{expr}.
;;
;; @item @var{expr}
;; A prefix-style expression.  The name of the output column described by
;; @var{expr} is usually @var{expr}'s outermost function or operator.
;; @end table
;;
(define (make-SELECT/COLS-tree cols)
  (commasep (lambda (x)
              (cond ((number? x) x)
                    ((symbol? x) (maybe-dq x))
                    ((and (pair? x) (string? (car x)))
                     (as (expr (cdr x))
                         (sql-pre (fs "~S" (car x)))))
                    ((pair? x) (expr x))
                    (else (error "bad col spec:" x))))
            cols))

;; Return a @dfn{from clause} tree for @var{items} (a list).
;; Each element of @var{items}, a @dfn{from-item},
;; can take one of several forms:
;;
;; @table @code
;; @item @var{table-name}
;; A symbol.
;; @item (@var{alias} . @var{table-name})
;; A pair of symbols.
;; @item (@var{jtype} [@var{jcondition}] @var{left-from} @var{right-from})
;; This is a @dfn{join clause},
;; where @var{jtype} is a keyword, one of @code{#:join},
;; @code{#:left-join}, @code{#:right-join}, @code{#:full-join};
;; and @var{left-from} and @var{right-from} are each a single
;; from-item to be handled recursively.
;; If @var{jtype} is @code{#:join}, @var{jcondition} must be omitted.
;; Otherwise, it is one of:
;;
;; @table @code
;; @item #:natural
;; @item (#:using @var{col1} @var{col2}...)
;; @item (#:on @var{pexp})
;; @end table
;; @end table
;;
(define (make-FROM-tree items)
  (define (one x)
    (cond ((symbol? x) (maybe-dq x))
          ((and (pair? x) (keyword? (car x))) (hairy x))
          ((and (pair? x) (symbol? (cdr x)))
           (as (maybe-dq (cdr x)) (maybe-dq (car x))))
          (else (error "bad from spec:" x))))
  (define (hairy x)
    (let ((rest (cdr x)))
      (define (parse+make-join-tree type)
        (define (bad!)
          (error "bad join spec:" rest))
        (or (pair? rest) (bad!))
        (let ((nat #f)
              (jcond (car rest)))
          (cond ((not jcond))
                ((and (keyword? jcond) (eq? #:natural jcond))
                 (set! nat #:NATURAL)
                 (set! jcond #f))
                ((pair? jcond)
                 (or (pair? (cdr jcond)) (bad!))
                 (set! jcond
                       (case (car jcond)
                         ((#:using)
                          (cons #:USING (paren (commasep one (cdr jcond)))))
                         ((#:on)
                          (list #:ON (expr (cadr jcond))))
                         (else (bad!)))))
                (else (bad!)))
          (set! rest (cdr rest))
          (paren (one (car rest)) (or nat '()) type #:JOIN
                 (one (cadr rest)) (or jcond '()))))
      (case (car x)
        ((#:join) (set! rest (cons #f rest)) (parse+make-join-tree '()))
        ((#:left-join)  (parse+make-join-tree #:LEFT))
        ((#:right-join) (parse+make-join-tree #:RIGHT))
        ((#:full-join)  (parse+make-join-tree #:FULL))
        (else (error "unrecognized:" (car x))))))
  (list #:FROM (commasep one items)))

;; Return a @dfn{select/from/col combination clause} tree for
;; @var{froms} and @var{cols} (both lists).  In addition to the
;; constituent processing done by @code{make-SELECT/COLS-tree}
;; and @code{make-FROM-tree} on @var{cols} and @var{froms},
;; respectively, prefix a @code{SELECT} token.  If @var{froms} is
;; @code{#f}, it is omitted.
;;
(define (make-SELECT/FROM/COLS-tree froms cols)
  (list #:SELECT
        (make-SELECT/COLS-tree cols)
        (if froms
            (make-FROM-tree froms)
            '())))

;; Return a @dfn{select tail} tree for @var{plist}, a list of
;; alternating keywords and related expressions.  These subsequences
;; are recognized:
;;
;; @table @code
;; @item #:from x
;; Pass @var{x} to @code{make-FROM-tree}.
;;
;; @item #:where x
;; Pass @var{x} to @code{make-WHERE-tree}.
;;
;; @item #:group-by x
;; Pass @var{x} to @code{make-GROUP-BY-tree}.
;;
;; @item #:having x
;; Pass @var{x} to @code{make-HAVING-tree}.
;;
;; @item #:order-by x
;; Pass @var{x} to @code{make-ORDER-BY-tree}.
;;
;; @item #:limit n
;; @itemx #:offset n
;; Arrange for the tree to include @code{LIMIT n}
;; and/or @code{OFFSET n}.  @var{n} is an integer.
;; @end table
;;
;; If an expression (@var{x} or @var{n}) is @code{#f}, omit the associated
;; clause completely from the returned tree.
;;
(define (parse+make-SELECT/tail-tree plist)
  (let ((acc (list '())))
    (let loop ((ls plist) (tp acc))
      (if (null? ls)
          (cdr acc)                     ; rv
          (let* ((kw (car ls))
                 (mk (case kw
                       ((#:from)     make-FROM-tree)
                       ((#:where)    make-WHERE-tree)
                       ((#:group-by) make-GROUP-BY-tree)
                       ((#:having)   make-HAVING-tree)
                       ((#:order-by) make-ORDER-BY-tree)
                       ((#:limit)    (lambda (n) (list #:LIMIT n)))
                       ((#:offset)   (lambda (n) (list #:OFFSET n)))
                       (else         (error "bad keyword:" kw)))))
            (and (null? (cdr ls))
                 (error "lonely keyword:" kw))
            (loop (cddr ls)
                  (cond ((cadr ls)
                         => (lambda (x)
                              (set-cdr! tp (list (mk x)))
                              (cdr tp)))
                        (else tp))))))))

;; Return a @dfn{select} tree of @var{composition} for @var{cols/subs}
;; and @var{tail}.
;;
;; If @var{composition} is @code{#t}, @var{cols/subs} is passed directly to
;; @code{make-SELECT/COLS-tree}.  Otherwise, it should be one of:
;;
;; @lisp
;; #:union       #:intersect       #:except
;; #:union-all   #:intersect-all   #:except-all
;; @end lisp
;;
;; @var{cols/subs} is a list of sublists taken as arguments to
;; @code{parse+make-SELECT-tree} (applied recursively to each sublist),
;; and finally combined by @var{composition}.
;;
;; @var{tail} is passed directly to @code{parse+make-SELECT/tail-tree}.
;;
(define (parse+make-SELECT-tree composition cols/subs . tail)
  (define (compose . type)
    ((if (null? tail)
         identity
         paren)
     ((list-sep-proc type)
      (lambda (x) (paren (apply parse+make-SELECT-tree x)))
      cols/subs)))
  (list (case composition
          ((#t) (list #:SELECT (make-SELECT/COLS-tree cols/subs)))
          ((#:union)        (compose #:UNION))
          ((#:union-all)    (compose #:UNION #:ALL))
          ((#:intersect)    (compose #:INTERSECT))
          ((#:intersect-all)(compose #:INTERSECT #:ALL))
          ((#:except)       (compose #:EXCEPT))
          ((#:except-all)   (compose #:EXCEPT #:ALL))
          (else (error "bad composition:" composition)))
        (parse+make-SELECT/tail-tree tail)))

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
              ((#:%LPAREN) "(")
              ((#:%RPAREN) ")")
              ((#:%COMMA)  ",")
              ((#:%SEMIC)  ";")
              ((#:ORDER-BY) "\nORDER BY")
              ((#:GROUP-BY) "\nGROUP BY")
              ((#:FROM #:WHERE) (fs "\n~A" (keyword->symbol x)))
              (else (keyword->symbol x)))))
          ((symbol? x)
           (display (hashq-ref ==display-aliases x x)))
          ((or (string? x) (number? x))
           (display x))
          ((pair? x)
           (out (car x))
           (or (null? (cdr x))
               (eq? #:%LPAREN (car x))
               (memq (cadr x) '(#:%RPAREN #:%COMMA #:%SEMIC))
               (display " "))
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
  (apply sql<-trees trees (list #:%SEMIC)))

;;; postgres-qcons.scm ends here
