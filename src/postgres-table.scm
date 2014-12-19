;;; postgres-table.scm --- abstract manipulation of a single PostgreSQL table

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

(define-module (database postgres-table)
  #:export (pgtable-manager
            pgtable-worker
            compile-outspec)
  #:use-module ((srfi srfi-1)
                #:select (find
                          filter-map))
  #:use-module ((database postgres)
                #:select (pg-connection?
                          pg-connectdb
                          pg-finish
                          pg-exec pg-ntuples pg-nfields
                          pg-fname pg-getvalue))
  #:use-module ((database postgres-types)
                #:select (type-registered?
                          type-stringifier
                          type-objectifier
                          type-default))
  #:use-module ((database postgres-col-defs)
                #:select (column-name
                          type-name
                          type-options
                          validate-def
                          objectifiers)
                #:prefix def:)
  #:use-module ((database postgres-qcons)
                #:select (sql-pre
                          sql-pre?
                          sql-quote
                          string-xrep
                          (make-comma-separated-tree . cseptree)
                          make-WHERE-tree
                          make-SELECT/FROM/COLS-tree
                          parse+make-SELECT/tail-tree
                          sql<-trees
                          sql-command<-trees))
  #:use-module ((database postgres-resx)
                #:select (result->object-alist
                          result->object-alists
                          result->object-rows))
  #:re-export (sql-pre))

;;; support

(define compiled-outspec? (make-object-property))
(define ohints (make-object-property))

;; Return a @dfn{compiled outspec object} from @var{spec} and @var{defs},
;; suitable for passing to the @code{select} choice of @code{pgtable-manager}.
;; @var{defs} is the same as that for @code{pgtable-manager}.  @var{spec} can
;; be one of the following:
;;
;; @itemize
;; @item a column name (a symbol)
;;
;; @item @code{#t}, which means all columns (notionally equivalent to "*")
;;
;; @item a list of column specifications each of which is either a column
;; name, or has the form @code{(@var{type} @var{title} @var{expr})}, where:
;;
;; @itemize
;; @item @var{expr} is a prefix-style expression to compute for the column
;; (@pxref{Query Construction})
;;
;; @item @var{title} is the title (string) of the column, or @code{#f}
;;
;; @item @var{type} is a column type (symbol) such as @code{int4},
;; or @code{#f} to mean @code{text}, or @code{#t} to mean
;; use the type associated with the column named in @var{expr},
;; or the pair @code{(#t . name)} to mean use the type associated
;; with column @var{name}
;; @end itemize
;; @end itemize
;;
;; A "bad select part" error results if specified columns or types do not
;; exist, or if other syntax errors are found in @var{spec}.
;;
(define (compile-outspec spec defs)

  (define (bad-select-part s)
    (error "bad select part:" s))

  (let ((objectifiers '()))

    (define (push! type)
      (or (type-registered? type)
          (bad-select-part type))
      (set! objectifiers
            (cons (type-objectifier type)
                  objectifiers)))

    (define (munge x)
      (cond ((symbol? x)
             (or (and=> (assq x defs)
                        (lambda (def)
                          (push! (def:type-name def))))
                 (bad-select-part x))
             x)
            ((and (list? x) (= 3 (length x)))
             (apply-to-args
              x (lambda (type title expr)
                  (and (string? expr) (bad-select-part expr))
                  (push! (cond ((symbol? type)
                                type)
                               ((eq? #f type)
                                'text)
                               ((and (eq? #t type)
                                     (or (assq expr defs)
                                         (bad-select-part expr)))
                                => def:type-name)
                               ((and (pair? type)
                                     (eq? #t (car type))
                                     (symbol? (cdr type))
                                     (or (assq (cdr type) defs)
                                         (bad-select-part (cdr type))))
                                => def:type-name)
                               (else
                                (bad-select-part type))))
                  (if title (cons title expr) expr))))
            (else
             (bad-select-part x))))

    (let* ((s (map munge (cond ((eq? #t spec) (map def:column-name defs))
                               ((pair? spec) spec)
                               (else (list spec)))))
           ;; a "compiled outspec"
           (rv (cons (reverse! objectifiers) s)))
      (set! (compiled-outspec? rv) #t)
      rv)))

(define (compiled-outspec?-extract obj)
  (and (compiled-outspec? obj) obj))

;;; dispatch

;; Return a closure that manages a table specified by
;; @var{db-spec} @var{table-name} @var{defs}.
;;
;; @var{db-spec} can either be a string simply naming the database to use, a
;; string comprised of space-separated @code{var=val} pairs, an empty string,
;; or an already existing connection.  @ref{Database Connections}.
;;
;; @var{table-name} is a string naming the table to be managed.
;;
;; @var{defs} is an alist of column definitions of the form
;; @code{(@var{name} @var{type} [@var{option}@dots{}])},
;; with @var{name} and @var{type} symbols and each @var{option} a string.
;; An old format w/o options is recognized also, but deprecated; support
;; for it will go away in a future release.
;;
;; The closure accepts a single keyword (or symbol) arg @var{choice} and
;; returns a procedure.  Here are the accepted keywords along w/ the args
;; (if any) taken by the returned procedure.
;;
;; @example
;; @findex drop
;; * #:drop
;; @findex create
;; * #:create
;; @findex insert-values
;; * #:insert-values [@var{data} ...]
;; @findex insert-col-values
;; * #:insert-col-values @var{cols} [@var{data} ...]
;; @findex insert-alist
;; * #:insert-alist @var{alist}
;; @findex delete-rows
;; * #:delete-rows @var{where-condition}
;; @findex update-col
;; * #:update-col @var{cols} @var{data} @var{where-condition}
;; @findex update-col-alist
;; * #:update-col-alist @var{alist} @var{where-condition}
;; @findex select
;; * #:select @var{outspec} [@var{rest-clauses} ...]
;; @findex tuples-result->object-alist
;;   #:tuples-result->object-alist @var{res}
;; @findex tuples-result->alists
;;   #:tuples-result->alists @var{res}
;; @findex tuples-result->rows
;;   #:tuples-result->rows @var{res}
;; @findex trace-exec
;;   #:trace-exec @var{oport}
;; @findex finish
;;   #:finish
;; @findex k
;;   #:k @var{var}
;; @end example
;;
;; The starred (*) procedures return whatever @code{pg-exec} returns for
;; that type of procedure.  Arguments can be:
;;
;; @table @var
;; @item var
;; A keyword, one of @code{#:table-name}, @code{#:col-defs}, or
;; @code{#:connection}.
;;
;; @item data
;; One or more Scheme objects.
;;
;; @item cols
;; Either a list of column names (symbols), or a single string
;; of comma-delimited column names.
;;
;; @item where-condition
;; A prefix-style expression.
;;
;; @item outspec
;; Either the result of @code{compile-outspec}, or a spec
;; that @code{compile-outspec} can process to produce such a result.
;;
;; @item rest-clauses
;; Zero or more prefix-style expressions.
;;
;; @item res
;; A tuples result, as returned by @code{pg-exec}
;; (assuming no error occurred).
;;
;; @item oport
;; An output port to write the @code{pg-exec} command to
;; immediately prior to executing it.  Use @code{#f} to disable tracing.
;; @end table
;;
;; As a special case, @code{#:finish} closes the (internal) connection
;; and arranges for all future invocations of the closure to signal a
;; @samp{dead connection} error.
;;
(define (pgtable-manager db-spec table-name defs)

  (define (fmt . args)
    (apply simple-format #f args))

  (define (symbol->qstring symbol)
    (string-xrep (symbol->string symbol)))

  (or (and (pair? defs) (not (null? defs)))
      (error "malformed defs:" defs))
  (for-each (lambda (def)
              (def:validate-def def type-registered?))
            defs)
  (let* ((conn (cond ((pg-connection? db-spec)
                      db-spec)
                     ((string? db-spec)
                      (pg-connectdb
                       (fmt (if (or (string-null? db-spec)
                                    (string-index db-spec #\=))
                                "~A"
                                "dbname=~A")
                            db-spec)))
                     (else (error "bad db-spec:" db-spec))))
         (trace-exec #f)
         (qstring-colnames (map (lambda (name)
                                  (cons name (symbol->qstring name)))
                                (map def:column-name defs)))
         (objectifiers (def:objectifiers defs))
         (dq-table-name (string-xrep table-name))
         ;; for do-insert
         (ncols (length defs))
         (typenames (map def:type-name defs))
         (insert/pre (delay (sql<-trees #:INSERT #:INTO dq-table-name)))
         ;; for delete-rows
         (delete-rows/pre (delay (sql<-trees #:DELETE #:FROM dq-table-name)))
         ;; for update-col
         (update-col/pre (delay (sql<-trees #:UPDATE dq-table-name #:SET)))
         ;; for select
         (froms (list (string->symbol table-name))))

    (define (col-defs defs cols)
      (map (lambda (col)
             (or (find (lambda (def)
                         (eq? (def:column-name def) col))
                       defs)
                 (error "invalid field name:" col)))
           cols))

    (define (xt . args)                 ; execute tree
      (let ((s (apply sql-command<-trees args)))
        (cond (trace-exec (display s trace-exec)
                          (newline trace-exec)))
        (pg-exec conn s)))

    (define (res->foo-proc proc)
      (lambda (res)
        (proc res (or (ohints res)
                      objectifiers))))

    (define (check-col-count who exp got)
      (or (= exp got)
          (error (fmt "column count mismatch for ~A ~A: expected ~A, got ~A"
                      who dq-table-name exp got))))

    (define (->db-insert-string db-col-type x)
      (or (and (sql-pre? x) x)
          (and (keyword? x) x)
          (let ((s (or (false-if-exception ((type-stringifier db-col-type) x))
                       (type-default db-col-type))))
            (or (string? s) (error "not a string:" s))
            (sql-pre (sql-quote s)))))

    (define (do-insert cols data)
      (xt (force insert/pre)
          (if cols
              (cseptree symbol->qstring cols #t)
              '())
          #:VALUES
          (cseptree ->db-insert-string
                    (if cols
                        (map def:type-name (col-defs defs cols))
                        typenames)
                    #t data)))

    ;; bundle it all up

    (define (drop)
      (map (lambda (x)
             (xt #:DROP x))
           `((#:TABLE ,dq-table-name)
             ;; Also drop associated sequences created by magic ‘serial’
             ;; type.  Apparently, this was not handled automatically in
             ;; old PostgreSQL versions.  See PostgreSQL User Guide: The
             ;; Serial Types.
             ,@(filter-map
                (lambda (def)
                  (and (eq? 'serial (def:type-name def))
                       `(#:SEQUENCE ,(fmt "~A_~A_seq"
                                          table-name
                                          (def:column-name def)))))
                defs))))

    (define (create)
      (xt #:CREATE #:TABLE dq-table-name
          (cseptree (lambda (def)
                      (list (symbol->qstring (def:column-name def))
                            (symbol->string (def:type-name def))
                            (def:type-options def)))
                    defs #t)))

    (define (insert-values . data)
      (check-col-count #:insert-values ncols (length data))
      (do-insert #f data))

    (define (insert-col-values cols . data)
      (check-col-count #:insert-col-values (length cols) (length data))
      (do-insert cols data))

    (define (insert-alist alist)
      (do-insert (map car alist)        ; cols
                 (map cdr alist)))      ; values

    (define (delete-rows where-condition)
      (xt (force delete-rows/pre)
          (make-WHERE-tree where-condition)))

    (define (update-col cols data where-condition)
      (xt (force update-col/pre)
          (cseptree (lambda (def val)
                      (list (assq-ref qstring-colnames (def:column-name def))
                            #:=
                            (->db-insert-string (def:type-name def) val)))
                    (col-defs defs cols)
                    #f data)
          (make-WHERE-tree where-condition)))

    (define (select outspec . rest-clauses)
      (let* ((hint+cols (or (compiled-outspec?-extract outspec)
                            (compiled-outspec?-extract
                             (compile-outspec outspec defs))))
             (res (xt (make-SELECT/FROM/COLS-tree froms (cdr hint+cols))
                      (cond ((null? rest-clauses) '())
                            (else (parse+make-SELECT/tail-tree
                                   rest-clauses))))))
        (set! (ohints res) (car hint+cols))
        res))

    (let ((tuples-result->object-alist (res->foo-proc result->object-alist))
          (tuples-result->alists (res->foo-proc result->object-alists))
          (tuples-result->rows (res->foo-proc result->object-rows)))

      (define (update-col-alist alist where-condition)
        (update-col (map car alist)
                    (map cdr alist)
                    where-condition))

      (define (die!)
        (set! tuples-result->rows #f)
        (set! tuples-result->alists #f)
        (set! tuples-result->object-alist #f)
        (set! trace-exec #f)
        (pg-finish conn)
        (set! conn #f))

      ;; rv
      (lambda (choice)
        (or conn (error "dead connection"))
        (case (if (symbol? choice)
                  (symbol->keyword choice)
                  choice)
          ((#:k) (lambda (var)
                   (case var
                     ((#:connection) conn)
                     ((#:table-name) table-name)
                     ((#:col-defs) defs)
                     (else (error "bad var:" var)))))
          ((#:drop) drop)
          ((#:create) create)
          ((#:insert-values) insert-values)
          ((#:insert-col-values) insert-col-values)
          ((#:insert-alist) insert-alist)
          ((#:delete-rows) delete-rows)
          ((#:update-col) update-col)
          ((#:update-col-alist) update-col-alist)
          ((#:select) select)
          ((#:tuples-result->object-alist) tuples-result->object-alist)
          ((#:tuples-result->alists) tuples-result->alists)
          ((#:tuples-result->rows) tuples-result->rows)
          ((#:trace-exec) (lambda (op)
                            (or (not op)
                                (output-port? op)
                                (error "not an output port:" op))
                            (set! trace-exec op)))
          ((#:finish) die!)
          (else (error "bad choice:" choice)))))))

;; Take @var{db-spec}, @var{table-name} and @var{defs} (exactly the same as
;; for @code{pgtable-manager}) and return a procedure @var{worker} similar to
;; that returned by @code{pgtable-manager} except that the @dfn{data} choices
;; @code{table-name}, @code{defs} and @code{pgdb}
;; result in error (only those choices which return a procedure remain),
;; and more importantly, @var{worker} actually @emph{does} the actions
;; (applying the chosen procedure to its args).  For example:
;;
;; @example
;; (define M (pgtable-manager spec name defs))
;; (define W (pgtable-worker  spec name defs))
;;
;; (equal? ((M #:tuples-result->alists) ((M #:select) #t))
;;         (W #:tuples-result->alists (W #:select #t)))
;; @result{} #t
;; @end example
;;
;; This example is not intended to be wry commentary on the behavioral
;; patterns of human managers and workers, btw.
;;
(define (pgtable-worker db-spec table-name defs)
  (let ((M (pgtable-manager db-spec table-name defs)))
    ;; rv
    (lambda (command . args)
      (let ((proc (M command)))
        (if (procedure? proc)
            (apply proc args)
            (error "command does not yield a procedure:" command))))))

;;; postgres-table.scm ends here
