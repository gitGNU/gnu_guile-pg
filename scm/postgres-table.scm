;;; postgres-table.scm --- abstract manipulation of a single PostgreSQL table

;;	Copyright (C) 2002, 2003, 2004, 2005 Thien-Thi Nguyen
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

;; This module exports these procedures:
;;   (sql-pre string)
;;   (tuples-result->table res)
;;   (where-clausifier string)
;;   (pgtable-manager db-spec table-name defs)
;;   (pgtable-worker db-spec table-name defs)
;;   (compile-outspec spec defs)
;;
;; TODO: Move some of this into a query-construction module.

;;; Code:

(define-module (database postgres-table)
  #:use-module ((ice-9 common-list)
                #:select (find-if
                          remove-if))
  #:use-module ((database postgres)
                #:select (pg-connection?
                          pg-connectdb
                          pg-exec pg-ntuples pg-nfields
                          pg-fname pg-getvalue))
  #:use-module ((database postgres-types)
                #:select (dbcoltype-lookup
                          dbcoltype:stringifier
                          dbcoltype:default
                          dbcoltype:objectifier))
  #:use-module ((database postgres-col-defs)
                #:select (column-name
                          type-name
                          type-options
                          validate-def
                          objectifiers)
                #:renamer (symbol-prefix-proc 'def:))
  #:use-module ((database postgres-qcons)
                #:select (sql-quote
                          make-SELECT/FROM/OUT-tree
                          sql-command<-trees))
  #:use-module ((database postgres-resx)
                #:select (result->object-alist
                          result->object-alists))
  #:export (sql-pre
            tuples-result->table
            pgtable-manager
            pgtable-worker
            compile-outspec
            where-clausifier))

;;; support

(define put set-object-property!)
(define get object-property)

(define (string-append/separator c ls)
  (let* ((len-ls (map string-length ls))
         (rv (make-string (max 0 (apply + (length len-ls) -1 len-ls)))))
    (let loop ((put -1) (ls ls) (len-ls len-ls))
      (if (null? ls)
          rv
          (let ((s (car ls))
                (len (car len-ls)))
            (or (= -1 put)
                (string-set! rv put c))
            (substring-move! s 0 len rv (1+ put))
            (loop (+ put 1 len) (cdr ls) (cdr len-ls)))))))

(define (string* ls)
  (string-append/separator #\space ls))

(define (char-sep char)
  (lambda (proc ls . more-ls)
    (if (null? ls)
        ""
        (string-append/separator char (apply map proc ls more-ls)))))

(define ssep (char-sep #\space))
(define csep (char-sep #\,))

(define (fmt . args)
  (apply simple-format #f args))

(define (symbol->qstring symbol)
  (fmt "~S" (symbol->string symbol)))

;; Return STRING w/ its `ttn-pgtable-sql-pre' property set.
;; This property disables type conversion and other formatting
;; when the string is used for "insert" and "update" operations.
;; (The "pre" means "preformatted".)
;;
;; For example, assume column `count' has type `int4'.  The following
;; fragment increments this value for all rows:
;; @lisp
;; (let ((m (pgtable-manager ...)))
;;   ((m 'update-col) '(count) `(,(sql-pre "count + 1"))))
;; @end lisp
;;
(define (sql-pre string)
  (put string 'ttn-pgtable-sql-pre #t)
  string)

(define (serial? def)
  (eq? 'serial (def:type-name def)))

;;; connection

(define (col-defs defs cols)
  (map (lambda (col)
         (or (find-if (lambda (def)
                        (eq? (def:column-name def) col))
                      defs)
             (error "invalid field name:" col)))
       cols))

;;; drops

(define (drop-proc beex table-name defs)
  (define cmds
    (delay (let loop ((ls defs)
                      (acc (list (fmt "DROP TABLE ~A;" table-name))))
             (if (null? ls)
                 (reverse! acc)
                 ;; Also drop associated sequences created by magic
                 ;; `serial' type.  Apparently, this was not handled
                 ;; automatically in old PostgreSQL versions.  See
                 ;; PostgreSQL User Guide, Chapter 3: Data Types.
                 (loop (cdr ls)
                       (if (serial? (car ls))
                           (cons (fmt "DROP SEQUENCE ~A_~A_seq;"
                                      table-name
                                      (def:column-name (car ls)))
                                 acc)
                           acc))))))
  ;; rv
  (lambda ()
    (map beex (force cmds))))

;;; create table

(define (create-proc beex table-name defs)
  (define (fmt-col def)
    (let ((name (symbol->qstring (def:column-name def)))
          (type (string-upcase
                 (symbol->string
                  (def:type-name def))))
          (opts (let ((opts (def:type-options def)))
                  (if (null? opts)
                      ""
                      (ssep identity (cons "" opts))))))
      (fmt "~A ~A~A" name type opts)))
  (define cmd
    (delay (fmt "CREATE TABLE ~A (~A);" table-name (csep fmt-col defs))))
  ;; rv
  (lambda ()
    (beex (force cmd))))

;;; inserts

(define (->db-insert-string db-col-type x)
  (or (and (get x 'ttn-pgtable-sql-pre) x)
      (let ((def (dbcoltype-lookup db-col-type)))
        (sql-quote (or (false-if-exception ((dbcoltype:stringifier def) x))
                       (dbcoltype:default def))))))

(define (clean-defs defs)
  (remove-if serial? defs))

(define (pre-insert-into table-name)
  (fmt "INSERT INTO ~A" table-name))

(define (insert-values-proc beex table-name defs)
  (define cdefs (delay (clean-defs defs)))
  (define pre (delay (fmt "~A (~A) VALUES"
                          (pre-insert-into table-name)
                          (csep (lambda (def)
                                  (symbol->qstring (def:column-name def)))
                                (force cdefs)))))
  (define types (delay (map def:type-name (force cdefs))))
  (define (make-cmd data)
    (fmt "~A (~A);" (force pre)
         (csep ->db-insert-string (force types) data)))
  ;; rv
  (lambda data
    (beex (make-cmd data))))

(define (insert-col-values-cmd pre defs cols data)
  (let ((cdefs (clean-defs (col-defs defs cols))))
    (fmt "~A (~A) VALUES (~A);" pre
         (csep symbol->qstring cols)    ;;; cols
         (csep ->db-insert-string       ;;; values
               (map def:type-name cdefs)
               data))))

(define (insert-col-values-proc beex table-name defs)
  (define pre (delay (pre-insert-into table-name)))
  (define (make-cmd cols data)
    (insert-col-values-cmd (force pre) defs cols data))
  (lambda (cols . data)
    (beex (make-cmd cols data))))

(define (insert-alist-proc beex table-name defs)
  (define pre (delay (pre-insert-into table-name)))
  (define (make-cmd alist)
    (insert-col-values-cmd (force pre) defs
                           (map car alist)        ;;; cols
                           (map cdr alist)))      ;;; values
  (lambda (alist)
    (beex (make-cmd alist))))

;;; delete

(define (delete-rows-proc beex table-name ignored-defs)
  (define pre (delay (fmt "DELETE FROM ~A WHERE" table-name)))
  (define (make-cmd where-condition)
    (fmt "~A ~A;" (force pre) where-condition))
  (lambda (where-condition)
    (beex (make-cmd where-condition))))

;;; update

(define (update-col-proc beex table-name defs)
  (define pre (delay (fmt "UPDATE ~A SET" table-name)))
  (define (make-cmd cols data where-condition)
    (let* ((cdefs (col-defs defs cols))
           (stuff (csep (lambda (def val)
                          (let* ((col (symbol->qstring (def:column-name def)))
                                 (type (def:type-name def))
                                 (instr (->db-insert-string type val)))
                            (fmt "~A=~A" col instr)))
                        cdefs
                        data)))
      (fmt "~A ~A WHERE ~A;" (force pre) stuff where-condition)))
  (lambda (cols data where-condition)
    (beex (make-cmd cols data where-condition))))

;;; select

;; Return a @dfn{compiled outspec object} from @var{spec} and @var{defs},
;; suitable for passing to the @code{select} choice of @code{pgtable-manager}.
;; @var{defs} is the same as that for @code{pgtable-manager}.  @var{spec} can
;; be one of the following:
;;
;; @itemize
;; @item a column name (a symbol)
;;
;; @item #t, which means all columns (notionally equivalent to "*")
;;
;; @item a list of column specifications each of which is either a column
;; name, or has the form @code{(TYPE TITLE EXPR)}, where:
;;
;; @itemize
;; @item @var{expr} is a prefix-style expression to compute for the column
;; (@pxref{Query Construction}); or a string (note, however, that support
;; for string @var{expr} WILL BE REMOVED after 2005-12-31; DO NOT rely on it)
;;
;; @item @var{title} is the title (string) of the column, or #f
;;
;; @item @var{type} is a column type (symbol) such as @code{int4},
;; or #f to mean @code{text}, or #t to mean use the type associated
;; with the column named in @var{expr}, or the pair @code{(#t . name)}
;; to mean use the type associated with column @var{name}
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
      (set! objectifiers
            (cons (dbcoltype:objectifier (or (dbcoltype-lookup type)
                                             (bad-select-part type)))
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
                  ;; todo: enable after 2005-12-31
                  ;;+ (and (string? expr) (bad-select-part expr))
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

    (let ((s (map munge (cond ((eq? #t spec) (map def:column-name defs))
                              ((pair? spec) spec)
                              (else (list spec))))))
      ;; rv, a "compiled outspec"
      (cons compile-outspec
            (cons (reverse! objectifiers) s)))))

(define (compiled-outspec?-extract obj)
  (and (pair? obj)
       (eq? compile-outspec (car obj))
       (cdr obj)))

(define (select-proc beex table-name defs)
  (let ((froms (list (string->symbol table-name))))
    (lambda (outspec . rest-clauses)
      (let ((hints #f))
        (define (set-hints!+sel pair)
          (set! hints (car pair))
          (cdr pair))
        (let* ((cmd (sql-command<-trees
                     (make-SELECT/FROM/OUT-tree
                      froms
                      (cond ((compiled-outspec?-extract outspec)
                             => set-hints!+sel)
                            ;; todo: zonk after 2005-12-31
                            ((string? outspec)
                             (list outspec))
                            (else
                             (set-hints!+sel
                              (cdr (compile-outspec outspec defs))))))
                     (string* rest-clauses)))
               (res (beex cmd)))
          (and hints (put res #:pgtable-ohints hints))
          res)))))

;;; results processing

(define (make-v len val-at)
  (let ((v (make-vector len)))
    (array-index-map! v val-at)
    v))

;; Extract data from the tuples result RES, and return an annotated array.
;; The array's values correspond to the data from RES, and has dimensions
;; `pg-ntuples' by `pg-nfields'.  Annotations are object properties:
;; @example
;;     names   -- vector of field names
;;     widths  -- vector of maximum field widths
;; @end example
;; When either number of tuples or number of fields is zero, they are taken
;; as one, instead, which ensures that the returned array has consistent
;; rank.  (This might not be such a hot idea, long-run; still evaluating.)
;;
(define (tuples-result->table res)
  (let* ((ntuples (pg-ntuples res))
         (nfields (pg-nfields res))
         (table (make-array #f (max 1 ntuples) (max 1 nfields)))
         (names (make-v nfields (lambda (fn)
                                  (pg-fname res fn))))
         (widths (make-v nfields (lambda (fn)
                                   (string-length (vector-ref names fn))))))
    (do ((tn 0 (1+ tn))) ((= ntuples tn))
      (do ((fn 0 (1+ fn))) ((= nfields fn))
        (let* ((val (pg-getvalue res tn fn))
               (len (string-length val)))
          (and (< (vector-ref widths fn) len)
               (vector-set! widths fn len))
          (array-set! table val tn fn))))
    ;; save some info
    (put table 'names names)
    (put table 'widths widths)
    table))

(define (t-obj-walk-proc defs)
  (lambda (table proc-o proc-non-o)
    (let* ((dim (array-dimensions table))
           (ntuples (car  dim))
           (nfields (cadr dim))
           (names (get table 'names)))
      (do ((fn 0 (1+ fn)))
          ((= nfields fn))
        (let* ((type (and=> (assq-ref defs (string->symbol
                                            (vector-ref names fn)))
                            (lambda (x) (if (pair? x)
                                            (car x)
                                            x))))
               (lookup (dbcoltype-lookup type)))
          (if lookup
              (and proc-o
                   (let ((o (lambda (s)
                              ((dbcoltype:objectifier lookup)
                               (or (and (< 0 (string-length s))
                                        (char=? #\' (string-ref s 0))
                                        (sql-unquote s))
                                   s)))))
                     (do ((tn 0 (1+ tn)))
                         ((= ntuples tn))
                       (let ((s (array-ref table tn fn)))
                         (proc-o table tn fn s (o s))))))
              (and proc-non-o
                   (do ((tn 0 (1+ tn)))
                       ((= ntuples tn))
                     (let ((s (array-ref table tn fn)))
                       (proc-non-o table tn fn s))))))))))

(define (table->object-alist-proc t-obj-walk)
  (lambda (table)
    (let* ((ret (map list (map string->symbol
                               (vector->list
                                (get table 'names)))))
           (lsfn 0)                     ; last-seen field number
           (place ret)                  ; advances every time lsfn changes
           (stash (lambda (fn x)
                    (or (= fn lsfn)
                        (begin
                          (set! place (cdr place))
                          (set! lsfn fn)))
                    (let ((inside (car place)))
                      (set-cdr! inside (cons x (cdr inside)))))))
      (t-obj-walk table
                  (lambda (table tn fn str obj) (stash fn obj))
                  (lambda (table tn fn str)     (stash fn str)))
      (do ((ls ret (cdr ls)))
          ((null? ls) ret)
        (set-cdr! (car ls) (reverse! (cdar ls)))))))

(define (object-alist->alists object-alist)
  (let ((names (map car object-alist)))
    (apply map (lambda slice
                 (map cons names slice))
           (map cdr object-alist))))

;;; dispatch

;; Return a closure that manages a table specified by DB-SPEC TABLE-NAME DEFS.
;;
;; DB-SPEC can either be a string simply naming the database to use, a string
;; comprised of space-separated @code{var=val} pairs, an empty string, or an
;; already existing connection.  @ref{Database Connections}.  TABLE-NAME is a
;; string naming the table to be managed.
;;
;; DEFS is an alist of column definitions of the form (NAME TYPE [OPTION
;; ...]), with NAME and TYPE symbols and each OPTION a string.  The old format
;; w/o options: `(NAME . TYPE)' is recognized also, but deprecated; support
;; for it will go away in a future release.
;;
;; The closure accepts a single arg CHOICE (a symbol or keyword) and returns
;; either the variable or procedure associated with CHOICE.  When CHOICE is
;; `help' or `menu', return a list of accepted choices, currently one of:
;;
;; @example
;;   table-name
;;   defs
;;   pgdb
;; * (drop)
;; * (create)
;; * (insert-values [DATA ...])
;; * (insert-col-values COLS [DATA ...])
;; * (insert-alist ALIST)
;; * (delete-rows WHERE-CONDITION)
;; * (update-col COLS DATA WHERE-CONDITION)
;; * (select OUTSPEC [REST-CLAUSES ...])
;;   (t-obj-walk TABLE PROC-O PROC-NON-O)
;;   (table->object-alist TABLE)
;;   (tuples-result->object-alist RES)
;;   (table->alists TABLE)
;;   (tuples-result->alists RES)
;; @end example
;;
;; In this list, procedures are indicated by signature (parens).  DATA is one
;; or more Scheme objects.  COLS is either a list of column names (symbols),
;; or a single string of comma-delimited column names.  WHERE-CONDITION is a
;; string.  OUTSPEC is either the result of `compile-outspec', or a spec
;; that `compile-outspec' can process to produce such a result.  NOTE: Some
;; older versions of `pgtable-manager' also accept a string for OUTSPEC.
;; DO NOT rely on this; string support WILL BE REMOVED after 2005-12-31.
;; REST-CLAUSES are zero or more strings.  TABLE and RES are the same
;; types as for proc `tuples-result->table', q.v.
;;
;; PROC-O is #f, or a procedure that is called by the table walker like so:
;;  (PROC-O TABLE TN FN STRING OBJ)
;;
;; Similarly, PROC-NON-O is #f, or a procedure with signature:
;;  (PROC-NON-O TABLE TN FN STRING)
;;
;; For both these procedures, TABLE is as described above, TN and FN are the
;; tuple and field numbers (zero-origin), respectively, STRING is the string
;; representation of the data and OBJ is the Scheme object converted from
;; STRING.
;;
;; The starred (*) procedures return whatever `pg-exec' returns for that type
;; of procedure.  See guile-pg info page for details.
;;
(define (pgtable-manager db-spec table-name defs)
  (or (and (pair? defs) (not (null? defs)))
      (error "malformed defs:" defs))
  (for-each (lambda (def)
              (def:validate-def def dbcoltype-lookup))
            defs)
  (let* ((conn (cond ((pg-connection? db-spec)
                      db-spec)
                     ((string? db-spec)
                      (pg-connectdb
                       (fmt (if (or (string=? "" db-spec)
                                    (string-index db-spec #\=))
                                "~A"
                                "dbname=~A")
                            db-spec)))
                     (else (error "bad db-spec:" db-spec))))
         (objectifiers (def:objectifiers defs))
         (pp (lambda (proc-proc)
               (proc-proc (lambda (s)   ; beex (back-end exec)
                            (pg-exec conn s))
                          table-name
                          defs)))
         (res->foo-proc (lambda (proc)
                          (lambda (res)
                            (proc res (or (get res #:pgtable-ohints)
                                          objectifiers)))))
         (t-obj-walk (t-obj-walk-proc defs)))

    (define drop (pp drop-proc))

    (define create (pp create-proc))

    (define insert-values (pp insert-values-proc))

    (define insert-col-values (pp insert-col-values-proc))

    (define insert-alist (pp insert-alist-proc))

    (define delete-rows (pp delete-rows-proc))

    (define update-col (pp update-col-proc))

    (define select (pp select-proc))

    (define table->object-alist (table->object-alist-proc t-obj-walk))

    (define tuples-result->object-alist (res->foo-proc result->object-alist))

    (define (table->alists table)
      (object-alist->alists (table->object-alist table)))

    (define tuples-result->alists (res->foo-proc result->object-alists))

    ;; rv
    (lambda (choice)
      (case (if (keyword? choice)
                (keyword->symbol choice)
                choice)
        ((help menu) '(help menu
                            table-name defs
                            pgdb
                            drop create insert-values insert-col-values
                            delete-rows update-col select
                            t-obj-walk
                            table->object-alist
                            tuples-result->object-alist
                            table->alists
                            tuples-result->alists))
        ((table-name) table-name)
        ((defs) defs)
        ((pgdb) conn)
        ((drop) drop)
        ((create) create)
        ((insert-values) insert-values)
        ((insert-col-values) insert-col-values)
        ((insert-alist) insert-alist)
        ((delete-rows) delete-rows)
        ((update-col) update-col)
        ((select) select)
        ((t-obj-walk) t-obj-walk)
        ((table->object-alist) table->object-alist)
        ((tuples-result->object-alist) tuples-result->object-alist)
        ((table->alists) table->alists)
        ((tuples-result->alists) tuples-result->alists)
        (else (error "bad choice:" choice))))))

;; Take @var{db-spec}, @var{table-name} and @var{defs} (exactly the same as
;; for @code{pgtable-manager}) and return a procedure @var{worker} similar to
;; that returned by @code{pgtable-manager} except that the @dfn{data} choices
;; @code{help}, @code{menu}, @code{table-name}, @code{defs} and @code{pgdb}
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

;;; "where" clausifier

;; Return an SQL "where clause" from STRING.
;;
(define (where-clausifier string)
  (fmt "WHERE ~A" string))

;;; postgres-table.scm ends here
