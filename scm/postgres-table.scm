;;; postgres-table.scm --- abstract manipulation of a single PostgreSQL table

;;    Guile-pg - A Guile interface to PostgreSQL
;;    Copyright (C) 2002, 2003, 2004 Free Software Foundation, Inc.
;;
;;    This program is free software; you can redistribute it and/or modify
;;    it under the terms of the GNU General Public License as published by
;;    the Free Software Foundation; either version 2 of the License, or
;;    (at your option) any later version.
;;
;;    This program is distributed in the hope that it will be useful,
;;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;    GNU General Public License for more details.
;;
;;    You should have received a copy of the GNU General Public License
;;    along with this program; if not, write to the Free Software
;;    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

;;; Author: Thien-Thi Nguyen <ttn@gnu.org>

;;; Commentary:

;; This module exports these procedures:
;;   (sql-pre string)
;;   (tuples-result->table res)
;;   (where-clausifier string)
;;   (pgtable-manager db-spec table-name defs)
;;
;; TODO: Move some of this into a query-construction module.

;;; Code:

(define-module (database postgres-table)
  #:use-module (ice-9 common-list)
  #:use-module (database postgres)
  #:use-module (database postgres-types)
  #:use-module ((database postgres-col-defs)
                #:renamer (symbol-prefix-proc 'def:))
  #:use-module (database postgres-resx)
  #:export (sql-pre
            tuples-result->table
            pgtable-manager
            where-clausifier
            ;; these will go away
            def:col-name
            def:type-name
            def:type-options))

(define def:col-name def:column-name)

;;; support

(define put set-object-property!)
(define get object-property)

(define (string-append/separator c ls)
  (with-output-to-string
    (lambda ()
      (or (null? ls)
          (begin
            (display (car ls))
            (for-each (lambda (x) (display c) (display x))
                      (cdr ls)))))))

(define (string* ls)
  (string-append/separator #\space ls))

(define (sql-quote s)                   ; also surrounds w/ quote
  (let loop ((acc (list #\'))
             (tail (string->list s)))
    (if (not (pair? tail))
        (list->string (reverse (cons #\' acc)))
        (loop (if (char=? #\' (car tail))
                  (append (list #\' #\\) acc)
                  (cons (car tail) acc))
              (cdr tail)))))

(define (char-sep char)
  (lambda (proc ls . more-ls)
    (if (null? ls)
        ""
        (string-append/separator char (apply map proc ls more-ls)))))

(define ssep (char-sep " "))
(define csep (char-sep ","))

(define (symbol->qstring symbol)
  (format #f "~S" (symbol->string symbol)))

(define (fmt . args)
  (apply format #f args))

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

;;; connection

(define (pgdb-connection db)
  (cond ((pg-connection? db) db)
        ((string? db) (pg-connectdb
                       (fmt (if (or (string=? "" db) (string-index db #\=))
                                "~A"
                                "dbname=~A")
                            db)))
        (else (error (fmt "bad db-spec: ~A" db)))))

(define (validate-def def)
  (or (get def 'validated)
      (and (pair? def)
           (let ((col-name (def:column-name def)))
             (and (symbol? col-name)
                  (every (lambda (c)
                           (or (char-alphabetic? c)
                               (char-numeric? c)
                               (char=? c #\_)))
                         (string->list (symbol->string col-name)))))
           (dbcoltype-lookup (def:type-name def))
           (put def 'validated #t)) ; cache
      (error "malformed def:" def)))

(define (validate-defs defs)
  (or (get defs 'validated)
      (and (list? defs)
           (not (null? defs))
           (for-each validate-def defs)
           (put defs 'validated #t)) ; cache
      (error "malformed defs:" defs)))

(define (col-defs defs cols)
  (map (lambda (col)
         (or (find-if (lambda (def)
                        (eq? (def:column-name def) col))
                      defs)
             (error "invalid field name:" col)))
       cols))

;;; drops

(define (drop-table-cmd table-name)
  (fmt "DROP TABLE ~A;" table-name))

(define (drop-sequence-cmd sequence-name)
  (fmt "DROP SEQUENCE ~A;" sequence-name))

(define (drop-proc pgdb table-name defs)
  (lambda ()
    (cons (pg-exec pgdb (drop-table-cmd table-name))
          ;; Also drop associated sequences created by magic `serial' type.
          ;; The naming algorithm is detailed in the PostgreSQL User Guide,
          ;; Chapter 3: Data Types.
          (map (lambda (col)
                 (pg-exec pgdb (drop-sequence-cmd
                                (string-append table-name "_"
                                               (symbol->string col)
                                               "_seq"))))
               (pick-mappings (lambda (def)
                                (and (eq? 'serial (def:type-name def))
                                     (def:column-name def)))
                              defs)))))

;;; create table

(define (create-cmd table-name defs)
  (validate-defs defs)
  (let ((dspec (csep (lambda (def)
                       (let ((name (symbol->qstring (def:column-name def)))
                             (type (string-upcase
                                    (symbol->string
                                     (def:type-name def))))
                             (opts (let ((opts (def:type-options def)))
                                     (if (null? opts)
                                         ""
                                         (ssep identity (cons "" opts))))))
                         (fmt "~A ~A~A" name type opts)))
                     defs)))
    (fmt "CREATE TABLE ~A (~A);" table-name dspec)))

(define (create-proc pgdb table-name defs)
  (validate-defs defs)
  (lambda ()
    (pg-exec pgdb (create-cmd table-name defs))))

;;; inserts

(define (->db-insert-string db-col-type x)
  (or (and (get x 'ttn-pgtable-sql-pre) x)
      (let ((def (dbcoltype-lookup db-col-type)))
        (sql-quote (or (false-if-exception ((dbcoltype:stringifier def) x))
                       (dbcoltype:default def))))))

(define (serial? def)
  (eq? 'serial (def:type-name def)))

(define (clean-defs defs)
  (validate-defs defs)
  (remove-if serial? defs))

(define (cdefs->cols cdefs)
  (csep (lambda (def)
          (symbol->qstring (def:column-name def)))
        cdefs))

(define (insert-values-cmd table-name defs data)
  (let ((cdefs (clean-defs defs)))
    (fmt "INSERT INTO ~A (~A) VALUES (~A);" table-name
         (cdefs->cols cdefs)            ;;; cols
         (csep ->db-insert-string       ;;; values
               (map def:type-name cdefs)
               data))))

(define (insert-values-proc pgdb table-name defs)
  (validate-defs defs)
  (lambda data
    (pg-exec pgdb (insert-values-cmd table-name defs data))))

(define (insert-col-values-cmd table-name defs cols data)
  (let ((cdefs (clean-defs (col-defs defs cols))))
    (fmt "INSERT INTO ~A (~A) VALUES (~A);" table-name
         (csep symbol->qstring cols)    ;;; cols
         (csep ->db-insert-string       ;;; values
               (map def:type-name cdefs)
               data))))

(define (insert-col-values-proc pgdb table-name defs)
  (validate-defs defs)
  (lambda (cols . data)
    (pg-exec pgdb (insert-col-values-cmd table-name defs cols data))))

(define (insert-alist-cmd table-name defs alist)
  (insert-col-values-cmd table-name defs
                         (map car alist)        ;;; cols
                         (map cdr alist)))      ;;; values

(define (insert-alist-proc pgdb table-name defs)
  (validate-defs defs)
  (lambda (alist)
    (pg-exec pgdb (insert-alist-cmd table-name defs alist))))

;;; delete

(define (delete-rows-cmd table-name where-condition)
  (fmt "DELETE FROM ~A WHERE ~A;" table-name where-condition))

(define (delete-rows-proc pgdb table-name)
  (lambda (where-condition)
    (pg-exec pgdb (delete-rows-cmd table-name where-condition))))

;;; update

(define (update-col-cmd table-name defs cols data where-condition)
  (let* ((cdefs (col-defs defs cols))
         (stuff (csep (lambda (def val)
                        (let* ((col (symbol->qstring (def:column-name def)))
                               (type (def:type-name def))
                               (instr (->db-insert-string type val)))
                          (fmt "~A=~A" col instr)))
                      cdefs
                      data)))
    (fmt "UPDATE ~A SET ~A WHERE ~A;" table-name stuff where-condition)))

(define (update-col-proc pgdb table-name defs)
  (lambda (cols data where-condition)
    (pg-exec pgdb (update-col-cmd
                   table-name defs cols data where-condition))))

;;; select

(define (select-cmd table-name defs selection rest-clauses)
  (validate-defs defs)
  (let* ((bad-selection (lambda (s) (error "bad selection:" s)))
         (sel (cond ((string? selection) selection)
                    ((list? selection)
                     (csep (lambda (x)
                             (or (and (symbol? x)
                                      (assq x defs)
                                      (symbol->qstring x))
                                 (bad-selection x)))
                           selection))
                    ((symbol? selection)
                     (or (and (assq selection defs)
                              (symbol->qstring selection))
                         (bad-selection selection)))
                    (else
                     (bad-selection selection))))
         (etc (string* rest-clauses)))
    (fmt "SELECT ~A FROM ~A ~A;" sel table-name etc)))

(define (select-proc pgdb table-name defs)
  (validate-defs defs)
  (lambda (selection . rest-clauses)
    (pg-exec pgdb (select-cmd table-name defs selection rest-clauses))))

;;; results processing

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
  (let* ((ntuples (pg-ntuples res)) (tn-range (iota ntuples))
         (nfields (pg-nfields res)) (fn-range (iota nfields))
         (table (make-array #f (max 1 ntuples) (max 1 nfields)))
         (names (map (lambda (fn)
                       (pg-fname res fn))
                     fn-range))
         (widths (list->vector (map string-length names))))
    (for-each (lambda (tn)
                (for-each (lambda (fn)
                            (let* ((val (pg-getvalue res tn fn))
                                   (len (string-length val)))
                              (and (< (vector-ref widths fn) len)
                                   (vector-set! widths fn len))
                              (array-set! table val tn fn)))
                          fn-range))
              tn-range)
    ;; save some info
    (put table 'names (list->vector names))
    (put table 'widths widths)
    table))

(define (t-obj-walk-proc defs)
  (lambda (table proc-o proc-non-o)
    (let ((tn-range (iota (car  (array-dimensions table))))
          (fn-range (iota (cadr (array-dimensions table))))
          (names (get table 'names)))
      (for-each (lambda (fn)
                  (let* ((type (cond ((assq-ref defs (string->symbol
                                                      (vector-ref names fn)))
                                      => (lambda (x)
                                           (if (pair? x)
                                               (car x)
                                               x)))))
                         (lookup (dbcoltype-lookup type)))
                    (if lookup
                        (and proc-o
                             (let ((o (lambda (s)
                                        ((dbcoltype:objectifier lookup)
                                         (or (and (< 0 (string-length s))
                                                  (char=? #\' (string-ref s 0))
                                                  (sql-unquote s))
                                             s)))))
                               (for-each (lambda (tn)
                                           (let ((s (array-ref table tn fn)))
                                             (proc-o table tn fn s (o s))))
                                         tn-range)))
                        (and proc-non-o
                             (for-each (lambda (tn)
                                         (let ((s (array-ref table tn fn)))
                                           (proc-non-o table tn fn s)))
                                       tn-range)))))
                fn-range))))

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
;; The closure accepts a single arg CHOICE (a symbol) and returns either the
;; variable or procedure associated with CHOICE.  When CHOICE is `help' or
;; `menu', return a list of accepted choices, currently one of:
;;
;; @example
;;   table-name
;;   defs
;;   pgdb
;; * (drop)
;; * (create)
;; * (insert-values . DATA)
;; * (insert-col-values COLS . DATA)
;; * (insert-alist ALIST)
;; * (delete-rows WHERE-CONDITION)
;; * (update-col COLS DATA WHERE-CONDITION)
;; * (select SELECTION . REST-CLAUSES)
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
;; string.  SELECTION is either a list of column names, or a string that is
;; passed directly to the backend in an SQL "select" clause.  REST-CLAUSES are
;; zero or more strings.  TABLE and RES are the same types as for proc
;; `tuples-result->table', q.v.
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
  (validate-defs defs)
  (let* ((pgdb (pgdb-connection db-spec))
         (drop (drop-proc pgdb table-name defs))
         (create (create-proc pgdb table-name defs))
         (insert-values (insert-values-proc pgdb table-name defs))
         (insert-col-values (insert-col-values-proc pgdb table-name defs))
         (insert-alist (insert-alist-proc pgdb table-name defs))
         (delete-rows (delete-rows-proc pgdb table-name))
         (update-col (update-col-proc pgdb table-name defs))
         (select (select-proc pgdb table-name defs))
         (t-obj-walk (t-obj-walk-proc defs))
         (table->object-alist (table->object-alist-proc t-obj-walk))
         (objectifiers (def:objectifiers defs))
         (tuples-result->object-alist (lambda (res)
                                        (result->object-alist
                                         res objectifiers)))
         (table->alists (lambda (table)
                          (object-alist->alists
                           (table->object-alist table))))
         (tuples-result->alists (lambda (res)
                                  (result->object-alists
                                   res objectifiers))))
    (lambda (choice)
      (case choice
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
        ((pgdb) pgdb)
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
        (else (error "bad choice"))))))

;;; "where" clausifier

;; Return an SQL "where clause" from STRING.
;;
(define (where-clausifier string)
  (fmt "WHERE ~A" string))

;;; postgres-table.scm ends here
