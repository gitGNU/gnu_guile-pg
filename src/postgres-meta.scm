;;; postgres-meta.scm --- Methods for understanding PostgreSQL data structures

;; Copyright (C) 2002, 2003, 2004, 2005, 2006,
;;   2007, 2008, 2009, 2011 Thien-Thi Nguyen
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

(define-module (database postgres-meta)
  #:export (information-schema-names
            information-schema-coldefs
            defs-from-psql
            check-type/elaborate
            strictly-check-types/elaborate!
            infer-defs
            describe-table!)
  #:use-module ((database postgres)
                #:select (pg-exec
                          pg-print))
  #:use-module ((database postgres-qcons)
                #:select (sql-command<-trees
                          parse+make-SELECT-tree))
  #:use-module ((database postgres-types)
                #:select (dbcoltype-lookup
                          define-db-col-type
                          define-db-col-type-array-variant))
  #:use-module ((database postgres-resx)
                #:select (result-field->object-list))
  #:use-module ((database postgres-table)
                #:select (pgtable-manager
                          compile-outspec))
  #:autoload (srfi srfi-13) (string-trim-both)
  #:autoload (ice-9 popen) (open-input-pipe))

(define (fs s . args)
  (apply simple-format #f s args))

;;;---------------------------------------------------------------------------
;;; Information schema bootstrap
;;;
;;; Underscores are ugly and we would like more than anything for them to be
;;; replaced by hyphens.  Unfortunately, that would require both forward- and
;;; backward-translation in many places outside Guile-PG's control, i.e., in
;;; user code and in PostgreSQL internal code.  Sigh.

(define-db-col-type 'cardinal_number "0"
  number->string
  string->number)

(define-db-col-type 'character_data ""
  identity
  identity)

(define-db-col-type 'sql_identifier ""
  identity
  identity)

';;;UNUSED?
(define-db-col-type 'time_stamp "1970-01-01 00:00:00"
  (lambda (time)
    (cond ((string? time) time)
          ((number? time) (strftime "%Y-%m-%d %H:%M:%S" (localtime time)))
          (else (error "bad timestamp-type input:" time))))
  (lambda (string)
    (car (mktime (car (strptime "%Y-%m-%d %H:%M:%S" string))))))

(define INFSCH-DEFS
  ;; ((name coldef ...) ...)
  ;; where each coldef is either (NAME TYPE), or
  ;; NAME, to be interpreted as (NAME sql_identifier).
  '(("information_schema_catalog_name"
     catalog_name)
    ("applicable_roles"
     grantee
     role_name
     (is_grantable character_data))
    ("check_constraints"
     constraint_catalog
     constraint_schema
     constraint_name
     (check_clause character_data))
    ("column_domain_usage"
     domain_catalog
     domain_schema
     domain_name
     table_catalog
     table_schema
     table_name
     column_name)
    ("column_privileges"
     grantor
     grantee
     table_catalog
     table_schema
     table_name
     column_name
     (privilege_type character_data)
     (is_grantable character_data))
    ("column_udt_usage"
     udt_catalog
     udt_schema
     udt_name
     table_catalog
     table_schema
     table_name
     column_name)
    ("columns"
     table_catalog
     table_schema
     table_name
     column_name
     (ordinal_position cardinal_number)
     (column_default character_data)
     (is_nullable character_data)
     (data_type character_data)
     (character_maximum_length cardinal_number)
     (character_octet_length cardinal_number)
     (numeric_precision cardinal_number)
     (numeric_precision_radix cardinal_number)
     (numeric_scale cardinal_number)
     (datetime_precision cardinal_number)
     (interval_type character_data)
     (interval_precision character_data)
     character_set_catalog
     character_set_schema
     character_set_name
     collation_catalog
     collation_schema
     collation_name
     domain_catalog
     domain_schema
     domain_name
     udt_catalog
     udt_schema
     udt_name
     scope_catalog
     scope_schema
     scope_name
     (maximum_cardinality cardinal_number)
     dtd_identifier
     (is_self_referencing character_data))
    ("constraint_column_usage"
     table_catalog
     table_schema
     table_name
     column_name
     constraint_catalog
     constraint_schema
     constraint_name)
    ("constraint_table_usage"
     table_catalog
     table_schema
     table_name
     constraint_catalog
     constraint_schema
     constraint_name)
    ("data_type_privileges"
     object_catalog
     object_schema
     object_name
     (object_type character_data)
     dtd_identifier)
    ("domain_constraints"
     constraint_catalog
     constraint_schema
     constraint_name
     domain_catalog
     domain_schema
     domain_name
     (is_deferrable character_data)
     (initially_deferred character_data))
    ("domain_udt_usage"
     udt_catalog
     udt_schema
     udt_name
     domain_catalog
     domain_schema
     domain_name)
    ("domains"
     domain_catalog
     domain_schema
     domain_name
     (data_type character_data)
     (character_maximum_length cardinal_number)
     (character_octet_length cardinal_number)
     character_set_catalog
     character_set_schema
     character_set_name
     collation_catalog
     collation_schema
     collation_name
     (numeric_precision cardinal_number)
     (numeric_precision_radix cardinal_number)
     (numeric_scale cardinal_number)
     (datetime_precision cardinal_number)
     (interval_type character_data)
     (interval_precision character_data)
     (domain_default character_data)
     udt_catalog
     udt_schema
     udt_name
     scope_catalog
     scope_schema
     scope_name
     (maximum_cardinality cardinal_number)
     dtd_identifier)
    ("element_types"
     object_catalog
     object_schema
     object_name
     (object_type character_data)
     array_type_identifier
     (data_type character_data)
     (character_maximum_length cardinal_number)
     (character_octet_length cardinal_number)
     character_set_catalog
     character_set_schema
     character_set_name
     collation_catalog
     collation_schema
     collation_name
     (numeric_precision cardinal_number)
     (numeric_precision_radix cardinal_number)
     (numeric_scale cardinal_number)
     (datetime_precision cardinal_number)
     (interval_type character_data)
     (interval_precision character_data)
     (domain_default character_data)
     udt_catalog
     udt_schema
     udt_name
     scope_catalog
     scope_schema
     scope_name
     (maximum_cardinality cardinal_number)
     dtd_identifier)
    ("enabled_roles"
     role_name)
    ("key_column_usage"
     constraint_catalog
     constraint_schema
     constraint_name
     table_catalog
     table_schema
     table_name
     column_name
     (ordinal_position cardinal_number))
    ("parameters"
     specific_catalog
     specific_schema
     specific_name
     (ordinal_position cardinal_number)
     (parameter_mode character_data)
     (is_result character_data)
     (as_locator character_data)
     parameter_name
     (data_type character_data)
     (character_maximum_length cardinal_number)
     (character_octet_length cardinal_number)
     character_set_catalog
     character_set_schema
     character_set_name
     collation_catalog
     collation_schema
     collation_name
     (numeric_precision cardinal_number)
     (numeric_precision_radix cardinal_number)
     (numeric_scale cardinal_number)
     (datetime_precision cardinal_number)
     (interval_type character_data)
     (interval_precision character_data)
     udt_catalog
     udt_schema
     udt_name
     scope_catalog
     scope_schema
     scope_name
     (maximum_cardinality cardinal_number)
     dtd_identifier)
    ("referential_constraints"
     constraint_catalog
     constraint_schema
     constraint_name
     unique_constraint_catalog
     unique_constraint_schema
     unique_constraint_name
     (match_option character_data)
     (update_rule character_data)
     (delete_rule character_data))
    ("role_column_grants"
     grantor
     grantee
     table_catalog
     table_schema
     table_name
     column_name
     (privilege_type character_data)
     (is_grantable character_data))
    ("role_routine_grants"
     grantor
     grantee
     specific_catalog
     specific_schema
     specific_name
     routine_catalog
     routine_schema
     routine_name
     (privilege_type character_data)
     (is_grantable character_data))
    ("role_table_grants"
     grantor
     grantee
     table_catalog
     table_schema
     table_name
     (privilege_type character_data)
     (is_grantable character_data))
    ("role_usage_grants"
     grantor
     grantee
     object_catalog
     object_schema
     object_name
     (object_type character_data)
     (privilege_type character_data)
     (is_grantable character_data))
    ("routine_privileges"
     grantor
     grantee
     specific_catalog
     specific_schema
     specific_name
     routine_catalog
     routine_schema
     routine_name
     (privilege_type character_data)
     (is_grantable character_data))
    ("routines"
     specific_catalog
     specific_schema
     specific_name
     routine_catalog
     routine_schema
     routine_name
     (routine_type character_data)
     module_catalog
     module_schema
     module_name
     udt_catalog
     udt_schema
     udt_name
     (data_type character_data)
     (character_maximum_length cardinal_number)
     (character_octet_length cardinal_number)
     character_set_catalog
     character_set_schema
     character_set_name
     collation_catalog
     collation_schema
     collation_name
     (numeric_precision_radix cardinal_number)
     (numeric_scale cardinal_number)
     (datetime_precision cardinal_number)
     (interval_type character_data)
     (interval_precision character_data)
     type_udt_catalog
     type_udt_schema
     type_udt_name
     scope_catalog
     scope_schema
     scope_name
     (maximum_cardinality cardinal_number)
     dtd_identifier
     (routine_body character_data)
     (routine_definition character_data)
     (external_name character_data)
     (external_language character_data)
     (parameter_style character_data)
     (is_deterministic character_data)
     (sql_data_access character_data)
     (is_null_call character_data)
     (sql_path character_data)
     (schema_level_routine character_data)
     (max_dynamic_result_sets cardinal_number)
     (is_user_defined_cast character_data)
     (is_implicitly_invocable character_data)
     (security_type character_data)
     to_sql_specific_catalog
     to_sql_specific_schema
     to_sql_specific_name
     (as_locator character_data))
    ("schemata"
     catalog_name
     schema_name
     schema_owner
     default_character_set_catalog
     default_character_set_schema
     default_character_set_name
     (sql_path character_data))
    ("sql_features"
     (feature_id character_data)
     (feature_name character_data)
     (sub_feature_id character_data)
     (sub_feature_name character_data)
     (is_supported character_data)
     (is_verified_by character_data)
     (comments character_data))
    ("sql_implementation_info"
     (implementation_info_id character_data)
     (implementation_info_name character_data)
     (integer_value cardinal_number)
     (character_value character_data)
     (comments character_data))
    ("sql_languages"
     (sql_language_source character_data)
     (sql_language_year character_data)
     (sql_language_conformance character_data)
     (sql_language_integrity character_data)
     (sql_language_implementation character_data)
     (sql_language_binding_style character_data)
     (sql_language_programming_language character_data))
    ("sql_packages"
     (feature_id character_data)
     (feature_name character_data)
     (is_supported character_data)
     (is_verified_by character_data)
     (comments character_data))
    ("sql_sizing"
     (sizing_id cardinal_number)
     (sizing_name character_data)
     (supported_value cardinal_number)
     (comments character_data))
    ("sql_sizing_profiles"
     (sizing_id cardinal_number)
     (sizing_name character_data)
     (profile_id character_data)
     (required_value character_data)
     (comments character_data))
    ("table_constraints"
     constraint_catalog
     constraint_schema
     constraint_name
     table_catalog
     table_schema
     table_name
     (constraint_type character_data)
     (is_deferrable character_data)
     (initially_deferred character_data))
    ("table_privileges"
     grantor
     grantee
     table_catalog
     table_schema
     table_name
     (privilege_type character_data)
     (is_grantable character_data)
     (with_hiearchy character_data))
    ("tables"
     table_catalog
     table_schema
     table_name
     (table_type character_data)
     self_referencing_column_name
     (reference_generation character_data)
     user_defined_type_catalog
     user_defined_type_schema
     user_defined_type_name)
    ("triggers"
     trigger_catalog
     trigger_schema
     trigger_name
     (event_manipulation character_data)
     event_object_catalog
     event_object_schema
     event_object_table
     (action_order cardinal_number)
     (action_condition character_data)
     (action_statement character_data)
     (action_orientation character_data)
     (condition_timing character_data)
     condition_reference_old_table
     condition_reference_new_table)
    ("usage_privileges"
     grantor
     grantee
     object_catalog
     object_schema
     object_name
     (object_type character_data)
     (privilege_type character_data)
     (is_grantable character_data))
    ("view_column_usage"
     view_catalog
     view_schema
     view_name
     table_catalog
     table_schema
     table_name
     column_name)
    ("view_table_usage"
     view_catalog
     view_schema
     view_name
     table_catalog
     table_schema
     table_name)
    ("views"
     view_catalog
     view_schema
     view_name
     (view_definition character_data)
     (check_option character_data)
     (is_updatable character_data)
     (is_insertable_into character_data))))

;; Return a list of information schema view names (each a string).
;; Optional arg @var{full?}, non-@code{#f} means to prefix each
;; name with @samp{information_schema.} (note trailing dot).
;;
;;-args: (- 1 0)
;;
(define (information-schema-names . full?)
  (map (if (and (not (null? full?))
                (car full?))
           (lambda (ent)
             (string-append "information_schema." (car ent)))
           car)
       INFSCH-DEFS))

;; Return the column definitions for view @var{name}.
;;
(define (information-schema-coldefs name)
  (and=> (assoc-ref INFSCH-DEFS name)
         (lambda (v)
           (map (lambda (cdef)
                  (if (symbol? cdef)
                      (list cdef 'sql_identifier)
                      cdef))
                v))))

;;;---------------------------------------------------------------------------
;;; Everything else

;; Run @var{psql} and return a @dfn{defs} form for db @var{db-name}, table
;; @var{table-name}.  @var{psql} can be a string specifying the filename of
;; the psql (or psql-workalike) program, a thunk that produces such a string,
;; or @code{#t}, which means use the first "psql" found in the directories
;; named by the @code{PATH} env var.  @code{defs-from-psql} signals "bad psql"
;; error otherwise.
;;
;; In the returned defs, the column names are exact.  The column types and
;; options are only as exact as @var{psql} can produce.  Options are returned
;; as a list, each element of which is either a string (possibly with embedded
;; spaces), or a sub-list of symbols and/or numbers.  Typically the sub-list,
;; if any, will be the first option.  For example, if the column is specified
;; as @code{amount numeric(9,2) not null}, the returned def is the
;; four-element list: @code{(amount numeric (9 2) "not null")}.
;;
(define (defs-from-psql psql db-name table-name)

  (define (psql-command)
    (fs "~A -d ~A -F ' ' -P t -A -c '\\d ~A'"
        (cond ((string? psql) psql)
              ((procedure? psql) (psql))
              ((eq? #t psql) "psql")
              (else (error "bad psql:" psql)))
        db-name table-name))

  (define (read-def p)                  ; NAME TYPE [OPTIONS...]
                                        ; => eof object, or def
    (define (read-type)
      (let ((type (read p)))
        (if (string? type)              ; handle "TYPE" as well as TYPE
            (string->symbol type)
            type)))

    (define (read-options)              ; => eof object, or list

      (define (stb s)
        (string-trim-both s))

      (define (line-remainder)
        (let drain ((c (read-char p)) (acc '()))
          (if (char=? #\newline c)
              (stb (list->string (reverse! acc)))
              (drain (read-char p)
                     (cons (if (char=? #\, c) #\space c) acc)))))

      (define (eol-if-null-string s)
        (and (string-null? s) '()))

      (let ((opts (line-remainder)))
        (cond ((eol-if-null-string opts))
              ((and (char=? #\( (string-ref opts 0))
                    (string-index opts #\)))
               => (lambda (cut)
                    (let ((rest (stb (substring opts (1+ cut)))))
                      (cons (with-input-from-string opts read)
                            (or (eol-if-null-string rest)
                                (list rest))))))
              (else
               (list opts)))))

    (let ((name (read p)))
      (or (and (eof-object? name) name)
          (cons* name (read-type) (read-options)))))

  (let* ((psql-spew (open-input-pipe (psql-command)))
         (next (lambda () (read-def psql-spew))))
    (let loop ((def (next)) (acc '()))
      (if (eof-object? def)
          (begin
            (close-pipe psql-spew)
            (reverse! acc))             ; rv
          (loop (next) (cons def acc))))))

;; Check @var{type}, a symbol.  If it not an array variant, return
;; non-@code{#f} only if its type converters are already registered with
;; Guile-PG.  If @var{type} is an array variant, check the base (non-array)
;; type first, and if needed, ensure that the array variant type is
;; registered.  Return non-@code{#f} if successful.
;;
(define (check-type/elaborate type)
  (let* ((s (symbol->string type))
         (n (string-index s #\[))
         (base (string->symbol (if n (substring s 0 n) s))))
    (and (dbcoltype-lookup base)
         (or (not n)
             (let ((array-variant type))
               (or (dbcoltype-lookup array-variant)
                   (begin
                     (define-db-col-type-array-variant array-variant base)
                     array-variant)))))))

;; For table @var{table-name}, check @var{types} (list of symbols) with
;; @code{check-type/elaborate} and signal error for those types that do not
;; validate, or return non-@code{#f} otherwise.  The @var{table-name} is used
;; only to form the error message.
;;
(define (strictly-check-types/elaborate! table-name types)
  (let ((bad '()))
    (for-each (lambda (type)
                (or (check-type/elaborate type)
                    (set! bad (cons type bad))))
              types)
    (or (null? bad)
        (error (fs "bad ~S types: ~S" table-name bad)))))

(define *class-defs*
  ;; todo: Either mine from "psql \d pg_class", or verify at "make install"
  ;;       time, and invalidate this with external psql fallback.
  (let ((nn "not null"))
    `((relname           name ,nn)
      (relnamespace       oid ,nn)
      (reltype            oid ,nn)
      (relowner       integer ,nn)
      (relam              oid ,nn)
      (relfilenode        oid ,nn)
      (relpages       integer ,nn)
      (reltuples         real ,nn)
      (reltoastrelid      oid ,nn)
      (reltoastidxid      oid ,nn)
      (relhasindex    boolean ,nn)
      (relisshared    boolean ,nn)
      (relkind           char ,nn)
      (relnatts      smallint ,nn)
      (relchecks     smallint ,nn)
      (reltriggers   smallint ,nn)
      (relukeys      smallint ,nn)
      (relfkeys      smallint ,nn)
      (relrefs       smallint ,nn)
      (relhasoids     boolean ,nn)
      (relhaspkey     boolean ,nn)
      (relhasrules    boolean ,nn)
      (relhassubclass boolean ,nn)
      (relacl         aclitem[]))))

(define (make-M:pg-class db-name)
  (pgtable-manager db-name "pg_class" *class-defs*))

(define *table-info-selection*
  (delay (compile-outspec
          (map (lambda (field)
                 (list #t (symbol->string field) (symbol-append 'rel field)))
               '(name
                 kind
                 natts
                 hasindex
                 checks
                 triggers
                 hasrules))
          *class-defs*)))

(define (table-info M:pg-class name)
  ((M:pg-class #:select) (force *table-info-selection*)
   #:where `(= relname ,name)))

(define (table-fields-info conn table-name)
  (pg-exec conn (sql-command<-trees
                 (parse+make-SELECT-tree
                  #t '(a.attname t.typname a.attlen a.atttypmod
                                 a.attnotnull a.atthasdef a.attnum)
                  #:from
                  '((c . pg_class) (a . pg_attribute) (t . pg_type))
                  #:where
                  `(and (= c.relname ,table-name)
                        (> a.attnum 0)
                        (= a.attrelid c.oid)
                        (= a.atttypid t.oid))
                  #:order-by
                  '((< a.attnum))))))

;; Return a @dfn{defs} form suitable for use with @code{pgtable-manager} for
;; connection @var{conn} and @var{table-name}.  The column names are exact.
;; The column types are incorrect for array types, which are described as
;; @code{_FOO}; there is currently no way to infer whether this means
;; @code{FOO[]} or @code{FOO[][]}, etc, without looking at the table's data.
;; No type options are checked at this time.
;;
(define (infer-defs conn table-name)
  (let ((res (table-fields-info conn table-name)))
    (map (lambda args args)
         (result-field->object-list res 0 string->symbol)
         (result-field->object-list res 1 string->symbol))))

;; Display information on database @var{db-name} table @var{table-name}.
;; Include a defs form suitable for use with @code{pgtable-manager};
;; info about the table (kind, natts, hasindex, checks, triggers, hasrules);
;; and info about each field in the table (typname, attlen, atttypmod,
;; attnotnull, atthasdef, attnum).
;;
(define (describe-table! db-name table-name)
  (let* ((M:pg-class (make-M:pg-class db-name))
         (conn ((M:pg-class #:k) #:connection)))
    (for-each (lambda (x) (display x) (newline))
              (infer-defs conn table-name))
    (newline)
    (pg-print (table-info M:pg-class table-name))
    (pg-print (table-fields-info conn table-name))))

;;; postgres-meta.scm ends here
