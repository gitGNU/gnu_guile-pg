;;; Copyright (C) 2003 Thien-Thi Nguyen
;;; This program is provided under the terms of the GNU GPL, version 2.
;;; See http://www.fsf.org/copyleft/gpl.html for details.

(define-module (markup)
  :use-module (database postgres)
  :use-module (database postgres-types)
  :use-module (database postgres-table)
  :use-module (ttn display-table))

;; type extension

(or (dbcoltype-lookup 'oid[])
    (define-db-col-type-array-variant 'oid[] 'oid))

;; database

(define *db* (getenv "USER"))

;; table defs

(define *markup-defs*
  '((raw    text)
    (markup text[])))

(define *client-defs*
  '((i           serial)
    (description oid[])))

;; display-tree

(define (display-tree tree)
  (if (list? tree)
      (for-each display-tree tree)
      (display tree)))

;; play!

(let ((m (pgtable-manager *db* "markup_play" *markup-defs*))
      (c (pgtable-manager *db* "client_play" *client-defs*)))

  ;; Add POSSIBLY-MARKED-TEXT to markup table, and return its OID.
  ;; POSSIBLY-MARKED-TEXT can either be a simple string, in which
  ;; case no markup is implied, or a list taking one of the forms:
  ;;
  ;;   (url URL)
  ;;   (url URL TEXT)
  ;;   (email TEXT ADDR)
  ;;
  ;; in which case, the markup is extracted as '("url" URL) and '("email"
  ;; ADDR), respectively, and the TEXT is passed through directly.  In the
  ;; url case, if TEXT is missing, use URL instead.
  ;;
  (define (add-possibly-marked-text possibly-marked-text)
    (pg-oid-value
     (let ((insert (m 'insert-values)))
       (if (string? possibly-marked-text)
           (insert possibly-marked-text '())
           (let* ((form possibly-marked-text)
                  (type (car form)))
             (insert (case type
                       ((url) ((if (= 3 (length form)) caddr cadr) form))
                       ((email) (cadr form))
                       (else (error (format #f "bad form: ~A" form))))
                     (list (symbol->string (car form))
                           ((case type
                              ((url) cadr)
                              ((email) caddr))
                            form))))))))

  (define (add-description ls)
    ((c 'insert-col-values) '(description)
     (map add-possibly-marked-text ls)))

  (define (>>table heading manager)
    (write-line heading)
    (display-table (tuples-result->table ((manager 'select) "*"))))

  (define (tree<-possibly-marked-text oid)
    (let* ((alist (car                   ; only one
                   ((m 'tuples-result->alists)
                    ((m 'select) "*" (where-clausifier
                                      (format #f "oid = ~A" oid))))))
           (raw (assq-ref alist 'raw))
           (markup (assq-ref alist 'markup)))
      (if (null? markup)
          raw
          (case (string->symbol (car markup))
            ((url) (list "<A HREF=\"" (cadr markup) "\">" raw "</A>"))
            ((email) (list "<A HREF=\"mailto:" (cadr markup) "\">" raw "</A>"))
            (else (error (format #f "bad markup: ~A" markup)))))))

  (define (>>description i)
    (format #t "description for: ~A\n" i)
    (for-each (lambda (oid)
                (display-tree (tree<-possibly-marked-text oid)))
              (assq-ref
               (car                     ; only one
                ((c 'tuples-result->alists)
                 ((c 'select) '(description)
                  (where-clausifier (format #f "i = ~A" i)))))
               'description))
    (newline))

  (define *samples*
    (list
     '("This is the guile scheme code that maintains the "
       (url "http://www.glug.org/projects/list.html"
            "guile projects list")
       ".  There are configurations for glug.org as well for "
       (url "http://www.gnu.org/software/guile/gnu-guile-projects.html"
            "the gnu.org subset of the list") ".")
     '("An interface to PostgreSQL from guile.")
     '((url "http://www-ccrma.stanford.edu/software/snd/")
       " is where you can find Snd.")
     '("The hobbit author is "
       (email "Tanel Tammet" "tammet@cs.chalmers.se") ".")))

  (write-line ((m 'create)))
  (write-line ((c 'create)))

  (for-each (lambda (sample)
              (write-line (add-description sample)))
            *samples*)

  (>>table "markup" m)
  (>>table "client" c)

  (let ((read-i (lambda ()
                  (format #t "i: ")
                  (flush-all-ports)
                  (read))))
    (let loop ((i (read-i)))
      (or (= 0 i)
          (begin
            (>>description i)
            (loop (read-i))))))

  (write-line ((c 'drop)))
  (write-line ((m 'drop))))

;;; markup.scm ends here
