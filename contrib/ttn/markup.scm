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
  '((raw   text)
    (mtype text)
    (mdata text)))

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
           (insert possibly-marked-text #f #f)
           (let* ((form possibly-marked-text)
                  (type (car form)))
             (insert (case type
                       ((url) ((if (= 3 (length form)) caddr cadr) form))
                       ((email) (cadr form))
                       (else (error (format #f "bad form: ~A" form))))
                     (symbol->string (car form))
                     ((case type
                        ((url) cadr)
                        ((email) caddr))
                      form)))))))

  (define (add-description ls)
    (pg-oid-value
     ((c 'insert-col-values) '(description)
      (map add-possibly-marked-text ls))))

  (define (>>table heading manager)
    (write-line heading)
    (display-table (tuples-result->table ((manager 'select) "*"))))

  (define (get-one-row manager oid)
    (car ((manager 'tuples-result->alists)
          ((manager 'select) "*" (where-clausifier
                                  (format #f "oid = ~A" oid))))))

  (define (tree<-possibly-marked-text oid)
    (let* ((alist (get-one-row m oid))
           (raw (assq-ref alist 'raw))
           (mtype (assq-ref alist 'mtype))
           (mdata (assq-ref alist 'mdata)))
      (if (string=? "" mtype)
          raw
          (case (string->symbol mtype)
            ((url) (list "<A HREF=\"" mdata "\">" raw "</A>"))
            ((email) (list "<A HREF=\"mailto:" mdata "\">" raw "</A>"))
            (else (error (format #f "bad markup type: ~A" mtype)))))))

  (define (>>description oid)
    (let* ((alist (get-one-row c oid))
           (i (assq-ref alist 'i))
           (description (assq-ref alist 'description)))
      (format #t "description for: ~A\n" i)
      (for-each (lambda (oid)
                  (display-tree (tree<-possibly-marked-text oid)))
                description))
    (newline))

  (define (delete-description oid)
    (let* ((alist (get-one-row c oid))
           (description (assq-ref alist 'description)))
      (for-each (lambda (oid)
                  ((m 'delete-rows) (format #f "oid = ~A" oid)))
                description)
      ((c 'delete-rows) (format #f "oid = ~A" oid))))

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

  (let ((desc-oids (map add-description *samples*)))
    (format #t "desc-oids: ~A\n" desc-oids)

    (>>table "markup" m)
    (>>table "client" c)

    (for-each >>description desc-oids)

    (write-line (delete-description (list-ref desc-oids 0)))
    (write-line (delete-description (list-ref desc-oids 1)))
    (set! desc-oids (cddr desc-oids))

    (for-each >>description desc-oids)

    (>>table "markup" m)
    (>>table "client" c))

  (write-line ((c 'drop)))
  (write-line ((m 'drop))))

;;; markup.scm ends here
