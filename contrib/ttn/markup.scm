;;; Copyright (C) 2003 Thien-Thi Nguyen
;;; This program is provided under the terms of the GNU GPL, version 2.
;;; See http://www.fsf.org/copyleft/gpl.html for details.

(define-module (markup)
  :use-module (database postgres)
  :use-module (database postgres-types)
  :use-module (database postgres-table)
  :use-module (ttn display-table))

;; database

(define *db* (getenv "USER"))

;; table defs

(define *markup-defs*
  '((raw   text)
    (mtype text)
    (mdata text)
    (back  text)                        ; client info (back pointer)
    (seq   int4)))                      ; client sequence number

(define *client-defs*
  '((project     text)
    (description int4)))                ; item count

;; display-tree

(define (display-tree tree)
  (if (list? tree)
      (for-each display-tree tree)
      (display tree)))

;; play!

(let ((m (pgtable-manager *db* "markup_play" *markup-defs*))
      (c (pgtable-manager *db* "client_play" *client-defs*)))

  ;; Add POSSIBLY-MARKED-TEXT for BACK/SEQ to markup table.
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
  ;; BACK is string indicating the client using this row.  SEQ is the sequence
  ;; number (integer) of this row in the client.
  ;;
  (define (add-possibly-marked-text possibly-marked-text back seq)
    (let ((insert (lambda (raw mtype mdata)
                    ((m 'insert-values) raw mtype mdata back seq))))
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
                     form))))))

  (define (add-project pair)
    ((c 'insert-values) (car pair)
     (let loop ((count 0) (ls (cdr pair)))
       (if (null? ls)
           count
           (begin
             (add-possibly-marked-text
              (car ls)
              (format #f "description:~A" (car pair))
              count)
             (loop (1+ count) (cdr ls)))))))

  (define (>>table heading manager)
    (write-line heading)
    (display-table (tuples-result->table ((manager 'select) "*"))))

  (define (get-rows manager order as . args)
    ((manager 'tuples-result->object-alist)
     ((manager 'select) "*"
      (where-clausifier (apply format #f as args))
      (if order (format #f "ORDER BY ~A" order) ""))))

  (define (get-one-row manager as . args)
    (map (lambda (col)
           (cons (car col) (cadr col)))
         (apply get-rows manager #f as args)))

  (define (tree<-possibly-marked-text back max-seq)
    (let ((alist (get-rows m "seq" "back = '~A' AND seq < ~A"
                           back max-seq)))
      (map (lambda (raw mtype mdata)
             (if (string=? "" mtype)
                 raw
                 (case (string->symbol mtype)
                   ((url) (list "<A HREF=\"" mdata "\">" raw "</A>"))
                   ((email) (list "<A HREF=\"mailto:" mdata "\">" raw "</A>"))
                   (else (error (format #f "bad markup type: ~A" mtype))))))
           (assq-ref alist 'raw)
           (assq-ref alist 'mtype)
           (assq-ref alist 'mdata))))

  (define (>>project project)
    (let* ((alist (get-one-row c "project = '~A'" project))
           (description (assq-ref alist 'description)))
      (format #t "project: ~A\ndescription: ~A\n" project description)
      (display-tree (tree<-possibly-marked-text
                     (format #f "description:~A" project)
                     description)))
    (newline))

  (define (delete-project project)
    (let* ((alist (get-one-row c "project = '~A'" project))
           (description (assq-ref alist 'description)))
      ((m 'delete-rows)
       (format #f "back = '~A' AND seq < ~A"
               (format #f "description:~A" project) description))
      ((c 'delete-rows) (format #f "project = '~A'" project))))

  (define *samples*
    (list
     (cons "guile projects list maintenance"
           '("This is the guile scheme code that maintains the "
             (url "http://www.glug.org/projects/list.html"
                  "guile projects list")
             ".  There are configurations for glug.org as well for "
             (url "http://www.gnu.org/software/guile/gnu-guile-projects.html"
                  "the gnu.org subset of the list") "."))
     (cons "guile-pg"
           '("An interface to PostgreSQL from guile."))
     (cons "snd"
           '((url "http://www-ccrma.stanford.edu/software/snd/")
             " is where you can find Snd."))
     (cons "hobbit"
           '("The hobbit author is "
             (email "Tanel Tammet" "tammet@cs.chalmers.se") "."))))

  ;;((m 'drop)) ((c 'drop))

  (write-line ((m 'create)))
  (write-line ((c 'create)))

  (for-each write-line (map add-project *samples*))

  (>>table "markup" m)
  (>>table "client" c)

  (for-each >>project (map car *samples*))

  (write-line (delete-project (list-ref (map car *samples*) 0)))
  (write-line (delete-project (list-ref (map car *samples*) 1)))

  ;;(for-each >>project (map car (cddr *samples*)))

  (>>table "markup" m)
  (>>table "client" c)

  (write-line ((c 'drop)))
  (write-line ((m 'drop))))

;;; markup.scm ends here
