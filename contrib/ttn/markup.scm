;;; Copyright (C) 2003 Thien-Thi Nguyen
;;; This program is provided under the terms of the GNU GPL, version 2.
;;; See http://www.fsf.org/copyleft/gpl.html for details.

;;; version: 3

(define-module (markup)
  :use-module (database postgres-table)
  :use-module (ttn display-table)
  :use-module (ice-9 pretty-print))

;; database

(define *db* (getenv "USER"))

;; table defs

(define *client-defs*
  '((project     text)
    (description int4)))                ; item count

;; display utilities

(define (display-tree tree)
  (if (list? tree)
      (for-each display-tree tree)
      (display tree)))

(define (>>table heading manager)
  (write-line heading)
  (display-table (tuples-result->table ((manager 'select) "*"))))

;; stylized selection and result transform

(define (get-rows manager order as . args)
  ((manager 'tuples-result->object-alist)
   ((manager 'select) "*"
    (where-clausifier (apply format #f as args))
    (if order (format #f "ORDER BY ~A" order) ""))))

(define (get-one-row manager as . args)
  (map (lambda (col)
         (cons (car col) (cadr col)))
       (apply get-rows manager #f as args)))

;; markup table interface: extend pgtable-manager

(define (markup-table-manager db name)
  (let ((m (pgtable-manager
            db name
            '((raw   text)
              (mtype text)
              (mdata text)
              (back  text)              ; client back pointer
              (seq   int4)))))          ; client sequence number

    (define (add ls back . canonicalize)
      (let loop ((ls ls) (count 0))
        (if (null? ls)
            count                       ; retval
            (let ((item (car ls)))
              (apply (m 'insert-col-values) '(back seq raw mtype mdata)
                     back count
                     (cond ((string? item) (list item #f #f))
                           ((null? canonicalize) item)
                           (else ((car canonicalize) item))))
              (loop (cdr ls) (1+ count))))))

    (define (del back)
      ((m 'delete-rows) (format #f "back = '~A'" back)))

    (define (upd ls back . canonicalize)
      (del back)                        ; ugh
      (add ls back canonicalize))

    (define (->tree back max-seq render)
      (let ((alist (get-rows m "seq" "back = '~A' AND seq < ~A"
                             back max-seq)))
        (map (lambda (raw mtype mdata)
               (if (string=? "" mtype)
                   raw
                   (render raw (string->symbol mtype) mdata)))
             (assq-ref alist 'raw)
             (assq-ref alist 'mtype)
             (assq-ref alist 'mdata))))

    (lambda (choice)                    ; retval
      (case choice
        ((add) add)
        ((del) del)
        ((upd) upd)
        ((->tree) ->tree)
        (else (m choice))))))

;; play!

(let ((m (markup-table-manager *db* "markup_play"))
      (c (pgtable-manager *db* "client_play" *client-defs*)))

  (define (canonicalize-markup form)
    ;; Take one of:
    ;;   (url URL)
    ;;   (url URL TEXT)
    ;;   (email TEXT ADDR)
    ;; and return canonical form: (RAW MTYPE MDATA).
    ;; In the first case, the URL is taken to be both RAW and MDATA.
    (let ((type (car form)))
      (list (case type
              ((url) ((if (= 3 (length form)) caddr cadr) form))
              ((email) (cadr form))
              (else (error (format #f "bad form: ~A" form))))
            (symbol->string (car form))
            ((case type
               ((url) cadr)
               ((email) caddr))
             form))))

  (define (DESC: x) (format #f "description:~A" x))

  (define (add-project ext)             ; external representation
    (let ((project (car (assq-ref ext 'name))))
      ((c 'insert-values)
       project
       ((m 'add) (assq-ref ext 'description)
        (DESC: project) canonicalize-markup))))

  (define (render-markup raw mtype mdata)
    (case mtype
      ((url) (list "<A HREF=\"" mdata "\">" raw "</A>"))
      ((email) (list "<A HREF=\"mailto:" mdata "\">" raw "</A>"))
      (else (error (format #f "bad markup type: ~A" mtype)))))

  (define (>>project project)
    (let* ((alist (get-one-row c "project = '~A'" project))
           (description (assq-ref alist 'description)))
      (format #t "project:     ~A\ndescription: " project)
      (display-tree ((m '->tree) (DESC: project) description render-markup)))
    (newline))

  (define (delete-project project)
    ((m 'del) (DESC: project))
    ((c 'delete-rows) (format #f "project = '~A'" project)))

  (define (externalize-markup raw mtype mdata)
    (case mtype
      ((url) (if (string=? raw mdata)
                 (list mtype raw)
                 (list mtype mdata raw)))
      ((email) (list mtype raw mdata))
      (else (error (format #f "unexpected mtype: ~A" mtype)))))

  (define (dump-project project)
    (let* ((alist (get-one-row c "project = '~A'" project))
           (description ((m '->tree) (DESC: project)
                         (assq-ref alist 'description)
                         externalize-markup))
           (form (list (list 'name project)
                       (cons 'description description))))
      (pretty-print form)))

  (define *samples*
    (list
     '((name "guile projects list maintenance")
       (description
        "This is the guile scheme code that maintains the "
        (url "http://www.glug.org/projects/list.html"
             "guile projects list")
        ".  There are configurations for glug.org as well for "
        (url "http://www.gnu.org/software/guile/gnu-guile-projects.html"
             "the gnu.org subset of the list") "."))
     '((name "guile-pg")
       (description
        "An interface to PostgreSQL from guile."))
     '((name "snd")
       (description
        (url "http://www-ccrma.stanford.edu/software/snd/")
        " is where you can find Snd."))
     '((name "hobbit")
       (description
        "The hobbit author is "
        (email "Tanel Tammet" "tammet@cs.chalmers.se") "."))))

  (define (*names* ls) (map (lambda (x) (car (assq-ref x 'name))) ls))

  ;;((m 'drop)) ((c 'drop))

  (write-line ((m 'create)))
  (write-line ((c 'create)))

  (for-each write-line (map add-project *samples*))

  (>>table "markup" m)
  (>>table "client" c)

  (for-each >>project (*names* *samples*))

  (write-line (delete-project (list-ref (*names* *samples*) 0)))
  (write-line (delete-project (list-ref (*names* *samples*) 1)))

  (>>table "markup" m)
  (>>table "client" c)

  (for-each dump-project (*names* (cddr *samples*)))

  (write-line ((c 'drop)))
  (write-line ((m 'drop))))

;;; markup.scm ends here
