(require 'cl)

(defun str (&rest objs)
  (apply 'concat (mapcar (lambda (obj) (format "%s" obj)) objs)))

(defmacro sql-interp (&rest templates)
  (with-temp-buffer
    (dolist (s templates) (insert s))
    (let ((last-point (point-min)) exprs)
      (goto-char (point-min))
      (while (re-search-forward "#," nil t)
	(push (buffer-substring last-point (match-beginning 0)) exprs)
	(let ((expr (read (current-buffer))))
	  (push (if (vectorp expr) (aref expr 0) expr) exprs)
	  (setf last-point (point))))
      (push (buffer-substring (point) (point-max)) exprs)
      (cons 'str (nreverse exprs)))))

(defun strjoin (sep strings)
  (let (out) 
    (dolist (i strings (apply 'concat (nreverse (cdr out))))
      (push i out) (push sep out))))

(defmacro chain (&rest exprs)
  `(let* ((_ ,(car exprs))
          ,@(mapcar (lambda (x)
                      (if (symbolp x) `(_ (,x _)) `(_ ,x)))
                    (cdr exprs)))
     _))

(defun sql-expr (datum)
  (if (not datum) 
      "NULL"
    (case (type-of datum)
      (cons (func-to-sql datum))
      (vector (func-to-sql (list 'quote (append datum nil))))
      ((integer symbol float) (str datum))
      (string (str-to-sql datum)))))

(defun sql-maybe-paren (x)
  (if (and (consp x) 
           (memq (car x) 
                 '(query ilike like < > + - * = != || or and not exists is in >= <=))) 
      (str "(" (sql-expr x) ")")
    (sql-expr x)))

(defun sql-case-clause (x)
  (if (eq (car x) 'else)
      (str "else " (sql-expr (cadr x)))
    (str "when " (sql-expr (car x)) 
         " then " (sql-expr (cadr x)))))

(defun sql-case (l)
  (chain
   (cdr l)
   (mapcar 'sql-case-clause _)
   (strjoin " " _)
   (str "case " _ " end")))

(defun func-to-sql (l)
  (case (car l)
    ((/ < > + - * = != || or and not exists is in >= <= like ilike ~) 
     (if (and (= (length l) 2)
              (memq (car l) '(not - exists)))
	  (str (car l) " (" (sql-expr (cadr l)) ")")
       (strjoin 
	(str " " (car l) " ")
	(mapcar 'sql-maybe-paren (cdr l)))))
    (interval (str "interval " (sql-expr (cadr l))))
    (aref (str "(" (sql-expr (cadr l)) ")" "[" (sql-expr (caddr l)) "]"))
    (case (sql-case l))
    ((join left-join right-join full-outer-join) (apply 'sql-join l)) 
    (query (render-query (cdr l)))
    (distinct (str "DISTINCT " (sql-expr (cadr l))))
    (over (str (sql-expr (cadr l))
	       " OVER ("
	       (strjoin " " (mapcar 'sql-expr (cddr l)))
	       ")"))
    (between (str (sql-expr (cadr l)) " BETWEEN "
                  (sql-expr (caddr l)) " AND " (sql-expr (cadddr l))))
    (cast (str (sql-maybe-paren (cadr l)) "::" (caddr l)))
    (array (str "ARRAY[" (sql-comma-joined (cdr l)) "]"))
    (desc (str (sql-expr (cadr l)) " desc"))
    (as (str (sql-maybe-paren (cadr l)) " as " (caddr l)))
    (quote (str "(" (sql-comma-joined (cadr l)) ")"))
    ((nil) "NULL")
    (otherwise (str (car l) "(" (sql-comma-joined (cdr l)) ")"))))

(defun de-hyphen (s)
  (replace-regexp-in-string "-" " " (str s)))

(defun sql-join (join-type a b condition &optional right-condition)
  (if right-condition
      (sql-interp "#,(sql-maybe-paren a) #,(de-hyphen join-type) #,(sql-maybe-paren b) "
	      "ON #,[a].#,condition = #,[b].#,right-condition")
    (str (sql-maybe-paren a) " " (de-hyphen join-type) " " (sql-maybe-paren b)
	 " on " (sql-expr condition))))

(defun str-to-sql (s)
  (str "E'" 
       (replace-regexp-in-string
	"\n" "\\n"
	(replace-regexp-in-string "'" "''" s) nil t)
       "'"))

(defun sql-comma-joined (exprs)
  (strjoin ", " (mapcar 'sql-expr exprs)))

(defun sql-maybe-clause (query key clause fun)
  (chain
   (assoc-default key query)
   (if _ (str " " clause " " (funcall fun _)) "")))

(defun sql-with (query)
  (chain
   (loop for q in (butlast (cdr query))
         collect (str (car q) " as (" (render-query (cdr q)) ")"))
   (strjoin ", " _)
   (str "with " _ " " (render-query (car (last query))))))

(defun render-query (query)
  (if (symbolp (car query))
      (case (car query)
	(with (sql-with query))
	(values (str "values " (sql-comma-joined (cdr query))))
	((union union-all intersect) 
         (strjoin 
          (str " " (de-hyphen (car query)) " ")
          (mapcar 'render-query (cdr query))))
	(otherwise (render-select (cdr query))))
    (render-select query)))

(defun render-select (query)
  (let* ((select (chain (assoc-default 'select query)
			 (if _ (sql-comma-joined _) '*)))
         (from (sql-maybe-clause query 'from "from" 'sql-comma-joined))
         (where (sql-maybe-clause 
                 query 'where "where" 
                 (lambda (x) (sql-expr (cons 'and x)))))
	 (having (sql-maybe-clause
		  query 'having "having"
		  (lambda (x) (sql-expr (cons 'and x)))))
         (order (sql-maybe-clause query 'order "order by" 'sql-comma-joined))
         (offset (sql-maybe-clause query 'offset "offset" 'sql-comma-joined))
         (limit (sql-maybe-clause query 'limit "limit" 'sql-comma-joined))
         (group (sql-maybe-clause query 'group "group by" 'sql-comma-joined)))
    (sql-interp "select #,[select]#,[from]#,[where]#,[group]#,[having]#,[order]#,[limit]#,[offset]")))

(defun render-update (table clauses)
  (let* ((table (sql-expr table))
         (assigns (sql-maybe-clause clauses 'set "set" 'sql-comma-joined))
         (from (sql-maybe-clause clauses 'from "from" 'sql-comma-joined))
         (where (sql-maybe-clause 
                 clauses 'where "where"
                 (lambda (x) (sql-expr (cons 'and x))))))
    (sql-interp "update #,[table]#,[assigns]#,[from]#,[where]")))

(provide 'elsql)
