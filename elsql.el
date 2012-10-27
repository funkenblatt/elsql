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
                 '(query < > + - * = != || or and not exists is in >= <=))) 
      (str "(" (sql-expr x) ")")
    (sql-expr x)))

(defun sql-case (l)
  (do-wad
   (sublist-n (cdr l) 2)
   (lc (if (eq (car x) 'else)
	   (str "else " (sql-expr (cadr x)))
	 (str "when " (sql-expr (car x)) 
	      " then " (sql-expr (cadr x))))
       x _)
   (strjoin " " _)
   (str "case " _ " end")))

(defun func-to-sql (l)
  (case (car l)
    ((< > + - * = != || or and not exists is in >= <=) 
     (if (and (= (length l) 2)
              (memq (car l) '(not - exists)))
	  (str (car l) " (" (sql-expr (cadr l)) ")")
       (strjoin 
	(str " " (car l) " ")
	(mapcar 'sql-maybe-paren (cdr l)))))
    (case (sql-case l))
    ((join left-join right-join full-outer-join) (apply 'sql-join l)) 
    (query (render-query (cdr l)))
    (over (str (sql-expr (cadr l))
	       " OVER ("
	       (strjoin " " (mapcar 'sql-expr (cddr l)))
	       ")"))
    (cast (str (sql-maybe-paren (cadr l)) "::" (caddr l)))
    (desc (str (sql-expr (cadr l)) " desc"))
    (as (str (sql-maybe-paren (cadr l)) " as " (caddr l)))
    (quote (str "(" (sql-comma-joined (cadr l)) ")"))
    ((nil) "NULL")
    (otherwise (str (car l) "(" (sql-comma-joined (cdr l)) ")"))))

(defun de-hyphen (s)
  (replace-regexp-in-string "-" " " (str s)))

(defun sql-join (join-type a b condition)
  (str (sql-maybe-paren a) " " (de-hyphen join-type) " " (sql-maybe-paren b)
       " on " (sql-expr condition)))

(defun str-to-sql (s)
  (str "E'" (replace-regexp-in-string "'" "''" s) "'"))

(defun sql-comma-joined (exprs)
  (strjoin ", " (mapcar 'sql-expr exprs)))

(defun sql-maybe-clause (query key clause fun)
  (do-wad
   (assoc-default key query)
   (if _ (str " " clause " " (funcall fun _)) "")))

(defun sql-with (query)
  (do-wad
   (loop for q in (butlast (cdr query))
         collect (str (car q) " as (" (render-query (cdr q)) ")"))
   (strjoin ", " _)
   (str "with " _ " " (render-query (car (last query))))))

(defun render-query (query)
  (if (symbolp (car query))
      (case (car query)
	(with (sql-with query))
	((union union-all) (strjoin 
			    (str " " (de-hyphen (car query)) " ")
			    (mapcar 'render-query (cdr query))))
	(otherwise (render-select query)))
    (render-select query)))

(defun render-select (query)
  (if (symbolp (car query)) (push (list 'from (pop query)) query)) 
  (let* ((select (do-wad (assoc-default 'select query)
			 (if _ (sql-comma-joined _) '*)))
         (from (sql-maybe-clause query 'from "from" 'sql-comma-joined))
         (where (sql-maybe-clause 
                 query 'where "where" 
                 (lambda (x) (sql-expr (cons 'and x)))))
         (order (sql-maybe-clause query 'order "order by" 'sql-comma-joined))
         (offset (sql-maybe-clause query 'offset "offset" 'sql-comma-joined))
         (limit (sql-maybe-clause query 'limit "limit" 'sql-comma-joined))
         (group (sql-maybe-clause query 'group "group by" 'sql-comma-joined)))
    (interp "select #,[select]#,[from]#,[where]#,[group]#,[order]#,[limit]#,[offset]")))
