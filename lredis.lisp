;;;; +----------------------------------------------------------------+
;;;; | LREDIS - Lisp Redis bindings                       DEATH, 2009 |
;;;; +----------------------------------------------------------------+

;;;; Ladies and Gentlemen, may I present to you...

(defpackage #:lredis
  (:nicknames #:redis)
  (:use #:cl)
  (:shadow #:set #:get #:type #:sort)
  (:export
   #:*port* #:*host* #:*connection*
   #:with-connection
   #:redis-error #:text
   #:quit #:auth #:ping
   #:set #:get #:getset #:mget #:setnx #:incr #:incrby #:decr #:decrby #:exists #:del #:type
   #:keys #:randomkey #:rename #:renamenx #:dbsize #:expire #:ttl
   #:rpush #:lpush #:llen #:lrange #:ltrim #:lindex #:lset #:lrem #:lpop #:rpop
   #:sadd #:srem #:spop #:smove #:scard #:sismember #:sinter #:sinterstore
   #:sunion #:sunionstore #:sdiff #:sdiffstore #:smembers
   #:select #:move #:flushdb #:flushall
   #:sort
   #:save #:bgsave #:lastsave #:shutdown
   #:info #:slaveof))

(in-package #:lredis)

(defparameter *port* 6379)
(defparameter *host* "localhost")

(defvar *connection*)

(defclass connection ()
  ((socket-stream :initarg :socket-stream :accessor socket-stream)))

(defun call-with-connection (fn host port)
  (when (null host) (setf host *host*))
  (when (null port) (setf port *port*))
  (usocket:with-client-socket (socket stream host port :element-type '(unsigned-byte 8))
    (let ((conn (make-instance 'connection :socket-stream stream)))
      (funcall fn conn))))

(defmacro with-connection ((&key connection host port) &body forms)
  (when (null connection) (setf connection '*connection*))
  `(call-with-connection (lambda (,connection) ,@forms) ,host ,port))

(define-condition redis-error (error)
  ((text :initarg :text :accessor text)))

(defun translate-result (result want-octets booleanize split)
  (labels ((str (x) (if split (values (split-sequence:split-sequence #\Space x)) x)))
    (etypecase result
      (null result)
      (integer (if booleanize (= 1 result) result))
      (string (if want-octets (babel:string-to-octets result) (str result)))
      (vector (if want-octets result (str (babel:octets-to-string result))))
      (cons (map-into result (lambda (x) (translate-result x want-octets booleanize split)) result)))))

(defun write-key (key stream)
  (etypecase key
    ((or string symbol) (princ key stream))
    (cons (format stream "~{~A~^:~}" key))))

(defun read-delimited-bytes (stream)
  (let ((magic (read-byte stream)))
    (let ((line (make-array 0 :adjustable t :fill-pointer 0 :element-type '(unsigned-byte 8))))
      (loop for x = (read-byte stream)
            until (= x 13)
            do (vector-push-extend x line)
            finally (read-byte stream))
      (values magic line))))

(defun read-reply (connection)
  (multiple-value-bind (magic line)
      (read-delimited-bytes (socket-stream connection))
    (ecase magic
      (43 (babel:octets-to-string line))
      (36 (let ((n (parse-integer (babel:octets-to-string line))))
            (cond ((= n -1) nil)
                  (t (let ((data (make-array n :element-type '(unsigned-byte 8))))
                       (read-sequence data (socket-stream connection))
                       (read-byte (socket-stream connection)) ; CR
                       (read-byte (socket-stream connection)) ; LF
                       data)))))
      (42 (let ((n (parse-integer (babel:octets-to-string line))))
            (cond ((= n -1) (values nil nil))
                  (t (values (loop repeat n collecting (read-reply connection)) t)))))
      (58 (values (parse-integer (babel:octets-to-string line))))
      (45 (error 'redis-error :text (babel:octets-to-string line))))))

(defmacro define-command (name &rest spec)
  (let ((booleanize (when (eq (car spec) :boolean) (pop spec)))
        (split (when (eq (car spec) :split) (pop spec)))
        (no-read (when (eq (car spec) :no-read) (pop spec))))
    (push (symbol-name name) spec)
    (let ((connection (gensym)) (octets (gensym)) (out (gensym)))
      (let ((inputs (append (mapcan (lambda (x)
                                      (when (consp x)
                                        (list (car x))))
                                    spec)
                            `(&key (,connection *connection*))
                            (unless no-read
                              `(,octets))))
            (octets-form
             `(babel-streams:with-output-to-sequence (,out)
                ,@(loop for first-time = t then nil
                        for x in spec
                        when (not first-time) collect `(write-byte 32 ,out)
                        when t collect
                        (etypecase x
                          (string
                           `(write-sequence ,(map 'vector #'char-code x) ,out))
                          ((cons symbol (cons (eql :string) null))
                           `(write-string ,(car x) ,out))
                          ((cons symbol (cons (eql :key) null))
                           `(write-key ,(car x) ,out))
                          ((cons symbol (cons (eql :keys) null))
                           `(dolist (element ,(car x))
                              (write-key element ,out)))
                          ((cons symbol (cons (eql :integer) null))
                           `(princ ,(car x) ,out))
                          ((cons symbol (cons (eql :bulk) null))
                           `(progn
                              (princ (length ,(car x)) ,out)
                              (write-sequence #(13 10) ,out)
                              (write-sequence ,(car x) ,out)))))
                (write-sequence #(13 10) ,out))))
        `(defun ,name ,inputs
           (write-sequence ,octets-form (socket-stream ,connection))
           (force-output (socket-stream ,connection))
           ,(if no-read
                `(values)
                `(translate-result (read-reply ,connection) ,octets ,booleanize ,split)))))))

(defmacro define-commands (&body commands)
  `(progn
     ,@(mapcar (lambda (x) `(define-command ,@x)) commands)))

(define-commands
  (quit :no-read)
  (auth (password :string))
  (ping)
  (set (key :key) (value :bulk))
  (get (key :key))
  (getset (key :key) (value :bulk))
  (mget (keys :keys))
  (setnx :boolean (key :key) (value :bulk))
  (incr (key :key))
  (incrby (key :key) (integer :integer))
  (decr (key :key))
  (decrby (key :key) (integer :integer))
  (exists :boolean (key :key))
  (del :boolean (key :key))
  (type (key :key))
  (keys :split (pattern :key))
  (randomkey)
  (rename (oldname :key) (newname :key))
  (renamenx :boolean (oldname :key) (newname :key))
  (dbsize)
  (expire :boolean (key :key) (seconds :integer))
  (ttl (key :key))
  (rpush (key :key) (value :bulk))
  (lpush (key :key) (value :bulk))
  (llen (key :key))
  (lrange (key :key) (start :integer) (end :integer))
  (ltrim (key :key) (start :integer) (end :integer))
  (lindex (key :key) (index :integer))
  (lset (key :key) (index :integer) (value :bulk))
  (lrem (key :key) (count :integer) (value :bulk))
  (lpop (key :key))
  (rpop (key :key))
  (sadd :boolean (key :key) (member :bulk))
  (srem :boolean (key :key) (member :bulk))
  (spop (key :key))
  (smove :boolean (srckey :key) (dstkey :key))
  (scard (key :key))
  (sismember :boolean (key :key) (member :key))
  (sinter (keys :keys))
  (sinterstore (dstkey :key) (keys :keys))
  (sunion (keys :keys))
  (sunionstore (dstkey :key) (keys :keys))
  (sdiff (keys :keys))
  (sdiffstore (dstkey :key) (keys :keys))
  (smembers (key :key))
  (select (index :integer))
  (move :boolean (key :key) (dbindex :integer))
  (flushdb)
  (flushall)
  (save)
  (bgsave)
  (lastsave)
  (shutdown :no-read)
  (info)
  (slaveof (host :string) (port :integer)))

(defun sort (key &key (connection *connection*) octets order limit-start limit-end by get alpha)
  (write-sequence
   (babel-streams:with-output-to-sequence (out)
     (write-sequence "SORT " out)
     (write-key key out)
     (when by
       (write-sequence " BY " out)
       (write-key by out))
     (when (and limit-start limit-end)
       (write-sequence " LIMIT " out)
       (princ limit-start out)
       (write-byte 32 out)
       (princ limit-end out))
     (when get
       (when (stringp get)
         (setf get (list get)))
       (dolist (x get)
         (write-sequence " GET " out)
         (write-key x out)))
     (ecase order
       ((:asc :ascending) (write-sequence " ASC" out))
       ((:desc :descending) (write-sequence " DESC" out))
       ((nil)))
     (when alpha
       (write-sequence " ALPHA" out))
     (write-sequence #(13 10) out))
   (socket-stream connection))
  (force-output (socket-stream connection))
  (translate-result (read-reply connection) octets nil nil))
