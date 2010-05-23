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
   #:open-connection #:close-connection #:with-connection
   #:redis-error #:text
   #:quit #:auth #:ping
   #:set #:get #:getset #:mget #:setnx #:incr #:incrby #:decr #:decrby #:exists #:del #:type
   #:keys #:randomkey #:rename #:renamenx #:dbsize #:expire #:ttl
   #:rpush #:lpush #:llen #:lrange #:ltrim #:lindex #:lset #:lrem #:lpop #:rpop
   #:sadd #:srem #:spop #:smove #:scard #:sismember #:sinter #:sinterstore
   #:sunion #:sunionstore #:sdiff #:sdiffstore #:smembers
   #:zadd #:zrem #:zincrby #:zrange #:zrevrange #:zrangebyscore #:zcard #:zscore #:zremrangebyscore
   #:select #:move #:flushdb #:flushall
   #:sort
   #:save #:bgsave #:lastsave #:shutdown
   #:info #:slaveof))

(in-package #:lredis)

(defparameter *port* 6379)
(defparameter *host* "localhost")

(defvar *connection*)

(defclass connection ()
  ((socket :initarg :socket :accessor connection-socket)))

(defun open-connection (&optional host port)
  (when (null host) (setf host *host*))
  (when (null port) (setf port *port*))
  (make-instance 'connection
                 :socket (usocket:socket-connect host port :element-type '(unsigned-byte 8))))

(defun close-connection (connection)
  (when (connection-socket connection)
    (usocket:socket-close (connection-socket connection))
    (setf (connection-socket connection) nil)))

(defun connection-stream (connection)
  (usocket:socket-stream (connection-socket connection)))

(defmacro with-connection ((&key connection host port) &body forms)
  (when (null connection) (setf connection '*connection*))
  `(let ((,connection (open-connection ,host ,port)))
     (unwind-protect
          (progn ,@forms)
       (when ,connection
         (close-connection ,connection)))))

(define-condition redis-error (error)
  ((text :initarg :text :accessor text))
  (:report (lambda (c s) (format s "Redis error: ~A" (text c)))))

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
      (read-delimited-bytes (connection-stream connection))
    (ecase magic
      (43 (babel:octets-to-string line))
      (36 (let ((n (parse-integer (babel:octets-to-string line))))
            (cond ((= n -1) nil)
                  (t (let ((data (make-array n :element-type '(unsigned-byte 8))))
                       (read-sequence data (connection-stream connection))
                       (read-byte (connection-stream connection)) ; CR
                       (read-byte (connection-stream connection)) ; LF
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
                            `(&key ((:connection ,connection) *connection*))
                            (unless no-read
                              `(((:octets ,octets))))))
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
                              (write-key element ,out)
                              (write-byte 32 ,out)))
                          ((cons symbol (cons (eql :integer) null))
                           `(princ ,(car x) ,out))
                          ((cons symbol (cons (eql :bulk) null))
                           `(progn
                              (when (stringp ,(car x))
                                (setf ,(car x)
                                      (babel:string-to-octets ,(car x))))
                              (princ (length ,(car x)) ,out)
                              (write-sequence #(13 10) ,out)
                              (write-sequence ,(car x) ,out)))))
                (write-sequence #(13 10) ,out))))
        `(defun ,name ,inputs
           (write-sequence ,octets-form (connection-stream ,connection))
           (force-output (connection-stream ,connection))
           ,(if no-read
                `(values)
                `(translate-result (read-reply ,connection) ,octets ,booleanize ,split)))))))

(define-command quit :no-read)
(define-command auth (password :string))
(define-command ping)
(define-command set (key :key) (value :bulk))
(define-command get (key :key))
(define-command getset (key :key) (value :bulk))
(define-command mget (keys :keys))
(define-command setnx :boolean (key :key) (value :bulk))
(define-command incr (key :key))
(define-command incrby (key :key) (integer :integer))
(define-command decr (key :key))
(define-command decrby (key :key) (integer :integer))
(define-command exists :boolean (key :key))
(define-command del :boolean (key :key))
(define-command type (key :key))
(define-command keys :split (pattern :key))
(define-command randomkey)
(define-command rename (oldname :key) (newname :key))
(define-command renamenx :boolean (oldname :key) (newname :key))
(define-command dbsize)
(define-command expire :boolean (key :key) (seconds :integer))
(define-command ttl (key :key))
(define-command rpush (key :key) (value :bulk))
(define-command lpush (key :key) (value :bulk))
(define-command llen (key :key))
(define-command lrange (key :key) (start :integer) (end :integer))
(define-command ltrim (key :key) (start :integer) (end :integer))
(define-command lindex (key :key) (index :integer))
(define-command lset (key :key) (index :integer) (value :bulk))
(define-command lrem (key :key) (count :integer) (value :bulk))
(define-command lpop (key :key))
(define-command rpop (key :key))
(define-command sadd :boolean (key :key) (member :bulk))
(define-command srem :boolean (key :key) (member :bulk))
(define-command spop (key :key))
(define-command smove :boolean (srckey :key) (dstkey :key))
(define-command scard (key :key))
(define-command sismember :boolean (key :key) (member :key))
(define-command sinter (keys :keys))
(define-command sinterstore (dstkey :key) (keys :keys))
(define-command sunion (keys :keys))
(define-command sunionstore (dstkey :key) (keys :keys))
(define-command sdiff (keys :keys))
(define-command sdiffstore (dstkey :key) (keys :keys))
(define-command smembers (key :key))
(define-command zadd (key :key) (score :integer) (member :bulk))
(define-command zrem (key :key) (member :bulk))
(define-command zincrby (key :key) (increment :integer) (member :bulk))
(define-command zrange (key :key) (start :integer) (end :integer))
(define-command zrevrange (key :key) (start :integer) (end :integer))
(define-command zrangebyscore (key :key) (min :integer) (max :integer))
(define-command zcard (key :key))
(define-command zscore (key :key) (element :bulk))
(define-command zremrangebyscore (key :key) (min :integer) (max :integer))
(define-command select (index :integer))
(define-command move :boolean (key :key) (dbindex :integer))
(define-command flushdb)
(define-command flushall)
(define-command save)
(define-command bgsave)
(define-command lastsave)
(define-command shutdown :no-read)
(define-command info)
(define-command slaveof (host :string) (port :integer))

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
   (connection-stream connection))
  (force-output (connection-stream connection))
  (translate-result (read-reply connection) octets nil nil))
