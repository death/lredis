;;;; +----------------------------------------------------------------+
;;;; | LREDIS - Lisp Redis bindings                       DEATH, 2009 |
;;;; +----------------------------------------------------------------+

;;;; Ladies and Gentlemen, may I present to you...

(defpackage #:lredis
  (:nicknames #:redis)
  (:use #:cl)
  (:shadow #:set #:get #:type #:sort #:append #:time)
  (:import-from #:babel #:string-to-octets #:octets-to-string)
  (:import-from #:babel-streams #:with-output-to-sequence)
  (:import-from #:usocket #:socket-connect #:socket-close #:socket-stream)
  (:export
   #:*port*
   #:*host*
   #:*connection*
   #:open-connection
   #:close-connection
   #:with-connection
   #:redis-error
   #:text
   ;; Connection handling
   #:quit
   #:auth
   ;; Commands operating on all kinds of values
   #:exists
   #:del
   #:type
   #:keys
   #:randomkey
   #:rename
   #:renamenx
   #:dbsize
   #:expire
   #:expireat
   #:pexpire
   #:pexpireat
   #:persist
   #:ttl
   #:pttl
   #:select
   #:move
   #:flushdb
   #:flushall
   #:dump
   #:restore
   ;; Commands operating on string values (incl. pseudo-integers)
   #:set
   #:get
   #:getset
   #:mget
   #:setnx
   #:setex
   #:mset
   #:msetnx
   #:incr
   #:incrby
   #:decr
   #:decrby
   #:append
   #:substr
   #:bitcount
   #:getbit
   #:setbit
   #:getrange
   #:setrange
   #:strlen
   #:psetex
   ;; Commands operating on lists
   #:rpush
   #:rpushx
   #:lpush
   #:lpushx
   #:llen
   #:lrange
   #:ltrim
   #:lindex
   #:lset
   #:lrem
   #:lpop
   #:rpop
   #:blpop
   #:brpop
   #:brpoplpush
   #:rpoplpush
   ;; Commands operating on sets
   #:sadd
   #:srem
   #:spop
   #:smove
   #:scard
   #:sismember
   #:sinter
   #:sinterstore
   #:sunion
   #:sunionstore
   #:sdiff
   #:sdiffstore
   #:smembers
   #:srandmember
   ;; Commands operating on sorted sets (zsets)
   #:zadd
   #:zrem
   #:zincrby
   #:zrank
   #:zrevrank
   #:zrange
   #:zrevrange
   #:zrangebyscore
   #:zcard
   #:zcount
   #:zscore
   #:zremrangebyrank
   #:zremrangebyscore
   #:zunionstore
   #:zinterstore
   ;; Commands operating on hashes
   #:hset
   #:hsetnx
   #:hget
   #:hmset
   #:hincrby
   #:hexists
   #:hdel
   #:hlen
   #:hkeys
   #:hvals
   #:hgetall
   #:hmget
   ;; Sorting
   #:sort
   ;; Transactions
   #:multi
   #:exec
   #:discard
   #:watch
   #:unwatch
   ;; Publish/Subscribe
   #:pubsub-bad-event
   #:pubsub-bad-event-event
   #:pubsub-spam
   #:publish
   #:subscribe
   #:unsubscribe
   #:pubsub-dispatch
   ;; Persistence control commands
   #:save
   #:bgsave
   #:lastsave
   #:shutdown
   #:bgrewriteaof
   ;; Remote server control commands
   #:info
   #:slaveof
   #:config
   #:echo
   #:ping
   #:time))

(in-package #:lredis)

(defparameter *port* 6379)
(defparameter *host* "localhost")

(defvar *connection*)

(defclass connection ()
  ((socket :initarg :socket :accessor connection-socket)
   (channel-subscriptions :initform (make-hash-table :test 'equal)
                          :accessor connection-channel-subscriptions)
   (pattern-subscriptions :initform (make-hash-table :test 'equal)
                          :accessor connection-pattern-subscriptions)))

(defun open-connection (&optional host port)
  (when (null host) (setf host *host*))
  (when (null port) (setf port *port*))
  (make-instance 'connection
                 :socket (socket-connect host port :element-type '(unsigned-byte 8))))

(defun close-connection (connection)
  (when (connection-socket connection)
    (socket-close (connection-socket connection))
    (setf (connection-socket connection) nil)
    (clrhash (connection-channel-subscriptions connection))
    (clrhash (connection-pattern-subscriptions connection))))

(defun connection-stream (connection)
  (socket-stream (connection-socket connection)))

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

(defun translate-result (result want-octets booleanize)
  (etypecase result
    (null result)
    (integer (if booleanize (= 1 result) result))
    (string (if want-octets (string-to-octets result) result))
    (vector (if want-octets result (octets-to-string result)))
    (cons (map-into result (lambda (x) (translate-result x want-octets booleanize)) result))))

(defun key-sequence (key)
  (etypecase key
    (cons (format nil "~{~A~^:~}" key))
    (sequence key)
    (symbol (symbol-name key))))

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
      (43 (octets-to-string line))
      (36 (let ((n (parse-integer (octets-to-string line))))
            (cond ((= n -1) nil)
                  (t (let ((data (make-array n :element-type '(unsigned-byte 8))))
                       (read-sequence data (connection-stream connection))
                       (read-byte (connection-stream connection)) ; CR
                       (read-byte (connection-stream connection)) ; LF
                       data)))))
      (42 (let ((n (parse-integer (octets-to-string line))))
            (cond ((= n -1) (values nil nil))
                  (t (values (loop repeat n collecting (read-reply connection)) t)))))
      (58 (values (parse-integer (octets-to-string line))))
      (45 (error 'redis-error :text (octets-to-string line))))))

(defun write-multi-bulk (sequences n out)
  (write-byte 42 out)
  (princ n out)
  (write-sequence #(13 10) out)
  (map nil (lambda (sequence)
             (when (stringp sequence)
               (setf sequence (string-to-octets sequence)))
             (write-byte 36 out)
             (princ (length sequence) out)
             (write-sequence #(13 10) out)
             (write-sequence sequence out)
             (write-sequence #(13 10) out))
       sequences))

(defmacro define-command (name &rest spec)
  (let ((booleanize (when (eq (car spec) :boolean) (pop spec)))
        (no-read (when (eq (car spec) :no-read) (pop spec)))
        (docstring (car (last spec)))
        (spec (butlast spec)))
    (unless (stringp (car spec))
      (push (symbol-name name) spec))
    (let ((inputs (cl:append (mapcan (lambda (x)
                                       (when (consp x)
                                         (list (if (eq (car x) 'list)
                                                   (cadr x)
                                                   (car x)))))
                                     spec)
                             `(&key (connection *connection*))
                             (unless no-read
                               `((octets nil))))))
      (labels ((sequence-adding-form (form)
                 `(progn
                    (push ,form sequences)
                    (incf nsequences)))
               (handle-arg (x)
                 (etypecase x
                   (string
                    (sequence-adding-form x))
                   ((cons (eql list))
                    (destructuring-bind (op var &rest types) x
                      (declare (ignore op))
                      `(do () ((null ,var))
                         ,@(mapcar (lambda (type)
                                     (handle-arg `((pop ,var) ,type)))
                                   types))))
                   ((cons t (cons (eql :string) null))
                    (sequence-adding-form (car x)))
                   ((cons t (cons (eql :key) null))
                    (sequence-adding-form `(key-sequence ,(car x))))
                   ((cons t (cons (eql :integer) null))
                    (sequence-adding-form `(princ-to-string ,(car x))))
                   ((cons t (cons (eql :bulk) null))
                    (sequence-adding-form (car x))))))
        `(defun ,name ,inputs
           ,docstring
           (let ((sequences '())
                 (nsequences 0))
             ,@(mapcar #'handle-arg spec)
             (write-sequence
              (with-output-to-sequence (out)
                (write-multi-bulk (nreverse sequences) nsequences out))
              (connection-stream connection)))
           (force-output (connection-stream connection))
           ,(if no-read
                `(values)
                `(translate-result (read-reply connection) octets ,booleanize)))))))

;; Connection handling
;; TODO: client commands

(define-command quit :no-read "Close the connection.")
(define-command auth (password :string) "Simple password authentication if enabled.")

;; Commands operating on all kinds of values
;; TODO: scan commands

(define-command exists :boolean (key :key) "Test if a key exists.")
(define-command del :boolean (key :key) "Delete a key.")
(define-command type (key :key) "Return the type of the value stored at key.")
(define-command keys (pattern :key) "Return all the keys matching a given pattern.")
(define-command randomkey "Return a random key from the key space.")
(define-command rename (oldname :key) (newname :key) "Rename the old key to the new one, superseding any existing key.")
(define-command renamenx :boolean (oldname :key) (newname :key) "Rename the old key to the new one unless a key with the new name already exists.")
(define-command dbsize "Return the number of keys in the current database.")
(define-command expire :boolean (key :key) (seconds :integer) "Expire key in a number of seconds from now.")
(define-command expireat :boolean (key :key) (timestamp :integer) "Set the expiration for a key as a UNIX timestamp.")
(define-command pexpire :boolean (key :key) (milliseconds :integer) "Set a key's time to live in milliseconds.")
(define-command pexpireat :boolean (key :key) (milliseconds-timestamp :integer) "Set the expiration for a key as a UNIX timestamp specified in milliseconds.")
(define-command persist :boolean (key :key) "Remove the expiration from a key.")
(define-command ttl (key :key) "Get the number of seconds from now until expiry.")
(define-command pttl (key :key) "Get the time to live for a key in milliseconds.")
(define-command select (index :integer) "Select the database having the specified index.")
(define-command move :boolean (key :key) (dbindex :integer) "Move the key from the currently selected database to a database specified by index.")
(define-command flushdb "Remove all the keys of the currently selected database.")
(define-command flushall "Remove all the keys from all the databases.")
(define-command dump (key :key) "Serialize the value stored at key.")
(define-command restore (key :key) (ttl :integer) (value :bulk) "Create a key associated with the deserialized value.")

;; Commands operating on string values (incl. pseudo-integers)
;; TODO: bitop commands, incrbyfloat

(define-command set (key :key) (value :bulk) "Set a key to a string value.")
(define-command get (key :key) "Return the string value of the key.")
(define-command getset (key :key) (value :bulk) "Set a key to a string returning the old value of the key.")
(define-command mget (list keys :key) "Multi-get, return the string values of the keys.")
(define-command setnx :boolean (key :key) (value :bulk) "Set a key to a string value if the key does not exist.")
(define-command setex (key :key) (time :integer) (value :bulk) "Set+Expire combo command")
(define-command mset (list keys/vals :key :bulk) "Set multiple keys to multiple values in a single atomic operation.")
(define-command msetnx (list keys/vals :key :bulk) "Set multiple keys to multiple values in a single atomic operation if none of the keys already exist.")
(define-command incr (key :key) "Increment the integer value of key.")
(define-command incrby (key :key) (integer :integer) "Increment the integer value of key by integer.")
(define-command decr (key :key) "Decrement the integer value of key.")
(define-command decrby (key :key) (integer :integer) "Decrement the integer value of key by integer.")
(define-command append (key :key) (value :bulk) "Append the specified string to the string stored at key.")
(define-command substr (key :key) (start :integer) (end :integer) "Return a substring out of a larger string.")
(define-command bitcount (key :key) (start :integer) (end :integer) "Count the number of set bits in a string.")
(define-command getbit (key :key) (offset :integer) "Returns the bit value at offset in the string value stored at key.")
(define-command setbit (key :key) (offset :integer) (value :integer) "Sets or clears the bit at offset in the string value stored at key.")
(define-command getrange (key :key) (start :integer) (end :integer) "Get a substring of the string stored at a key (like SUBSTR).")
(define-command setrange (key :key) (offset :integer) (value :bulk) "Overwrite part of a string at key starting at the specified offset.")
(define-command strlen (key :key) "Get the length of the value stored in key.")
(define-command psetex (key :key) (milliseconds :integer) (value :bulk) "Set the value and expiration in milliseconds of a key.")

;; Commands operating on lists
;; TODO: linsert

(define-command rpush (key :key) (value :bulk) "Append an element to the tail of the list value at key.")
(define-command rpushx (key :key) (value :bulk) "Append a value to a list, only if the list exists.")
(define-command lpush (key :key) (value :bulk) "Append an element to the head of the list value at key.")
(define-command lpushx (key :key) (value :bulk) "Prepend a value to a list, only if the list exists.")
(define-command llen (key :key) "Return the length of the list value at key.")
(define-command lrange (key :key) (start :integer) (end :integer) "Return a range of elements from the list at key.")
(define-command ltrim (key :key) (start :integer) (end :integer) "Trim the list at key to the specified range of elements.")
(define-command lindex (key :key) (index :integer) "Return the element at index position from the list at key.")
(define-command lset (key :key) (index :integer) (value :bulk) "Set a new value as the element at index position of the list at key.")
(define-command lrem (key :key) (count :integer) (value :bulk) "Remove the first-N, last-N, or all the elements matching value from the list at key.")
(define-command lpop (key :key) "Return and remove (atomically) the first element of the list at key.")
(define-command rpop (key :key) "Return and remove (atomically) the last element of the list at key.")
(define-command blpop (list keys :key) (timeout :integer) "Blocking LPOP")
(define-command brpop (list keys :key) (timeout :integer) "Blocking RPOP")
(define-command brpoplpush (source :key) (destination :key) (timeout :integer) "Pop a value from a list, push it to another list and return it; or block until one is available.")
(define-command rpoplpush (srckey :key) (dstkey :key)
  "Return and remove (atomically) the last element of the source list
stored at srckey and push the same element to the destination list
stored at dstkey")

;; Commands operating on sets

(define-command sadd :boolean (key :key) (member :bulk) "Add the specified member to the set value at key.")
(define-command srem :boolean (key :key) (member :bulk) "Remove the specified member from the set value at key.")
(define-command spop (key :key) "Remove and return (pop) a random element from the set value at key.")
(define-command smove :boolean (srckey :key) (dstkey :key) "Move the specified member from one set to another atomically.")
(define-command scard (key :key) "Return the number of elements (the cardinality) of the set at key.")
(define-command sismember :boolean (key :key) (member :key) "Test if the specified value is a member of the set at key.")
(define-command sinter (list keys :key) "Return the intersection of the sets stored at keys.")
(define-command sinterstore (dstkey :key) (list keys :key) "Compute the intersection of the sets stored at keys and store the resulting set at dstkey.")
(define-command sunion (list keys :key) "Return the union of the sets stored at keys.")
(define-command sunionstore (dstkey :key) (list keys :key) "Compute the union of the sets stored at keys and store the resulting set at dstkey.")
(define-command sdiff (list keys :key) "Return the difference between the set stored at the first key and the sets stored at the rest of the keys.")
(define-command sdiffstore (dstkey :key) (list keys :key)
  "Compute the difference between the set stored at the first key and
the sets stored at the rest of the keys, and store it at dstkey.")
(define-command smembers (key :key) "Return all the members of the set value at key.")
(define-command srandmember (key :key) "Return a random member of the set value at key.")

;; Commands operating on sorted sets (zsets)
;; TODO: withscores stuff

(define-command zadd (key :key) (score :integer) (member :bulk)
  "Add the specified member to the sorted set value at key or update
the score if it already exists.")
(define-command zrem (key :key) (member :bulk) "Remove the specified member from the sorted set value at key.")
(define-command zincrby (key :key) (increment :integer) (member :bulk)
  "If the member already exists, increment its score by increment,
otherwise add the member setting increment as score.")
(define-command zrank (key :key) (member :bulk)
  "Return the rank (or index) of member in the sorted set at key, with
scores being oredered from low to high.")
(define-command zrevrank (key :key) (member :bulk)
  "Return the rank (or index) of member in the sorted set at key, with
scores being ordered from high to low.")
(define-command zrange (key :key) (start :integer) (end :integer)
  "Return a range of elements from the sorted set at key, ordered by
smallest to greatest score.")
(define-command zrevrange (key :key) (start :integer) (end :integer)
  "Return a range of elements from the sorted set at key, ordered by
greatest to the smallest score.")
(define-command zrangebyscore (key :key) (min :integer) (max :integer)
  "Return all the elements with min <= score <= max (a range query)
from the sorted set.")
(define-command zcard (key :key) "Return the number of elements (cardinality) of the sorted set at key.")
(define-command zcount (key :key) (min :integer) (max :integer) "Count the members in a sorted set with scores within the given values.")
(define-command zscore (key :key) (element :bulk) "Return the score associated with the specified element of the sorted set at key.")
(define-command zremrangebyrank (key :key) (min :integer) (max :integer)
  "Remove all the elements with min <= rank <= max rank from the sorted set.")
(define-command zremrangebyscore (key :key) (min :integer) (max :integer)
  "Remove all the elements with min <= score <= max score from the sorted set.")

(macrolet ((frob (name docstring)
             `(defun ,name (dstkey n keys &key (connection *connection*) (octets nil) weights aggregate)
                ,docstring
                (let ((sequences '())
                      (nsequences 0))
                  (flet ((add-sequence (sequence)
                           (push sequence sequences)
                           (incf nsequences)))
                    (add-sequence ,(princ-to-string name))
                    (add-sequence (key-sequence dstkey))
                    (add-sequence (princ-to-string n))
                    (dolist (key keys)
                      (add-sequence (key-sequence key)))
                    (when weights
                      (add-sequence "WEIGHTS")
                      (dolist (weight weights)
                        (add-sequence (princ-to-string weight))))
                    (when aggregate
                      (add-sequence "AGGREGATE")
                      (add-sequence
                       (ecase aggregate
                         (:sum "SUM")
                         (:min "MIN")
                         (:max "MAX")))))
                  (write-sequence
                   (with-output-to-sequence (out)
                     (write-multi-bulk (nreverse sequences) nsequences out))
                   (connection-stream connection))
                  (force-output (connection-stream connection))
                  (translate-result (read-reply connection) octets nil)))))
  (frob zunionstore "Union over a number of sorted sets with optional weight and aggregate.")
  (frob zinterstore "Intersect over a number of sorted sets with optional weight and aggregate."))

;; Commands operating on hashes
;; TODO: hincrbyfloat

;; Some commands here are bulk commands rather than inline.  Why?

(define-command hset (key :key) (field :string) (value :bulk) "Set the hash field to the specified value.  Creates the hash if needed.")
(define-command hsetnx (key :key) (field :string) (value :bulk) "Set the hash field to the specified value, if the field does not exist.  Creates the hash if needed.")
(define-command hget (key :key) (field :bulk) "Retrieve the value of the specified hash field.")
(define-command hmset (key :key) (list fields/vals :string :bulk) "Set the hash fields to their respective values.")
(define-command hincrby (key :key) (field :string) (integer :integer) "Increment the integer value of the hash at key on field with integer.")
(define-command hexists :boolean (key :key) (field :bulk) "Test for existence of a specified field in a hash.")
(define-command hdel (key :key) (field :bulk) "Remove the specified field from a hash.")
(define-command hlen (key :key) "Return the number of items in a hash.")
(define-command hkeys (key :key) "Return all the fields in a hash.")
(define-command hvals (key :key) "Return all the values in a hash.")
(define-command hgetall (key :key) "Return all the fields and associated values in a hash.")
(define-command hmget (key :key) (list fields :string) "Returns the values associated with the specified fields in the hash stored at key.")

;; Sorting

(defun sort (key &key (connection *connection*) (octets nil) order (limit-start 0) limit-end by get alpha)
  "Sort a set or a list according to the specified parameters."
  (let ((sequences '())
        (nsequences 0))
    (flet ((add-sequence (sequence)
             (push sequence sequences)
             (incf nsequences)))
      (add-sequence "SORT")
      (add-sequence (key-sequence key))
      (when by
        (add-sequence "BY")
        (add-sequence (key-sequence by)))
      (when (and limit-start limit-end)
        (add-sequence "LIMIT")
        (add-sequence (princ-to-string limit-start))
        (add-sequence (princ-to-string limit-end)))
      (when get
        (when (stringp get)
          (setf get (list get)))
        (dolist (x get)
          (add-sequence "GET")
          (add-sequence (key-sequence x))))
      (ecase order
        ((:asc :ascending) (add-sequence "ASC"))
        ((:desc :descending) (add-sequence "DESC"))
        ((nil)))
      (when alpha
        (add-sequence "ALPHA")))
    (write-sequence
     (with-output-to-sequence (out)
       (write-multi-bulk (nreverse sequences) nsequences out))
     (connection-stream connection))
    (force-output (connection-stream connection))
    (translate-result (read-reply connection) octets nil)))

;; Transactions

(define-command multi "Begin a transaction")
(define-command exec "Commit transaction")
(define-command discard "Rollback transaction")
(define-command watch (list keys :key) "Watch the given keys to determine execution of the MULTI/EXEC block.")
(define-command unwatch "Forget about all watched keys.")

;; Publish/Subscribe
;; TODO: pubsub

(define-command %subscribe :no-read "SUBSCRIBE" (list channels :key) "Subscribe the client to the specified channels.")
(define-command %unsubscribe :no-read "UNSUBSCRIBE" (list channels :key)
  "Unsubscribe the client from the given channels, or from all of them if none is given.")
(define-command %psubscribe :no-read "PSUBSCRIBE" (list patterns :key) "Subscribe the client to the given patterns.")
(define-command %punsubscribe :no-read "PUNSUBSCRIBE" (list patterns :key) "Unsubscribe the client from the given patterns.")
(define-command publish (channel :key) (message :bulk) "Post a message to a given channel.")

;; We assume channel names are always represented as strings.

(defclass subscription ()
  ((function :initarg :function :reader subscription-function)
   (want-octets :initarg :want-octets :reader subscription-want-octets)))

(defun subscribe (function &key (connection *connection*)
                                (want-octets nil)
                                (channels '())
                                (patterns '()))
  "Subscribe the client to the specified channels or patterns, calling
the function when a relevant event is available.  The function should
take two arguments: the originating channel and the actual message."
  (let ((subscription (make-instance 'subscription
                                     :function function
                                     :want-octets want-octets)))
    (dolist (channel channels)
      (setf (gethash channel (connection-channel-subscriptions connection)) subscription))
    (when channels
      (%subscribe channels :connection connection))
    (dolist (pattern patterns)
      (setf (gethash pattern (connection-pattern-subscriptions connection)) subscription))
    (when patterns
      (%psubscribe patterns :connection connection))))

(defun unsubscribe (&key (channels '())
                         (patterns '())
                         (connection *connection*))
  "Unsubscribe the client from the specified channels or patterns.  In
order to unsubscribe from all channels or patterns, pass :ALL as an
argument."
  (when channels
    (let ((subscriptions (connection-channel-subscriptions connection)))
      (if (eq channels :all)
          (clrhash subscriptions)
          (dolist (channel channels)
            (remhash channel subscriptions))))
    (%unsubscribe (if (eq channels :all) '() channels) :connection *connection*))
  (when patterns
    (let ((subscriptions (connection-pattern-subscriptions connection)))
      (if (eq patterns :all)
          (clrhash subscriptions)
          (dolist (pattern patterns)
            (remhash pattern subscriptions))))
    (%punsubscribe (if (eq patterns :all) '() patterns) :connection connection)))

(define-condition pubsub-bad-event (redis-error)
  ((event :initarg :event :reader pubsub-bad-event-event))
  (:default-initargs :text "Bad publish/subscribe event."))

(defun translate-event (event)
  (let ((kind (first event))
        (key (octets-to-string (second event))))
    (cond ((equalp kind #(115 117 98 115 99 114 105 98 101))
           (values :subscribe key (third event)))
          ((equalp kind #(117 110 115 117 98 115 99 114 105 98 101))
           (values :unsubscribe key (third event)))
          ((equalp kind #(109 101 115 115 97 103 101))
           (values :message key (third event)))
          ((equalp kind #(112 115 117 98 115 99 114 105 98 101))
           (values :subscribe key (third event) key))
          ((equalp kind #(112 117 110 115 117 98 115 99 114 105 98 101))
           (values :unsubscribe key (third event) key))
          ((equalp kind #(112 109 101 115 115 97 103 101))
           (values :message key (fourth event) (octets-to-string (third event))))
          (t (error 'pubsub-bad-event :event event)))))

(define-condition pubsub-spam (pubsub-bad-event)
  ()
  (:default-initargs :text "Publish/subscribe spam event."))

(defun pubsub-dispatch (&key (connection *connection*))
  "Wait for a publisher/subscriber event from Redis and dispatch
appropriately.  There's no notification about subscriptions or
unsubscriptions, but we return true if any subscriptions are still
active and false otherwise."
  (let ((event (read-reply connection)))
    (multiple-value-bind (kind key payload channel) (translate-event event)
      (if (eq kind :unsubscribe)
          (plusp payload)
          (let* ((subscriptions (if channel
                                    (connection-pattern-subscriptions connection)
                                    (connection-channel-subscriptions connection)))
                 (subscription (gethash key subscriptions)))
            (when (null subscription)
              (error 'pubsub-spam :event event))
            (when (eq kind :message)
              (funcall (subscription-function subscription)
                       (or channel key)
                       (if (subscription-want-octets subscription)
                           payload
                           (octets-to-string payload))))
            t)))))

;; Persistence control commands
;; TODO: new shutdown

(define-command save "Synchronously save the database on disk.")
(define-command bgsave "Asynchronously save the database on disk.")
(define-command lastsave "Return the UNIX timestamp of the last successful save.")
(define-command shutdown :no-read "Synchronously save the database on disk, then shutdown the server.")
(define-command bgrewriteaof "Rewrite the append only file in the background when it gets too big.")

;; Remote server control commands
;; TODO: new config, debug, eval, evalsha, new info, migrate, monitor, object, script, slowlog

(define-command info "Provide information and statistics about the server")
(define-command slaveof (host :string) (port :integer) "Change the replication settings.")
(define-command config "Configure a redis server at runtime.")
(define-command echo (message :string) "Echo the given string.")
(define-command ping "Ping the redis server.")
(define-command time "Return the current server time.")
