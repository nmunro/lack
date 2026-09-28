(defpackage lack/middleware/session/store/redis
  (:nicknames :lack.middleware.session.store.redis
              :lack.session.store.redis
              :lack/session/store/redis)
  (:use :cl
        :lack/middleware/session/store)
  (:import-from :marshal
                :marshal
                :unmarshal)
  (:import-from :cl-base64
                :base64-string-to-usb8-array
                :usb8-array-to-base64-string)
  (:import-from :trivial-utf-8
                :string-to-utf-8-bytes
                :utf-8-bytes-to-string)
  (:export :redis-store
           :make-redis-store
           :redis-connection
           :disconnect-store
           :fetch-session
           :store-session
           :remove-session))
(in-package :lack/middleware/session/store/redis)

(defun open-connection (&key host port auth)
  (make-instance 'redis:redis-connection
                 :host host
                 :port port
                 :auth auth))

(defstruct (redis-store (:include store)
                        (:constructor %make-redis-store))
  (host "127.0.0.1")
  (port 6379)
  (auth nil :type (or null string))
  (namespace "session" :type string)
  (expires nil :type (or null integer))
  (serializer (lambda (data)
                (usb8-array-to-base64-string
                 (string-to-utf-8-bytes (prin1-to-string (marshal data))))))
  (deserializer (lambda (data)
                  (unmarshal (safe-read-from-string
                              (utf-8-bytes-to-string (base64-string-to-usb8-array data))))))
  (lock (bordeaux-threads-2:make-lock :name "redis session store lock"))
  connection)

(defun make-redis-store (&rest args &key (host "127.0.0.1") (port 6379) auth connection namespace expires serializer deserializer lock)
  (declare (ignore host port auth namespace expires serializer deserializer lock))
  (when connection
    (setf (getf args :host) (redis::conn-host connection)
          (getf args :port) (redis::conn-port connection)
          (getf args :auth) (redis::conn-auth connection)))
  (apply #'%make-redis-store args))

(defun disconnect-store (store)
  "Closes and resets the connection on STORE if present."
  (check-type store redis-store)
  (with-slots (connection) store
    (when connection
      (ignore-errors (redis:close-connection connection))
      (setf connection nil))))

(defun redis-connection (store)
  (check-type store redis-store)
  (with-slots (host port auth connection) store
    (unless (and connection
                 (ignore-errors (redis::connection-open-p connection)))
      (disconnect-store store)
      (setf connection
            (open-connection :host host :port port :auth auth)))
    connection))

(defmacro with-connection (store &body body)
  (let ((s (gensym "STORE")))
    `(let ((,s ,store))
       (bordeaux-threads-2:with-lock-held ((redis-store-lock ,s))
         (flet ((run ()
                  (let ((redis::*connection* (redis-connection ,s)))
                    ,@body)))
           (handler-case (run)
             (error ()
               (disconnect-store ,s)
               (run))))))))

(defmethod fetch-session ((store redis-store) sid)
  (let ((data (with-connection store
                (red:get (format nil "~A:~A"
                                 (redis-store-namespace store)
                                 sid)))))
    (if data
        (handler-case (funcall (redis-store-deserializer store) data)
          (error (e)
            (warn "Error (~A) occured while deserializing a session. Ignoring.~2%    Data:~%        ~A~2%    Error:~%        ~A"
                  (class-name (class-of e))
                  (let ((s (or data "")))
                    (if (> (length s) 200)
                        (concatenate 'string (subseq s 0 200) "... [truncated]")
                        s))
                  e)
            nil))
        nil)))

(defmethod store-session ((store redis-store) sid session)
  (let ((data (funcall (redis-store-serializer store) session))
        (key  (format nil "~A:~A" (redis-store-namespace store) sid)))
    (with-connection store
      (red:set key data)
      (when (redis-store-expires store)
        (red:expire key (redis-store-expires store))))))

(defmethod remove-session ((store redis-store) sid)
  (with-connection store
    (red:del (format nil "~A:~A"
                     (redis-store-namespace store)
                     sid))))
