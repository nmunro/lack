(defpackage lack/tests/session/store/redis
  (:use :cl
        :lack
        :lack/test
        :lack/session/store/redis
        :rove)
  (:import-from :lack/session/store/redis
                :redis-connection
                :disconnect-store))
(in-package :lack/tests/session/store/redis)

(defvar *namespace* "session_test")
(defvar *connection*)

(setup
  (setf *connection* (redis-connection (make-redis-store)))

  (let ((redis::*connection* *connection*))
    (let ((keys (red:keys (format nil "~A:*" *namespace*))))
      (when keys
        (apply #'red:del keys)))))

(deftest session-middleware
  (let ((app
          (builder
           (:session
            :store (make-redis-store :namespace *namespace* :connection *connection*))
           (lambda (env)
             (unless (gethash :counter (getf env :lack.session))
               (setf (gethash :counter (getf env :lack.session)) 0))
             `(200
               (:content-type "text/plain")
               (,(format nil "Hello, you've been here for ~Ath times!"
                         (incf (gethash :counter (getf env :lack.session)))))))))
        session)
    (diag "1st request")
    (destructuring-bind (status headers body)
        (funcall app (generate-env "/"))
      (ok (eql status 200))
      (setf session (parse-lack-session headers))
      (ok session)
      (ok (equalp body '("Hello, you've been here for 1th times!"))))

    (diag "2nd request")
    (destructuring-bind (status headers body)
        (funcall app (generate-env "/" :cookies `(("lack.session" . ,session))))
      (declare (ignore headers))
      (ok (eql status 200))
      (ok (equalp body '("Hello, you've been here for 2th times!")))))

  (testing "utf-8 session data"
    (let ((app
            (builder
             (:session
              :store (make-redis-store :namespace *namespace* :connection *connection*))
             (lambda (env)
               (unless (gethash :user (getf env :lack.session))
                 (setf (gethash :user (getf env :lack.session)) "深町英太郎"))
               (unless (gethash :counter (getf env :lack.session))
                 (setf (gethash :counter (getf env :lack.session)) 0))
               `(200
                 (:content-type "text/plain")
                 (,(format nil "Hello, ~A! You've been here for ~Ath times!"
                           (gethash :user (getf env :lack.session))
                           (incf (gethash :counter (getf env :lack.session)))))))))
          session)
      (destructuring-bind (status headers body)
          (funcall app (generate-env "/"))
        (ok (eql status 200))
        (setf session (parse-lack-session headers))
        (ok session)
        (ok (equalp body '("Hello, 深町英太郎! You've been here for 1th times!"))))

      (destructuring-bind (status headers body)
          (funcall app (generate-env "/" :cookies `(("lack.session" . ,session))))
        (declare (ignore headers))
        (ok (eql status 200))
        (ok (equalp body '("Hello, 深町英太郎! You've been here for 2th times!"))))))

  (testing "expires"
    (let ((app
            (builder
             (:session
              :store (make-redis-store :namespace *namespace* :connection *connection*
                                       :expires 3))
             (lambda (env)
               (unless (gethash :user (getf env :lack.session))
                 (setf (gethash :user (getf env :lack.session)) "深町英太郎"))
               (unless (gethash :counter (getf env :lack.session))
                 (setf (gethash :counter (getf env :lack.session)) 0))
               `(200
                 (:content-type "text/plain")
                 (,(format nil "Hello, ~A! You've been here for ~Ath times!"
                           (gethash :user (getf env :lack.session))
                           (incf (gethash :counter (getf env :lack.session)))))))))
          session)

      (destructuring-bind (status headers body)
          (funcall app (generate-env "/"))
        (ok (eql status 200))
        (setf session (parse-lack-session headers))
        (ok session)
        (ok (equalp body '("Hello, 深町英太郎! You've been here for 1th times!"))))

      (let ((body (nth 2 (funcall app (generate-env "/" :cookies `(("lack.session" . ,session)))))))
        (ok (equalp body '("Hello, 深町英太郎! You've been here for 2th times!"))))

      (sleep 2)

      (let ((body (nth 2 (funcall app (generate-env "/" :cookies `(("lack.session" . ,session)))))))
        (ok (equalp body '("Hello, 深町英太郎! You've been here for 3th times!"))
            "Still the session is alive"))

      (sleep 2)

      (let ((body (nth 2 (funcall app (generate-env "/" :cookies `(("lack.session" . ,session)))))))
        (ok (equalp body '("Hello, 深町英太郎! You've been here for 4th times!"))
            "Reset the expiration when accessed"))

      (sleep 3.5)

      (let ((body (nth 2 (funcall app (generate-env "/" :cookies `(("lack.session" . ,session)))))))
        (ok (equalp body '("Hello, 深町英太郎! You've been here for 1th times!"))
            "Session has expired after 3 seconds since the last access"))))

  (let ((redis::*connection* *connection*))
    (ok (eql (length (red:keys (format nil "~A:*" *namespace*)))
             3)
        "'session' has three records")))

(deftest lazy-and-reconnect
  (testing "lazy connection"
    (let ((store (make-redis-store :port 6389)))
      (ok (typep store 'redis-store) "make-redis-store returns a redis-store without connecting")
      (ok (signals (redis-connection store) 'error) "connection is attempted lazily on access")))

  (testing "reconnect on closed connection"
    (let ((store (make-redis-store :namespace *namespace*)))
      (let ((conn1 (redis-connection store)))
        (ok (redis::connection-open-p conn1) "initial connection is open")
        (redis:close-connection conn1)
        (ok (null (redis::connection-open-p conn1)) "connection is closed")
        (let ((conn2 (redis-connection store)))
          (ok (redis::connection-open-p conn2) "re-opens connection when accessed")
          (redis:close-connection conn2)))))

  (testing "auto-reconnect on dropped connection during store/fetch"
    (let ((store (make-redis-store :namespace *namespace*)))
      (store-session store "drop-key" '(("status" . "saved")))
      (disconnect-store store)
      ;; fetch-session should transparently reconnect and fetch the session
      (let ((val (fetch-session store "drop-key")))
        (ok (equal val '(("status" . "saved"))) "transparently reconnects and fetches session"))))

  (testing "thread-safe concurrent access"
    (let ((store (make-redis-store :namespace *namespace*))
          (errors nil)
          (threads nil)
          (err-lock (bordeaux-threads-2:make-lock :name "test-err-lock")))
      (dotimes (i 6)
        (let ((idx i))
          (push (bordeaux-threads-2:make-thread
                 (lambda ()
                   (handler-case
                       (dotimes (j 10)
                         (let ((sid (format nil "thread-~D-session-~D" idx j))
                               (data `(("user" . ,(format nil "user-~D" idx))
                                       ("counter" . ,j))))
                           (store-session store sid data)
                           (let ((fetched (fetch-session store sid)))
                             (unless (equal fetched data)
                               (error "Fetched data mismatch: expected ~S got ~S" data fetched)))))
                     (error (e)
                       (bordeaux-threads-2:with-lock-held (err-lock)
                         (push (cons idx e) errors)))))
                 :name (format nil "test-worker-~D" i))
                threads)))
      (dolist (th threads)
        (bordeaux-threads-2:join-thread th))
      (ok (null errors) "concurrent access across threads completed without errors"))))

(teardown
  (redis:close-connection *connection*))
