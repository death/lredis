;;;; +----------------------------------------------------------------+
;;;; | LREDIS - Lisp Redis bindings                       DEATH, 2009 |
;;;; +----------------------------------------------------------------+

;;;; System definition

;;; -*- Mode: LISP; Syntax: COMMON-LISP; Package: CL-USER; Base: 10 -*-

(asdf:defsystem #:lredis
  :description "Lisp Redis bindings"
  :author "death <github.com/death>"
  :license "BSD"
  :depends-on (#:babel #:babel-streams #:usocket)
  :components ((:file "lredis")))
