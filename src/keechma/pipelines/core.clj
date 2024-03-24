(ns keechma.pipelines.core
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.core.specs.alpha]))

(defn rescue? [val]
  (= "rescue!" (name val)))

(defn finally? [val]
  (= "finally!" (name val)))

(s/def ::pipeline-body
  (s/and list?
    (s/or
      :begin-rescue-finally (s/cat :begin (s/* any?) :rescue ::rescue :finally ::finally)
      :begin-finally (s/cat :begin (s/* any?) :finally ::finally)
      :begin-rescue (s/cat :begin (s/* any?) :rescue ::rescue)
      :begin (s/cat :begin (s/* any?)))))

(s/def ::pipeline-args
  (s/coll-of :clojure.core.specs.alpha/binding-form :kind vector? :count 2))

(s/def ::rescue-finally-args
  (s/coll-of :clojure.core.specs.alpha/binding-form :kind vector? :count 1))

(s/def ::rescue
  (s/and list?
    (s/cat :rescue rescue?
      :args ::rescue-finally-args
      :body (s/* any?))))

(s/def ::finally
  (s/and list?
    (s/cat :finally finally?
      :args ::rescue-finally-args
      :body (s/* any?))))

(defn expand-body [args body]
  (->> (map
         (fn [f]
           `(fn ~args ~f))
         body)
    (into [])))

(defn make-pipeline [_ _])

(defn begin-forms [acc args {:keys [begin]}]
  (if (seq begin)
    (assoc acc :begin (expand-body (conj args '_) begin))
    acc))

(defn rescue-finally-forms [acc block args body]
  (let [block-body (get body block)]
    (if (seq block-body)
      (let [block-args (s/unform ::rescue-finally-args (:args block-body))]
        (assoc acc block (expand-body (vec (concat args block-args)) (:body block-body))))
      acc)))

;; TODO: Throw error if rescue! or finally! block is the in wrong position
(defn prepare-pipeline [args body]
  (s/assert ::pipeline-args args)
  (s/assert ::pipeline-body body)
  (let [[_ conformed-body] (s/conform ::pipeline-body body)
        id (keyword (gensym ::pipeline))]
    `(keechma.pipelines.core/make-pipeline
       ~id
       ~(-> {}
          (begin-forms args conformed-body)
          (rescue-finally-forms :rescue args conformed-body)
          (rescue-finally-forms :finally args conformed-body)))))

(defmacro pipeline! [args & body]
  (prepare-pipeline args (or body `())))