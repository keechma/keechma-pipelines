(ns keechma.pipelines.core
  (:refer-clojure :exclude [swap! reset!])
  (:require [keechma.pipelines.runtime :as runtime])
  (:require-macros [keechma.pipelines.core :refer [pipeline!]]))

(def make-pipeline runtime/make-pipeline)
(def start! runtime/start!)
(def stop! runtime/stop!)
(def has-pipeline? runtime/has-pipeline?)
(def invoke runtime/invoke)
(def in-pipeline? runtime/in-pipeline?)

(defn set-queue
  "Explicitly set the queue name. Second argument can be a function in which case, it will be called with the pipeline arguments and should return a queue name"
  [pipeline queue]
  (assoc-in pipeline [:config :queue-name] queue))

(defn use-existing
  "If there is an in flight pipeline started with the same arguments, return its promise instead of starting a new one. It can be combined with any other concurrency behavior (`restartable`, `dropping`, `enqueued` and `keep-latest`)"
  [pipeline]
  (assoc-in pipeline [:config :use-existing] true))

(defn restartable
  "Cancel any running pipelines and start a new one"
  ([pipeline] (restartable pipeline 1))
  ([pipeline max-concurrency]
   (assoc-in pipeline [:config :concurrency] {:behavior :restartable :max max-concurrency})))

(defn enqueued
  "Enqueue requests and execute them sequentially"
  ([pipeline] (enqueued pipeline 1))
  ([pipeline max-concurrency]
   (assoc-in pipeline [:config :concurrency] {:behavior :enqueued :max max-concurrency})))

(defn dropping
  "Drop new request while another one is running"
  ([pipeline] (dropping pipeline 1))
  ([pipeline max-concurrency]
   (assoc-in pipeline [:config :concurrency] {:behavior :dropping :max max-concurrency})))

(defn keep-latest
  "Drop all intermediate requests, enqueue the last one"
  ([pipeline] (keep-latest pipeline 1))
  ([pipeline max-concurrency]
   (assoc-in pipeline [:config :concurrency] {:behavior :keep-latest :max max-concurrency})))

(defn cancel-on-shutdown
  "Should the pipeline be cancelled when the runtime is shut down"
  ([pipeline] (cancel-on-shutdown pipeline true))
  ([pipeline should-cancel]
   (assoc-in pipeline [:config :cancel-on-shutdown] should-cancel)))

(defn detached
  ([pipeline] (detached pipeline true))
  ([pipeline is-detached]
   (assoc-in pipeline [:config :is-detached] is-detached)))

(defn muted [pipeline]
  (pipeline! [value _]
    (let [value' value]
      (pipeline! [_ _]
        pipeline
        value'))))

(defn swap!
  "swap! that can be used inside pipeline! - returns nil so it doesn't change the current value"
  [& args]
  (apply clojure.core/swap! args)
  nil)

(defn reset!
  "reset! that can be used inside pipeline! - returns nil so it doesn't change the current value"
  [& args]
  (apply clojure.core/reset! args)
  nil)