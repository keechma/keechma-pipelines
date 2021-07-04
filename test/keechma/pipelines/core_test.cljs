(ns keechma.pipelines.core-test
  (:require [cljs.test :refer-macros [deftest testing is async use-fixtures]]
            [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
            [keechma.pipelines.runtime :as runtime]
            [promesa.core :as p]
            [cljs.core.async :refer [<! >! chan close! put! alts! timeout]])
  (:require-macros [cljs.core.async.macros :refer [go alt!]]))

#_(use-fixtures :once
    {:before (fn [] (js/console.clear))})

(defn delay-pipeline
  ([] (delay-pipeline 10))
  ([msec] (p/delay msec)))

(defn fn-returning-value []
  {:baz :qux})

(defn fn-returning-nil []
  nil)

(defn is-returning-nil [check]
  (is check)
  nil)

(defn fn-throwing []
  (throw (ex-info "ERROR" {:ex :info})))

(def promise-error (js/Error. "Promise Rejected"))

(defn fn-promise-rejecting []
  (p/create (fn [_ reject]
              (reject promise-error))))

(defn throwing-fn [msg]
  (throw (ex-info msg {})))

(defn make-context
  ([] (make-context nil))
  ([initial-state]
   {:log* (atom [])
    :state* (atom initial-state)}))

(deftest basic-pipeline-1 []
  (let [{:keys [state*] :as context} (make-context)
        p       (pipeline! [value {:keys [state*]}]
                  (pp/reset! state* value))
        runtime (start! context {:p p})]
    (is (= 1) (invoke runtime :p 1))
    (is (= 1 @state*))))

(deftest basic-pipeline-2 []
  (let [{:keys [state*] :as context} (make-context)
        p       (pipeline! [value {:keys [state*]}]
                  (pipeline! [value {:keys [state*]}]
                    (pp/reset! state* value)))
        runtime (start! context {:p p})]
    (is (= 1) (invoke runtime :p 1))
    (is (= 1 @state*))))

(deftest basic-pipeline-3 []
  (let [{:keys [state*] :as context} (make-context)
        p       (pipeline! [value {:keys [state*]}]
                  (p/delay 10)
                  (pipeline! [value {:keys [state*] :as ctx}]
                    (p/delay 10)
                    (pp/reset! state* value)))
        runtime (start! context {:p p})]
    (async done
           (->> (invoke runtime :p 1)
                (p/map (fn [res]
                         (is (= 1 res))
                         (is (= 1 @state*))
                         (done)))
                (p/error (fn [err]
                           (done)))))))

(deftest basic-restartable-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (p/delay 250)
                                (pp/swap! state* conj value))
                              (pp/restartable))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query "S")
             (<! (timeout 20))
             (invoke runtime :query "SE")
             (<! (timeout 20))
             (invoke runtime :query "SEA")
             (<! (timeout 20))
             (invoke runtime :query "SEAR")
             (invoke runtime :query "SEARC")
             (<! (timeout 20))
             (->> (invoke runtime :query "SEARCH")
                  (p/map (fn [res]
                           (is (= ["SEARCH"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest multiconcurrency-restartable-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (p/delay 250)
                                (pp/swap! state* #(vec (conj % value))))
                              (pp/restartable 3))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 10))
             (invoke runtime :query 2)
             (<! (timeout 10))
             (invoke runtime :query 3)
             (<! (timeout 10))
             (invoke runtime :query 4)
             (invoke runtime :query 5)
             (<! (timeout 10))
             (->> (invoke runtime :query 6)
                  (p/map (fn [_]
                           (is (= [4 5 6] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest basic-dropping-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (p/delay 250)
                                (pp/swap! state* #(vec (conj % value))))
                              (pp/dropping))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 20))
             (invoke runtime :query 2)
             (<! (timeout 20))
             (invoke runtime :query 3)
             (<! (timeout 20))
             (invoke runtime :query 4)
             (invoke runtime :query 5)
             (<! (timeout 20))
             (invoke runtime :query 6)
             (<! (timeout 500))
             (is (= [1] @state*))
             (done)))))

(deftest multiconcurrency-dropping-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (p/delay 250)
                                (pp/swap! state* #(vec (conj % value))))
                              (pp/dropping 3))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 20))
             (invoke runtime :query 2)
             (<! (timeout 20))
             (invoke runtime :query 3)
             (<! (timeout 20))
             (invoke runtime :query 4)
             (invoke runtime :query 5)
             (<! (timeout 20))
             (invoke runtime :query 6)
             (<! (timeout 500))
             (is (= [1 2 3] @state*))
             (done)))))

(deftest basic-enqueued-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (pp/swap! state* #(vec (conj % value)))
                                (p/delay 50)
                                (pp/swap! state* #(vec (conj % (str "DONE-" value)))))
                              (pp/enqueued))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query "FIRST")
             (<! (timeout 20))
             (invoke runtime :query "SECOND")
             (<! (timeout 20))
             (invoke runtime :query "THIRD")
             (<! (timeout 20))
             (invoke runtime :query "FOURTH")
             (invoke runtime :query "FIFTH")
             (<! (timeout 20))
             (->> (invoke runtime :query "SIXTH")
                  (p/map (fn [_]
                           (is (= ["FIRST" "DONE-FIRST"
                                   "SECOND" "DONE-SECOND"
                                   "THIRD" "DONE-THIRD"
                                   "FOURTH" "DONE-FOURTH"
                                   "FIFTH" "DONE-FIFTH"
                                   "SIXTH" "DONE-SIXTH"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest multiconcurrency-enqueued-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (pp/swap! state* #(vec (conj % value)))
                                (p/delay 50)
                                (pp/swap! state* #(vec (conj % (str "DONE-" value)))))
                              (pp/enqueued 3))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query "FIRST")
             (<! (timeout 10))
             (invoke runtime :query "SECOND")
             (<! (timeout 10))
             (invoke runtime :query "THIRD")
             (<! (timeout 10))
             (invoke runtime :query "FOURTH")
             (invoke runtime :query "FIFTH")
             (<! (timeout 10))
             (->> (invoke runtime :query "SIXTH")
                  (p/map (fn [_]
                           (is (= ["FIRST"
                                   "SECOND"
                                   "THIRD"
                                   "DONE-FIRST"
                                   "FOURTH"
                                   "DONE-SECOND"
                                   "FIFTH"
                                   "DONE-THIRD"
                                   "SIXTH"
                                   "DONE-FOURTH"
                                   "DONE-FIFTH"
                                   "DONE-SIXTH"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest set-queue-name-pipeline
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query-1 (-> (pipeline! [value {:keys [state*]}]
                                  (pp/swap! state* #(vec (conj % value)))
                                  (p/delay 50)
                                  (pp/swap! state* #(vec (conj % (str "DONE-" value)))))
                                (pp/enqueued)
                                (pp/set-queue :query))
                   :query-2 (-> (pipeline! [value {:keys [state*]}]
                                  (pp/swap! state* #(vec (conj % value)))
                                  (p/delay 50)
                                  (pp/swap! state* #(vec (conj % (str "DONE-" value)))))
                                (pp/enqueued)
                                (pp/set-queue :query))
                   :query-3 (-> (pipeline! [value {:keys [state*]}]
                                  (pp/swap! state* #(vec (conj % value)))
                                  (p/delay 50)
                                  (pp/swap! state* #(vec (conj % (str "DONE-" value)))))
                                (pp/enqueued)
                                (pp/set-queue :query))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query-1 "FIRST")
             (<! (timeout 20))
             (invoke runtime :query-2 "SECOND")
             (<! (timeout 20))
             (invoke runtime :query-3 "THIRD")
             (<! (timeout 20))
             (invoke runtime :query-1 "FOURTH")
             (invoke runtime :query-2 "FIFTH")
             (<! (timeout 20))
             (->> (invoke runtime :query-3 "SIXTH")
                  (p/map (fn [_]
                           (is (= ["FIRST" "DONE-FIRST"
                                   "SECOND" "DONE-SECOND"
                                   "THIRD" "DONE-THIRD"
                                   "FOURTH" "DONE-FOURTH"
                                   "FIFTH" "DONE-FIFTH"
                                   "SIXTH" "DONE-SIXTH"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest set-queue-fn-pipeline
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:favorite (-> (pipeline! [value {:keys [state*]}]
                                   (pp/swap! state* #(vec (conj % {:user (:user value)})))
                                   (p/delay 50)
                                   (pp/swap! state* #(vec (conj % (str "DONE-" (:user value))))))
                                 (pp/enqueued)
                                 (pp/set-queue (fn [value]
                                                 [:user (:user value)])))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :favorite {:user 1})
             (<! (timeout 10))
             (invoke runtime :favorite {:user 2})
             (<! (timeout 10))
             (invoke runtime :favorite {:user 3})
             (<! (timeout 20))
             (invoke runtime :favorite {:user 1})
             (invoke runtime :favorite {:user 2})
             (->> (invoke runtime :favorite {:user 3})
                  (p/map (fn [_]
                           (is (= [{:user 1}
                                   {:user 2}
                                   {:user 3}
                                   "DONE-1"
                                   {:user 1}
                                   "DONE-2"
                                   {:user 2}
                                   "DONE-3"
                                   {:user 3}
                                   "DONE-1"
                                   "DONE-2"
                                   "DONE-3"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest basic-keep-latest-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (p/delay 250)
                                (pp/swap! state* #(vec (conj % value))))
                              (pp/keep-latest))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 20))
             (invoke runtime :query 2)
             (<! (timeout 20))
             (invoke runtime :query 3)
             (<! (timeout 20))
             (invoke runtime :query 4)
             (invoke runtime :query 5)
             (<! (timeout 20))
             (invoke runtime :query 6)
             (<! (timeout 500))
             (is (= [1 6] @state*))
             (done)))))

(deftest multiconcurrency-keep-latest-pipeline []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (p/delay 250)
                                (pp/swap! state* #(vec (conj % value))))
                              (pp/keep-latest 3))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 20))
             (invoke runtime :query 2)
             (<! (timeout 20))
             (invoke runtime :query 3)
             (<! (timeout 20))
             (invoke runtime :query 4)
             (invoke runtime :query 5)
             (<! (timeout 20))
             (invoke runtime :query 6)
             (<! (timeout 250))
             (invoke runtime :query 7)
             (<! (timeout 20))
             (invoke runtime :query 8)
             (<! (timeout 20))
             (invoke runtime :query 9)
             (<! (timeout 20))
             (invoke runtime :query 10)
             (<! (timeout 500))
             (is (= [1 2 3 6 7 8 10] @state*))
             (done)))))

(deftest nested-restartable []
  (let [{:keys [state*] :as context} (make-context)
        restartable-pp (-> (pipeline! [value {:keys [state*]}]
                             (p/delay 100)
                             (pp/swap! state* #(vec (conj % (str value "-END")))))
                           (pp/restartable))
        pipelines      {:query (pipeline! [value {:keys [state*]}]
                                 (pp/swap! state* #(vec (conj % value)))
                                 restartable-pp)}
        runtime        (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 10))
             (invoke runtime :query 2)
             (<! (timeout 10))
             (invoke runtime :query 3)
             (<! (timeout 10))
             (invoke runtime :query 4)
             (<! (timeout 10))
             (->> (invoke runtime :query 5)
                  (p/map (fn [_]
                           (is (= [1 2 3 4 5 "5-END"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest nested-enqueued []
  (let [{:keys [state*] :as context} (make-context)
        enqueued-pp (-> (pipeline! [value {:keys [state*]}]
                          (p/delay 100)
                          (pp/swap! state* #(vec (conj % (str value "-END")))))
                        (pp/enqueued))
        pipelines   {:query (pipeline! [value {:keys [state*]}]
                              (pp/swap! state* #(vec (conj % value)))
                              enqueued-pp)}
        runtime     (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 10))
             (invoke runtime :query 2)
             (<! (timeout 10))
             (invoke runtime :query 3)
             (<! (timeout 10))
             (invoke runtime :query 4)
             (<! (timeout 10))
             (->> (invoke runtime :query 5)
                  (p/map (fn [_]
                           (is (= [1 2 3 4 5 "1-END" "2-END" "3-END" "4-END" "5-END"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest nested-dropping []
  (let [{:keys [state*] :as context} (make-context)
        dropping-pp (-> (pipeline! [value {:keys [state*]}]
                          (p/delay 100)
                          (pp/swap! state* #(vec (conj % (str value "-END")))))
                        (pp/dropping))
        pipelines   {:query (pipeline! [value {:keys [state*]}]
                              (pp/swap! state* #(vec (conj % value)))
                              dropping-pp)}
        runtime     (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 10))
             (invoke runtime :query 2)
             (<! (timeout 10))
             (invoke runtime :query 3)
             (<! (timeout 10))
             (invoke runtime :query 4)
             (<! (timeout 10))
             (invoke runtime :query 5)
             (<! (timeout 200))
             (is (= [1 2 3 4 5 "1-END"] @state*))
             (done)))))

(deftest nested-keep-latest []
  (let [{:keys [state*] :as context} (make-context)
        keep-latest-pp (-> (pipeline! [value {:keys [state*]}]
                             (p/delay 100)
                             (pp/swap! state* #(vec (conj % (str value "-END")))))
                           (pp/keep-latest))
        pipelines      {:query (pipeline! [value {:keys [state*]}]
                                 (pp/swap! state* #(vec (conj % value)))
                                 keep-latest-pp)}
        runtime        (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query 1)
             (<! (timeout 10))
             (invoke runtime :query 2)
             (<! (timeout 10))
             (invoke runtime :query 3)
             (<! (timeout 10))
             (invoke runtime :query 4)
             (<! (timeout 10))
             (invoke runtime :query 5)
             (<! (timeout 200))
             (is (= [1 2 3 4 5 "1-END" "5-END"] @state*))
             (done)))))

(deftest shutting-down-runtime-cancels-live-pipelines []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (pipeline! [value {:keys [state*]}]
                            (p/delay 100)
                            (pp/swap! state* #(vec (conj % value))))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query :query-1)
             (invoke runtime :query :query-2)
             (invoke runtime :query :query-3)
             (stop! runtime)
             (<! (timeout 200))
             (is (= nil @state*))
             (done)))))

(deftest pipeline-can-be-configured-to-survive-shutdown []
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (pipeline! [value {:keys [state*]}]
                            (p/delay 100)
                            (pp/swap! state* #(vec (conj % value))))
                   :surviving-query (-> (pipeline! [value {:keys [state*]}]
                                          (p/delay 100)
                                          (pp/swap! state* #(vec (conj % [:surviving-query value]))))
                                        (pp/cancel-on-shutdown false))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query :query-1)
             (invoke runtime :query :query-2)
             (invoke runtime :query :query-3)
             (invoke runtime :surviving-query "I will survive")
             (stop! runtime)
             (<! (timeout 200))
             (is (= [[:surviving-query "I will survive"]] @state*))
             (done)))))

(deftest use-existing-pipeline
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:query (-> (pipeline! [value {:keys [state*]}]
                                (pp/swap! state* #(vec (conj % value)))
                                (p/delay 50)
                                (pp/swap! state* #(vec (conj % (str "DONE-" value)))))
                              (pp/use-existing))}
        runtime   (start! context pipelines)]
    (async done
           (go
             (invoke runtime :query "FIRST")
             (<! (timeout 10))
             (->> (invoke runtime :query "FIRST")
                  (p/map (fn [_]
                           (is (= ["FIRST" "DONE-FIRST"] @state*))
                           (done)))
                  (p/error (fn [_]
                             (is false)
                             (done))))))))

(deftest sync-behavior-1
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:inc (pipeline! [value {:keys [state*]}]
                          (pp/swap! state* inc)
                          (pp/swap! state* inc)
                          (pp/swap! state* inc))}
        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :inc)
    (is (= 3 @state*))))

(deftest sync-behavior-2
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:inc (pipeline! [value {:keys [state*]}]
                          (inc value)
                          (inc value)
                          (inc value)
                          (pp/reset! state* value))}
        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :inc 1)
    (is (= 4 @state*))))

(deftest errors-1
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:inc (pipeline! [value {:keys [state*]}]
                          (pp/swap! state* inc)
                          (throw (ex-info "Error" {:error true}))
                          (rescue! [error]
                            (is (= (ex-message error) "Error"))
                            (is (= (ex-data error) {:error true}))
                            (pp/reset! state* [@state* :rescued])))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :inc)
    (is (= [1 :rescued] @state*))))

(deftest errors-2
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:inc (pipeline! [value {:keys [state*]}]
                          (pp/swap! state* inc)
                          (p/delay 20)
                          (throw (ex-info "Error" {:error true}))
                          (rescue! [error]
                            (is (= (ex-message error) "Error"))
                            (is (= (ex-data error) {:error true}))
                            (pp/reset! state* [@state* :rescued])))}

        runtime   (start! context pipelines)]
    (async done
           (go
             (is (nil? @state*))
             (invoke runtime :inc)
             (<! (timeout 20))
             (is (= [1 :rescued] @state*))
             (done)))))

(deftest sending-promise-as-value
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:inc (pipeline! [value {:keys [state*]}]
                          (inc value)
                          (inc value)
                          (inc value)
                          (pp/reset! state* value))}
        runtime   (start! context pipelines)]
    (async done
           (is (nil? @state*))
           (->> (invoke runtime :inc (p/promise 1))
                (p/map (fn [value]
                         (is (= 4 value @state*))
                         (done)))))))

(deftest finally-1
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          [:begin]
                          (finally! [error]
                                    (conj value :finally)
                                    (pp/reset! state* value)))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run)
    (is (= [:begin :finally] @state*))))

(deftest finally-2
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          [:begin]
                          (throw (ex-info "FOOBAR" {}))
                          (finally! [error]
                                    (conj value :finally)
                                    (pp/reset! state* value)
                                    (is (= "FOOBAR" (ex-message error)))))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run)
    (is (= [:begin :finally] @state*))))

(deftest finally-3
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          [:begin]
                          (throw (ex-info "FOOBAR" {}))
                          (rescue! [error]
                            (conj value :rescue))
                          (finally! [error]
                                    (conj value :finally)
                                    (pp/reset! state* value)
                                    (is (= "FOOBAR" (ex-message error)))))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run)
    (is (= [:begin :rescue :finally] @state*))))

(deftest finally-4
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          [:begin]
                          (throw (ex-info "FOOBAR" {}))
                          (rescue! [error]
                            (conj value :rescue)
                            (throw (ex-info "BARBAZ" {})))
                          (finally! [error]
                                    (conj value :finally)
                                    (pp/reset! state* value)
                                    (is (= "BARBAZ" (ex-message error)))))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run)
    (is (= [:begin :rescue :finally] @state*))))

(deftest rescue-1
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          [:begin]
                          (throw (ex-info "FOOBAR" {}))
                          (rescue! [error]
                            (conj value :rescue)
                            (pp/reset! state* value)
                            (is (= "FOOBAR" (ex-message error)))))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run)
    (is (= [:begin :rescue] @state*))))

(deftest nested-pipeline-cancelling-1
  (let [{:keys [state*] :as context} (make-context [])
        pipelines {:run (-> (pipeline! [value {:keys [state*]}]
                              (pp/swap! state* conj :outer)
                              (pipeline! [value {:keys [state*]}]
                                (p/delay 10)
                                (pp/swap! state* conj :inner)))
                            pp/restartable)}
        runtime   (start! context pipelines                 ;;{:watcher #(println (with-out-str (cljs.pprint/pprint %4)))}
                          )]
    (async done
           (is (= [] @state*))
           (invoke runtime :run)
           (->> (invoke runtime :run)
                (p/map (fn [_]
                         (is (= @state* [:outer :outer :inner]))
                         (done)))))))


(deftest nested-pipeline-cancelling-2
  (let [{:keys [state*] :as context} (make-context [])
        pipelines {:run (-> (pipeline! [value {:keys [state*]}]
                              (pp/swap! state* conj :outer)
                              (pipeline! [value {:keys [state*]}]
                                (when (= 1 (count @state*))
                                  ::runtime/cancelled)
                                (p/delay 10)
                                (pp/swap! state* conj :inner)))
                            pp/restartable)}
        runtime   (start! context pipelines                 ;;{:watcher #(println (with-out-str (cljs.pprint/pprint %4)))}
                          )]
    (async done
           (is (= [] @state*))
           (invoke runtime :run)
           (->> (invoke runtime :run)
                (p/map (fn [_]
                         (is (= @state* [:outer :outer :inner]))
                         (done)))))))

(deftest nested-pipeline-cancelling-3
  (let [{:keys [state*] :as context} (make-context [])
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (pp/swap! state* conj :outer)
                          (-> (pipeline! [value {:keys [state*]}]
                                (when (= 1 (count @state*))
                                  ::runtime/cancelled)
                                (p/delay 10)
                                (pp/swap! state* conj :inner))
                              pp/restartable))}
        runtime   (start! context pipelines)]
    (async done
           (is (= [] @state*))
           (invoke runtime :run)
           (->> (invoke runtime :run)
                (p/map (fn [_]
                         (is (= @state* [:outer :outer :inner]))
                         (done)))))))

(deftest detach-1
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:run (->
                         (pipeline! [value _]
                           (pipeline! [value {:keys [state*]}]
                             (pp/reset! state* [[value :main-1]])
                             (pp/detached
                              (pipeline! [value {:keys [state*]}]
                                (pp/swap! state* conj [value :detached-1])
                                (p/delay 10)
                                (pp/swap! state* conj [value :detached-2])
                                (p/delay 50)
                                (pp/swap! state* conj [value :detached-3])))
                             (pp/swap! state* conj [value :main-2])))
                         pp/restartable)}

        runtime   (start! context pipelines                 ;;{:watcher #(println (with-out-str (cljs.pprint/pprint %4)))}
                          )]
    (async done
           (go
             (is (nil? @state*))
             (invoke runtime :run 1)
             (is (= [[1 :main-1]
                     [1 :detached-1]
                     [1 :main-2]] @state*))
             (<! (timeout 30))
             (is (= [[1 :main-1]
                     [1 :detached-1]
                     [1 :main-2]
                     [1 :detached-2]]
                    @state*))
             (->> (invoke runtime :run 2)
                  (p/map #(p/delay 100))
                  (p/map (fn []
                           (is (= [[2 :main-1]
                                   [2 :detached-1]
                                   [2 :main-2]
                                   [2 :detached-2]
                                   [2 :detached-3]] @state*))
                           (done))))))))

(deftest detach-2
  (let [{:keys [state*] :as context} (make-context [])
        inner     (-> (pipeline! [value {:keys [state*]}]
                        (pp/swap! state* conj [value :inner-1])
                        (p/delay 50)
                        (pp/swap! state* conj [value :inner-2])
                        (p/delay 50)
                        (pp/swap! state* conj [value :inner-3]))
                      pp/restartable)
        pipelines {:run (pipeline! [value _]
                          (pp/detached
                           (pipeline! [value {:keys [state*]}]
                             (p/delay 20)
                             inner))
                          (p/delay 30)
                          inner)}
        runtime   (start! context pipelines                 ;;{:watcher #(println (with-out-str (cljs.pprint/pprint %4)))}
                          )]
    (async done
           (go
             (invoke runtime :run 1)
             (p/delay 20)
             (invoke runtime :run 2)
             (p/delay 20)
             (->> (invoke runtime :run 3)
                  (p/map #(p/delay 100))
                  (p/map (fn []
                           (is (= [[1 :inner-1]
                                   [2 :inner-1]
                                   [3 :inner-1]
                                   [1 :inner-1]
                                   [2 :inner-1]
                                   [3 :inner-1]
                                   [3 :inner-2]
                                   [3 :inner-3]]
                                  @state*))
                           (done))))))))


(deftest mute-1
  (let [{:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          :main-value
                          (pp/reset! state* [value])
                          (pp/muted
                           (pipeline! [value {:keys [state*]}]
                             :muted-value
                             (pp/swap! state* conj value)))
                          (pp/swap! state* conj value))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run)
    (is (= [:main-value :muted-value :main-value] @state*))))

(deftest rejecting-1
  (let [errors*   (atom 0)
        {:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (fn-promise-rejecting))}
        runtime   (start! context pipelines {:error-reporter #(swap! errors* inc)})]
    (async done
           (->> (invoke runtime :run)
                (p/map (fn [_]
                         (is false "Should reject")
                         (done)))
                (p/error (fn [_]
                           (is (pos? @errors*))
                           (done)))))))

(deftest rejecting-2
  (let [errors*   (atom 0)
        {:keys [state*] :as context} (make-context)
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (fn-throwing))}
        runtime   (start! context pipelines {:error-reporter #(swap! errors* inc)})]
    (invoke runtime :run)
    (is (pos? @errors*))))

(deftest on-cancel
  (let [is-on-cancel-called* (atom false)
        pipelines {:run (pipeline! [value ctx]
                          (p/delay 1000))}
        runtime (start! {} pipelines {:on-cancel #(reset! is-on-cancel-called* true)})]
    (async done
           (go
             (invoke runtime :run)
             (stop! runtime)
             (<! (timeout 10))
             (is @is-on-cancel-called*)
             (done)))))

(deftest changing-interpreter-state-1
  (let [{:keys [state*] :as context} (make-context)
        capture
        (runtime/fn->pipeline-step
         (fn [runtime context value error opts]
           (let [{:keys [parent interpreter-state]} opts
                 interpreter-state' (assoc-in interpreter-state [0 :state :value] :resumed)
                 resumable          (runtime/interpreter-state->resumable interpreter-state' true)]
             (runtime/invoke-resumable runtime resumable (assoc opts :interpreter-state interpreter-state'))
             nil)))
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (pp/reset! state* [:shared])
                          capture
                          (swap! state* conj value))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run :original)
    (is (= [:shared :resumed :original] @state*))))

(deftest changing-interpreter-state-2
  (let [{:keys [state*] :as context} (make-context)
        capture
        (runtime/fn->pipeline-step
         (fn [runtime _ _ _ {:keys [interpreter-state]}]
           (let [root-val (get-in (last interpreter-state) [:state :value])]
             (if (even? root-val)
               (runtime/interpreter-state->resumable (assoc-in interpreter-state [0 :state :value] :resumed))
               nil))))
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (pp/reset! state* [:shared value])
                          capture
                          (swap! state* conj value))}

        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (invoke runtime :run 1)
    (is (= [:shared 1 1] @state*))
    (invoke runtime :run 2)
    (is (= [:shared 2 :resumed] @state*))))

(deftest changing-interpreter-state-3
  (let [{:keys [state*] :as context} (make-context)
        capture
        (runtime/fn->pipeline-step
         (fn [_ _ _ _ {:keys [interpreter-state]}]
           (let [root-val (get-in (last interpreter-state) [:state :value])]
             (if (even? root-val)
               (runtime/interpreter-state->resumable (assoc-in interpreter-state [0 :state :value] :resumed))
               nil))))
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (pp/reset! state* [:shared value])
                          (p/delay 10)
                          capture
                          (p/delay 10)
                          (swap! state* conj value))}

        runtime   (start! context pipelines)]
    (async done
           (is (nil? @state*))
           (->> (invoke runtime :run 1)
                (p/map #(is (= [:shared 1 1] @state*)))
                (p/map #(invoke runtime :run 2))
                (p/map #(is (= [:shared 2 :resumed] @state*)))
                (p/map done)))))

(deftest changing-interpreter-state-4
  (let [{:keys [state*] :as context} (make-context)
        capture
        (runtime/fn->pipeline-step
         (fn [_ _ _ _ {:keys [interpreter-state]}]
           (let [root-val (get-in (last interpreter-state) [:state :value])]
             (if (even? root-val)
               (runtime/interpreter-state->resumable (assoc-in interpreter-state [0 :state :value] :resumed))
               nil))))
        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (pp/reset! state* [:shared value])
                          (p/delay 10)
                          (pipeline! [value {:keys [state*]}]
                            (pipeline! [value {:keys [state*]}]
                              (pp/swap! state* conj :nested)
                              capture))
                          (p/delay 10)
                          (swap! state* conj value))}

        runtime   (start! context pipelines)]
    (async done
           (is (nil? @state*))
           (->> (invoke runtime :run 1)
                (p/map #(is (= [:shared 1 :nested 1] @state*)))
                (p/map #(invoke runtime :run 2))
                (p/map #(is (= [:shared 2 :nested :resumed] @state*)))
                (p/map done)))))

(deftest changing-interpreter-state-5
  (let [{:keys [state*] :as context} (make-context)
        extra-pipeline (fn [& _]
                         (pipeline! [value {:keys [state*]}]
                           (pp/swap! state* conj :injected)
                           :injected-value))

        inject-extra-pipeline
        (fn [{:keys [state] :as resumable}]
          (update-in resumable [:state :pipeline (:block state)] #(concat [extra-pipeline] %)))

        capture
        (runtime/fn->pipeline-step
         (fn [_ _ _ _ {:keys [interpreter-state]}]
           (runtime/interpreter-state->resumable (update interpreter-state 0 inject-extra-pipeline))))

        pipelines      {:run (pipeline! [value {:keys [state*]}]
                               (pp/reset! state* [:shared value])
                               (p/delay 10)
                               (pipeline! [value {:keys [state*]}]
                                 (pipeline! [value {:keys [state*]}]
                                   (pp/swap! state* conj :nested)
                                   capture))
                               (p/delay 10)
                               (swap! state* conj value))}

        runtime        (start! context pipelines)]
    (async done
           (is (nil? @state*))
           (->> (invoke runtime :run 1)
                (p/map #(is (= [:shared 1 :nested :injected :injected-value] @state*)))
                (p/map done)))))

(deftest changing-interpreter-state-6
  (let [{:keys [state*] :as context} (make-context)
        extra-pipeline (fn [& _]
                         (pipeline! [value {:keys [state*]}]
                           (pp/swap! state* conj :injected)
                           :injected-value))
        inject-extra-pipeline
        (fn [{:keys [state] :as resumable}]
          (update-in resumable [:state :pipeline (:block state)] #(concat [extra-pipeline] %)))
        capture
        (runtime/fn->pipeline-step
         (fn [_ _ _ _ {:keys [interpreter-state]}]
           (runtime/interpreter-state->resumable (update interpreter-state 0 inject-extra-pipeline))))
        pipelines      {:run (pipeline! [value {:keys [state*]}]
                               (pp/reset! state* [:shared value])
                               (p/delay 10)
                               (pipeline! [value {:keys [state*]}]
                                 (pipeline! [value {:keys [state*]}]
                                   (pp/swap! state* conj :nested)
                                   capture
                                   (pp/swap! state* conj :after-capture)))
                               (p/delay 10)
                               (swap! state* conj value))}

        runtime        (start! context pipelines)]
    (async done
           (is (nil? @state*))
           (->> (invoke runtime :run 1)
                (p/map #(is (= [:shared 1 :nested :injected :after-capture :injected-value] @state*)))
                (p/map done)))))

(deftest changing-interpreter-state-7
  (let [{:keys [state*] :as context} (make-context)
        inject-extra
        (fn [{:keys [state] :as resumable} extra]
          (update-in resumable [:state :pipeline (:block state)]
                     #(conj (vec %) (constantly (runtime/->pipeline (runtime/interpreter-state->resumable extra true))))))

        capture
        (runtime/fn->pipeline-step
         (fn [runtime _ _ _ {:keys [interpreter-state]}]
           (let [interpreter-state-without-last (vec (drop-last interpreter-state))
                 last-resumable                 (last interpreter-state)]

             (runtime/interpreter-state->resumable
              (conj interpreter-state-without-last (inject-extra last-resumable interpreter-state))))))

        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (pp/reset! state* 0)
                          (pp/swap! state* inc)
                          capture
                          (pp/swap! state* inc)
                          (pipeline! [value {:keys [state*]}]
                            (p/delay 10)
                            (pipeline! [value {:keys [state*]}]
                              (pp/swap! state* inc)
                              (p/delay 10)
                              (pipeline! [value {:keys [state*]}]
                                (pp/swap! state* inc)))
                            (p/delay 10)
                            (pp/swap! state* inc)))}

        runtime   (start! context pipelines)]
    (async done
           (is (nil? @state*))
           (->> (invoke runtime :run)
                (p/map #(is (= 9 @state*)))
                (p/map done)))))

(deftest changing-interpreter-state-8
  (let [cache*    (atom {})
        val*      (atom {})
        get-val   (fn [req-name]
                    (p/create
                     (fn [resolve]
                       (js/setTimeout (fn []
                                        (let [val-store (swap! val* update req-name inc)
                                              val       (get val-store req-name)]
                                          (swap! cache* assoc req-name val)
                                          (resolve val))) 20))))
        {:keys [state*] :as context} (make-context)

        set-interpreter-value
        (fn [interpreter-state value]
          (assoc-in interpreter-state [0 :state :value] value))

        set-revalidate
        (fn [interpreter-state runtime pipeline-opts req]
          (let [interpreter-state-without-last (vec (drop-last interpreter-state))
                last-resumable                 (last interpreter-state)
                state                          (:state last-resumable)
                resumable                      (-> (runtime/interpreter-state->resumable interpreter-state)
                                                   (assoc-in [:state :value] (get-val req))
                                                   pp/detached)
                as-pipeline                    (fn [_ _]
                                                 (runtime/invoke-resumable runtime resumable pipeline-opts))]


            (conj interpreter-state-without-last (update-in last-resumable [:state :pipeline (:block state)] #(conj (vec %) as-pipeline)))))
        stale-while-revalidate
        (fn [req-name]
          (let [cache  @cache*
                cached (get cache req-name)]
            (if-not cached
              (get-val req-name)
              (runtime/fn->pipeline-step
               (fn [runtime _ _ _ {:keys [interpreter-state] :as pipeline-opts}]
                 (runtime/interpreter-state->resumable
                  (-> interpreter-state
                      (set-interpreter-value cached)
                      (set-revalidate runtime pipeline-opts req-name))))))))

        pipelines {:run (pipeline! [value {:keys [state*]}]
                          (pp/reset! state* nil)
                          (stale-while-revalidate :foo)
                          (pipeline! [value {:keys [state*]}]
                            [:wrap value]
                            (pp/reset! state* value)))}

        runtime   (start! context pipelines)]
    (async done
           (is (nil? @state*))
           (->> (invoke runtime :run)
                (p/map (fn [val]
                         (is (= [:wrap 1] @state*))))
                (p/map (fn []
                         (invoke runtime :run)
                         (is (= [:wrap 1] @state*))))
                (p/map #(p/delay 50))
                (p/map #(is (= [:wrap 2] @state*)))
                (p/map done)))))

(deftest querying-runtime
  (let [log* (atom [])
        invoke-without-blocking (fn [runtime k]
                                  (pp/invoke runtime k)
                                  nil)
        pipelines {:inc (pipeline! [value ctx]
                          (p/delay 10)
                          (inc value))
                   :dec (pipeline! [value ctx]
                          (p/delay 10)
                          (inc value))
                   :runtime-query (pipeline! [value {:pipeline/keys [runtime ident]}]
                                    (pp/swap! log* conj (set (keys (pp/get-active runtime))))
                                    (let [active           (pp/get-active runtime)
                                          idents-to-cancel (->> (vals active)
                                                                (mapcat (fn [v] (keys v)))
                                                                (remove #(= ident %)))]
                                      (pp/cancel-all runtime idents-to-cancel)
                                      (-> (pipeline! [value {:pipeline/keys [runtime ident]}]
                                            (pp/swap! log* conj (set (keys (pp/get-active runtime))))
                                            (invoke-without-blocking runtime :inc)
                                            (pp/swap! log* conj (set (keys (pp/get-active runtime)))))
                                          (pp/set-queue :inner))))}
        runtime (start! {} pipelines)]
    (async done
           (invoke runtime :inc)
           (invoke runtime :dec)
           (->> (invoke runtime :runtime-query)
                (p/map (fn []
                         (is (= [#{:inc :dec :runtime-query}
                                 #{:runtime-query :inner}
                                 #{:inc :runtime-query :inner}]
                                @log*))
                         (is (= [:inc] (keys (pp/get-active runtime))))
                         (let [idents-to-cancel (->> (vals (pp/get-active runtime))
                                                     (mapcat #(keys %)))]
                           (pp/cancel-all runtime idents-to-cancel)
                           (is (= nil (keys (pp/get-active runtime))))
                           (stop! runtime)
                           (done))))))))

(deftest runtime-should-not-report-error-if-rescue-block-exists-1
  (let [error-log* (atom nil)
        pipelines  {:test (pipeline! [_ _]
                            (throw (ex-info "Error" {:is-error true}))
                            (rescue! [err]))}
        runtime (start! {} pipelines {:error-reporter #(reset! error-log* %)})]
    (invoke runtime :test)
    (is (nil? @error-log*))))

(deftest runtime-should-not-report-error-if-rescue-block-exists-async-1
  (let [error-log* (atom nil)
        async-error (fn [err]
                      (let [deferred (p/deferred)]
                        (js/setTimeout #(p/reject! deferred err) 1)
                        deferred))
        pipelines  {:test (pipeline! [_ _]
                            (async-error (ex-info "Error" {:is-error true}))
                            (rescue! [err]))}
        runtime (start! {} pipelines {:error-reporter #(reset! error-log* %)})]
    (async done
           (->> (invoke runtime :test)
                (p/map (fn []
                         (is (nil? @error-log*))
                         (done)))
                (p/error (fn []
                           (is false)
                           (done)))))))


(deftest runtime-should-not-report-error-if-rescue-block-exists-2
  (let [error-log* (atom nil)
        pipelines  {:test (pipeline! [_ _]
                            (throw (ex-info "Error" {:is-error true}))
                            (rescue! [err]
                              1))}
        runtime (start! {} pipelines {:error-reporter #(reset! error-log* %)})]
    (is (= 1 (invoke runtime :test)))
    (is (nil? @error-log*))))

(deftest runtime-should-not-report-error-if-rescue-block-exists-async-2
  (let [error-log* (atom nil)
        async-error (fn [err]
                      (let [deferred (p/deferred)]
                        (js/setTimeout #(p/reject! deferred err) 1)
                        deferred))
        pipelines  {:test (pipeline! [_ _]
                            (async-error (ex-info "Error" {:is-error true}))
                            (rescue! [err]
                              1))}
        runtime (start! {} pipelines {:error-reporter #(reset! error-log* %)})]
    (async done
           (->> (invoke runtime :test)
                (p/map (fn [res]
                         (is (= 1 res))
                         (is (nil? @error-log*))
                         (done)))
                (p/error (fn []
                           (is false)
                           (done)))))))

(deftest it-should-be-possible-to-resolve-a-promise-with-pipeline-1
  (let [{:keys [state*] :as context} (make-context)
        inner-pipeline (pipeline! [value {:keys [state*]}]
                         (pp/swap! state* inc)
                         (pp/swap! state* inc)
                         @state*)
        fn-returning-pipeline (constantly inner-pipeline)
        fn-returning-promise-resolved-to-pipeline (fn []
                                                    (p/resolved inner-pipeline))
        pipelines {:inc (pipeline! [value {:keys [state*]}]
                          (pp/swap! state* inc)
                          (pp/swap! state* inc)
                          (pp/swap! state* inc)
                          inner-pipeline
                          (fn-returning-pipeline)
                          (fn-returning-promise-resolved-to-pipeline)
                          (fn-returning-promise-resolved-to-pipeline)
                          (pp/swap! state* inc))}
        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (async done
           (->> (invoke runtime :inc)
                (p/map (fn [res]
                         (is (= 11 res))
                         (is (= 12 @state*))
                         (done)))
                (p/error (fn []
                           (is false)
                           (done)))))))

(deftest it-should-be-possible-to-resolve-a-promise-with-pipeline-2
  (let [{:keys [state*] :as context} (make-context)
        inner-pipeline (pipeline! [value {:keys [state*]}]
                         (inc value))
        fn-returning-pipeline (constantly inner-pipeline)
        fn-returning-promise-resolved-to-pipeline (fn []
                                                    (p/resolved inner-pipeline))
        pipelines {:inc (pipeline! [value {:keys [state*]}]
                          (inc value)
                          (inc value)
                          (inc value)
                          inner-pipeline
                          (fn-returning-pipeline)
                          (fn-returning-promise-resolved-to-pipeline)
                          (fn-returning-promise-resolved-to-pipeline)
                          (inc value))}
        runtime   (start! context pipelines)]
    (is (nil? @state*))
    (async done
           (->> (invoke runtime :inc 1)
                (p/map (fn [res]
                         (is (= 9 res))
                         (done)))
                (p/error (fn []
                           (is false)
                           (done)))))))

(deftest it-can-run-pipeline-without-registering-it-first
  (let [{:keys [state*] :as context} (make-context)
        p (pipeline! [value {:keys [state*]}]
            (inc value)
            (reset! state* value))
        runtime (start! context {})
        res (invoke runtime p 0)]
    (is (= res 1))
    (is (= 1 @state*))))

(deftest error-reporter-is-called-only-once-sync
  (let [{:keys [state*] :as context} (make-context)
        error-reporter-count* (atom 0)
        p (pipeline! [value {:keys [state*]}]
            (throw (ex-info "Error1" {})))
        runtime (start! context {:p p} {:error-reporter (fn [_] (swap! error-reporter-count* inc))})]
    (invoke runtime :p 0)
    (is (= 1 @error-reporter-count*))))

(deftest error-reporter-is-called-only-once-async
  (let [{:keys [state*] :as context} (make-context)
        error-reporter-count* (atom 0)
        p (pipeline! [value {:keys [state*]}]
            (p/delay 10)
            (throw (ex-info "Error2" {})))
        runtime (start! context {:p p} {:error-reporter (fn [_] (swap! error-reporter-count* inc))})]
    (invoke runtime :p 0)
    (async done
           (go
             (<! (timeout 20))
             (is (= 1 @error-reporter-count*))
             (done)))))