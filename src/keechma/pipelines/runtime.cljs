(ns keechma.pipelines.runtime
  (:require [promesa.core :as p]
            [medley.core :refer [dissoc-in]]
            [cljs.core.async :refer [chan put! <! close! alts!]])
  (:require-macros [cljs.core.async.macros :refer [go-loop go]]))

(def ^:dynamic *pipeline-depth* 0)
(declare invoke-resumable)
(declare start-resumable)
(declare make-ident)
(declare map->Pipeline)

(defprotocol IPipelineRuntime
  (invoke [this pipeline] [this pipeline args] [this pipeline args config])
  (cancel [this ident])
  (cancel-all [this idents])
  (on-cancel [this promise])
  (wait [this ident])
  (wait-all [this idents])
  (transact [this transact-fn])
  (stop! [this])
  (report-error [this error])
  (get-pipeline-instance* [this ident])
  (get-state* [this])
  (get-active [this]))

(defprotocol IPipeline
  (->resumable [this pipeline-name value]))

(defprotocol IResumable
  (->pipeline [this]))

(defrecord Resumable [id ident config args state tail]
  IResumable
  (->pipeline [this]
    (map->Pipeline {:id id
                    :config config
                    :pipeline (:pipeline state)})))

(defrecord Pipeline [id pipeline config]
  IPipeline
  (->resumable [_ pipeline-name value]
    (map->Resumable {:id id
                     :ident (make-ident (or pipeline-name id))
                     :args value
                     :config config
                     :state {:block :begin
                             :pipeline pipeline
                             :value value}})))

(defn make-ident [pipeline-id]
  [pipeline-id (keyword "pipeline" (gensym 'instance))])

(defn make-pipeline [id pipeline]
  (map->Pipeline
   {:id id
    :pipeline pipeline
    :config {:concurrency {:max js/Infinity}
             :cancel-on-shutdown true}}))

(defn in-pipeline? []
  (pos? *pipeline-depth*))

(defn resumable? [val]
  (instance? Resumable val))

(defn fn->pipeline-step [pipeline-fn]
  (with-meta pipeline-fn {::pipeline-step? true}))

(defn error? [value]
  (instance? js/Error value))

(defn pipeline? [val]
  (instance? Pipeline val))

(defn pipeline-step? [value]
  (let [m (meta value)]
    (::pipeline-step? m)))

(defn as-error [value]
  (if (error? value)
    value
    (ex-info "Unknown Error" {:value value})))

(defn promise->chan [promise]
  (let [promise-chan (chan)]
    (->> promise
         (p/map (fn [v]
                  (when v
                    (put! promise-chan v))
                  (close! promise-chan)))
         (p/error (fn [e]
                    (put! promise-chan (as-error e))
                    (close! promise-chan))))
    promise-chan))

(defn interpreter-state->resumable
  ([stack] (interpreter-state->resumable stack false))
  ([stack use-fresh-idents]
   (reduce
    (fn [acc v]
      (let [[pipeline-id instance-id] (:ident v)
            ident (if use-fresh-idents (make-ident pipeline-id) [pipeline-id instance-id])]
        (assoc (map->Resumable (assoc v :ident ident)) :tail acc)))
    nil
    stack)))

(defn execute [runtime context ident action value error get-interpreter-state]
  (try
    (let [val (action value (assoc context :pipeline/runtime runtime :pipeline/ident ident) error)]
      (cond
        (and (fn? val) (pipeline-step? val))
        (val runtime context value error {:parent ident :interpreter-state (get-interpreter-state)})

        (pipeline? val)
        (let [resumable (->resumable val nil value)]
          (invoke-resumable runtime resumable {:parent ident :interpreter-state (get-interpreter-state)}))

        :else val))
    (catch :default err
      err)))

(defn real-value [value prev-value]
  (if (nil? value)
    prev-value
    value))

(defn resumable-with-resumed-tail [runtime resumable]
  (if-let [tail (:tail resumable)]
    (let [ident (:ident resumable)
          resumed-value
          (invoke-resumable runtime tail {:parent ident :is-detached false :interpreter-state (:tail tail)})]
      (-> resumable
          (assoc :tail nil)
          (assoc-in [:state :value] resumed-value)))
    resumable))

(defn run-sync-block [runtime context resumable props]
  (let [resumable' (resumable-with-resumed-tail runtime resumable)
        {:keys [ident state]} resumable'
        {:keys [block prev-value value error pipeline]} state]

    (loop [block      block
           pipeline   pipeline
           prev-value prev-value
           value      value
           error      error]
      (let [{:keys [begin rescue finally]} pipeline
            get-interpreter-state
            (fn []
              (let [{:keys [interpreter-state]} props
                    state {:block block
                           :pipeline (update pipeline block rest)
                           :prev-value prev-value
                           :value value
                           :error error}]
                (vec (concat [(assoc resumable' :state state)] interpreter-state))))]

        (cond
          (= ::cancelled value)
          [:result value]

          (resumable? value)
          [:resumable-state value]

          (p/promise? value)
          [:promise
           (assoc resumable' :state {:pipeline pipeline
                                     :block block
                                     :value (p/then value #(real-value % prev-value))
                                     :prev-value prev-value
                                     :error error})]

          (pipeline? value)
          (let [resumable (->resumable value nil prev-value)
                res (invoke-resumable runtime resumable {:parent ident :interpreter-state (get-interpreter-state)})]
            (recur block pipeline prev-value (real-value res prev-value) error))

          :else
          (case block
            :begin
            (cond
              (error? value)
              (cond
                (boolean rescue) (recur :rescue pipeline prev-value prev-value value)
                (boolean finally) (recur :finally pipeline prev-value prev-value value)
                :else [:error value])

              (and (not (seq begin)) (boolean finally))
              (recur :finally pipeline prev-value value error)

              (not (seq begin))
              [:result value]

              :else
              (let [[action & rest-actions] begin
                    next-value (execute runtime context ident action value error get-interpreter-state)]
                (recur :begin (assoc pipeline :begin rest-actions) value (real-value next-value value) error)))

            :rescue
            (cond
              (error? value)
              (cond
                (boolean finally) (recur :finally pipeline prev-value prev-value value)
                :else [:error value])

              (and (not (seq rescue)) (boolean finally))
              (recur :finally pipeline prev-value value error)

              (not (seq rescue))
              [:result value]

              :else
              (let [[action & rest-actions] rescue
                    next-value (execute runtime context ident action value error get-interpreter-state)]
                (recur :rescue (assoc pipeline :rescue rest-actions) value (real-value next-value value) error)))

            :finally
            (cond
              (error? value)
              [:error value]

              (not (seq finally))
              [:result value]

              :else
              (let [[action & rest-actions] finally
                    next-value (execute runtime context ident action value error get-interpreter-state)]
                (recur :finally (assoc pipeline :finally rest-actions) value (real-value next-value value) error)))))))))

(defn run-sync-block-until-no-resumable-state [runtime context resumable props]
  (let [[res-type payload] (transact runtime #(run-sync-block runtime context resumable props))]
    (if (= :resumable-state res-type)
      (if (:is-root props)
        (recur runtime context payload props)
        [res-type payload])
      [res-type payload])))

(defn start-interpreter [runtime context resumable props]
  (let [{:keys [deferred-result canceller ident]} props
        [res-type payload] (run-sync-block-until-no-resumable-state runtime context resumable props)]
    (cond
      (= :resumable-state res-type) payload
      (= :result res-type) payload
      (= :error res-type) (throw payload)
      :else
      (do
        (go-loop [resumable payload]
          (let [state (:state resumable)
                [value c] (alts! [(promise->chan (:value state)) canceller])]
            (cond
              (or (= ::cancelled (:state (get-pipeline-instance* runtime ident))) (= canceller c))
              (do
                (on-cancel runtime (:value state))
                nil)

              (= ::cancelled value)
              (p/resolve! deferred-result ::cancelled)

              :else
              (let [[next-res-type next-payload]
                    (run-sync-block-until-no-resumable-state runtime context (assoc-in resumable [:state :value] value) props)]
                (cond
                  (= :resumable-state res-type) next-payload
                  (= :result next-res-type) (p/resolve! deferred-result next-payload)
                  (= :error next-res-type) (p/reject! deferred-result next-payload)
                  :else (when next-payload (recur next-payload)))))))
        deferred-result))))

(def live-states #{::running ::pending ::waiting-children})
(def running-states #{::running ::waiting-children})

(defn process-pipeline [[pipeline-name pipeline]]
  [pipeline-name (update-in pipeline [:config :queue-name] #(or % pipeline-name))])

(defn get-pipeline-instance [state ident]
  (get-in state [:instances ident]))

(defn can-use-existing? [resumable]
  (get-in resumable [:config :use-existing]))

(defn get-queue [state queue-name]
  (get-in state [:queues queue-name]))

(defn get-queue-config [state queue-name]
  (get-in state [:queues queue-name :config]))

(defn get-queue-queue [state queue-name]
  (get-in state [:queues queue-name :queue]))

(defn get-resumable-queue-name [resumable]
  (let [queue-name (or (get-in resumable [:config :queue-name]) (:id resumable))]
    (if (fn? queue-name)
      (queue-name (:args resumable))
      queue-name)))

(defn get-existing [state resumable]
  (let [queue-name (get-resumable-queue-name resumable)]
    (->> (get-queue-queue state queue-name)
         (filter
          (fn [ident]
            (let [instance (get-pipeline-instance state ident)]
              (and (= (get-in instance [:resumable :id]) (:id resumable))
                   (= (get-in instance [:resumable :args]) (:args resumable))
                   (contains? live-states (:state instance))))))
         first
         (get-pipeline-instance state))))

(defn add-to-parent [state {:keys [ident]}]
  (let [instance     (get-pipeline-instance state ident)
        parent-ident (get-in instance [:props :parent])]
    (if parent-ident
      (update-in state [:instances parent-ident :props :children] #(conj (set %) ident))
      state)))

(defn remove-from-parent [state {:keys [ident]}]
  (let [instance     (get-pipeline-instance state ident)
        parent-ident (get-in instance [:props :parent])]
    (if (and parent-ident (get-in state [:instances parent-ident]))
      (update-in state [:instances parent-ident :props :children] #(disj (set %) ident))
      state)))

(defn add-to-queue [state resumable]
  (let [ident      (:ident resumable)
        queue-name (or (get-resumable-queue-name resumable))
        queue      (or (get-queue state queue-name) {:config (get-in resumable [:config :concurrency]) :queue []})]
    (assoc-in state [:queues queue-name] (assoc queue :queue (conj (:queue queue) ident)))))

(defn remove-from-queue [state resumable]
  (let [ident       (:ident resumable)
        queue-name  (get-resumable-queue-name resumable)
        queue-queue (get-queue-queue state queue-name)]
    (assoc-in state [:queues queue-name :queue] (filterv #(not= ident %) queue-queue))))

(defn can-start-immediately? [state resumable]
  (let [queue-name      (get-resumable-queue-name resumable)
        queue-config    (get-queue-config state queue-name)
        realized-queue  (map #(get-pipeline-instance state %) (get-queue-queue state queue-name))
        max-concurrency (get queue-config :max js/Infinity)
        enqueued        (filter #(contains? live-states (:state %)) realized-queue)]
    (> max-concurrency (count enqueued))))

(defn update-instance-state [state resumable instance-state]
  (assoc-in state [:instances (:ident resumable) :state] instance-state))

(defn register-instance
  ([state resumable props] (register-instance state resumable props ::pending))
  ([state resumable props instance-state]
   (-> state
       (add-to-queue resumable)
       (assoc-in [:instances (:ident resumable)] {:state instance-state :resumable resumable :props props})
       (add-to-parent resumable))))

(defn deregister-instance [state resumable]
  (-> state
      (remove-from-queue resumable)
      (remove-from-parent resumable)
      (dissoc-in [:instances (:ident resumable)])))

(defn queue-assoc-last-result [state resumable result]
  (let [queue-name (get-resumable-queue-name resumable)]
    (assoc-in state [:queues queue-name :last-result] result)))

(defn queue-assoc-last-error [state resumable result]
  (let [queue-name (get-resumable-queue-name resumable)]
    (assoc-in state [:queues queue-name :last-error] result)))

(defn queue-assoc-last [state resumable result]
  (if (not= ::cancelled result)
    (if (error? result)
      (queue-assoc-last-error state resumable result)
      (queue-assoc-last-result state resumable result))
    state))

(defn get-queued-idents-to-cancel [state queue-name]
  (let [queue           (get-queue state queue-name)
        queue-queue     (:queue queue)
        max-concurrency (get-in queue [:config :max])
        behavior        (get-in queue [:config :behavior])
        free-slots      (dec max-concurrency)]
    (case behavior
      :restartable
      (let [cancellable (filterv #(contains? live-states (:state (get-pipeline-instance state %))) queue-queue)]
        (take (- (count cancellable) free-slots) cancellable))

      :keep-latest
      (filterv #(= ::pending (:state (get-pipeline-instance state %))) queue-queue)

      [])))

(defn get-queued-idents-to-start [state queue-name]
  (let [queue           (get-queue state queue-name)
        queue-queue     (:queue queue)
        max-concurrency (get-in queue [:config :max])
        pending         (filterv #(= ::pending (:state (get-pipeline-instance state %))) queue-queue)
        running         (filterv #(contains? running-states (:state (get-pipeline-instance state %))) queue-queue)]
    (take (- max-concurrency (count running)) pending)))

(defn start-next-in-queue [{:keys [state*] :as runtime} queue-name]
  (let [queued-idents-to-start (get-queued-idents-to-start @state* queue-name)]
    (doseq [ident queued-idents-to-start]
      (start-resumable runtime (:resumable (get-pipeline-instance @state* ident))))))

(defn cleanup-parents [{:keys [state*] :as runtime} instance]
  (let [parent-instance (get-in @state* [:instances (get-in instance [:props :parent])])
        children        (get-in instance [:props :children])]
    (when (and (= ::waiting-children (:state parent-instance))
               (not (seq children)))
      (let [resumable (:resumable parent-instance)]
        (swap! state* deregister-instance resumable)
        (start-next-in-queue runtime (get-resumable-queue-name resumable))
        (recur runtime parent-instance)))))

(defn finish-resumable [{:keys [state*] :as runtime} {:keys [ident] :as resumable} result]
  (when (get-pipeline-instance @state* ident)
    (if (= ::cancelled result)
      (cancel runtime ident)
      (let [instance (get-pipeline-instance @state* ident)]
        (if (seq (get-in instance [:props :children]))
          (swap! state* update-instance-state resumable ::waiting-children)
          (do (swap! state* (fn [state]
                              (-> state
                                  (queue-assoc-last resumable result)
                                  (deregister-instance resumable))))
              (cleanup-parents runtime instance)
              (start-next-in-queue runtime (get-resumable-queue-name resumable)))))))
  result)

(defn enqueue-resumable [{:keys [state*] :as runtime} resumable props]
  (let [queued-idents-to-cancel (get-queued-idents-to-cancel @state* (get-resumable-queue-name resumable))]
    (swap! state* (fn [state] (-> state (register-instance resumable props ::pending))))
    (cancel-all runtime queued-idents-to-cancel)
    (:deferred-result props)))

(defn start-resumable [{:keys [state* context] :as runtime} {:keys [ident] :as resumable}]
  (swap! state* update-instance-state resumable ::running)
  (let [props           (:props (get-pipeline-instance @state* ident))
        deferred-result (:deferred-result props)
        res             (try
                          (start-interpreter runtime context resumable props)
                          (catch :default e
                            (report-error runtime e)
                            e))]
    (if (p/promise? res)
      (->> deferred-result
           (p/map #(finish-resumable runtime resumable %))
           (p/error (fn [error]
                      (report-error runtime error)
                      (finish-resumable runtime resumable error)
                      (p/rejected error))))
      (do (if (error? res)
            (p/reject! deferred-result res)
            (p/resolve! deferred-result res))
          (finish-resumable runtime resumable res)))))

(defn throw-if-queues-not-matching [state resumable]
  (let [queue-name      (get-resumable-queue-name resumable)
        queue-config    (get-queue-config state queue-name)
        pipeline-config (get-in resumable [:config :concurrency])]
    (when (and queue-config (not= queue-config pipeline-config))
      (throw (ex-info "Pipeline's queue config is not matching queue's config"
                      {:pipeline (:ident resumable)
                       :queue queue-name
                       :queue-config queue-config
                       :pipeline-config pipeline-config})))))

(defn process-resumable [{:keys [state*] :as runtime} resumable props]
  (cond
    (can-start-immediately? @state* resumable)
    (do (swap! state* register-instance resumable props ::running)
        (start-resumable runtime resumable))

    (= :dropping (get-in resumable [:config :concurrency :behavior]))
    ::cancelled

    :else
    (enqueue-resumable runtime resumable props)))

(defn invoke-resumable [runtime resumable {:keys [parent] :as pipeline-opts}]
  (let [{:keys [state*]} runtime
        deferred-result (p/deferred)
        canceller       (chan)
        is-detached     (get-in resumable [:config :is-detached])
        props           (merge
                         {:interpreter-state []}
                         pipeline-opts
                         {:canceller canceller
                          :ident (:ident resumable)
                          :is-root (nil? parent)
                          :deferred-result deferred-result
                          :children #{}})
        state           @state*]

    (throw-if-queues-not-matching state resumable)

    (let [res (if (can-use-existing? resumable)
                (if-let [existing (get-in (get-existing state resumable) [:props :deferred-result])]
                  existing
                  (process-resumable runtime resumable props))
                (process-resumable runtime resumable props))]
      (if-not is-detached res nil))))

(defn get-cancel-root-ident [state ident]
  (let [instance     (get-pipeline-instance state ident)
        is-detached  (get-in instance [:resumable :config :is-detached])
        parent-ident (get-in instance [:props :parent])]
    (if (and parent-ident (not is-detached))
      (recur state parent-ident)
      ident)))

(defn get-ident-and-descendant-idents
  ([state ident] (get-ident-and-descendant-idents state ident [ident]))
  ([state ident descendants]
   (let [instance (get-pipeline-instance state ident)
         children (get-in instance [:props :children])]
     (if (seq children)
       (vec (concat descendants (mapcat #(get-ident-and-descendant-idents state %) children)))
       descendants))))

(defn has-pipeline? [{:keys [state*]} pipeline-name]
  (boolean (get-in @state* [:pipelines pipeline-name])))

(defrecord PipelineRuntime [context state* pipelines opts]
  IPipelineRuntime
  (invoke [this pipeline-name]
    (invoke this pipeline-name nil nil))
  (invoke [this pipeline-name args]
    (invoke this pipeline-name args nil))
  (invoke [this pipeline-name args pipeline-opts]
    (let [pipeline (if (pipeline? pipeline-name)
                     pipeline-name
                     (get-in @state* [:pipelines pipeline-name]))]
      (invoke-resumable this (->resumable pipeline pipeline-name args) pipeline-opts)))
  (transact [_ transaction]
    (binding [*pipeline-depth* (inc *pipeline-depth*)]
      (let [{:keys [transactor]} opts]
        (transactor transaction))))
  (report-error [_ error]
    (let [reporter (:error-reporter opts)]
      (reporter error)))
  (get-pipeline-instance* [this ident]
    (reify
      IDeref
      (-deref [_] (get-pipeline-instance @state* ident))))
  (cancel [this ident]
    (let [root-ident               (get-cancel-root-ident @state* ident)
          initial-idents-to-cancel (reverse (get-ident-and-descendant-idents @state* root-ident))
          queues-to-refresh (loop [idents-to-cancel  initial-idents-to-cancel
                                   queues-to-refresh #{}]
                              (if (not (seq idents-to-cancel))
                                queues-to-refresh
                                (let [[ident-to-cancel & rest-idents-to-cancel] idents-to-cancel
                                      instance (get-pipeline-instance @state* ident-to-cancel)]
                                  (if instance
                                    (let [resumable       (:resumable instance)
                                          canceller       (get-in instance [:props :canceller])
                                          deferred-result (get-in instance [:props :deferred-result])
                                          queue-name      (get-resumable-queue-name resumable)]
                                      (swap! state* update-instance-state resumable ::cancelled)
                                      (close! canceller)
                                      (p/resolve! deferred-result ::cancelled)
                                      (swap! state* deregister-instance resumable)
                                      (recur rest-idents-to-cancel (conj queues-to-refresh queue-name)))
                                    (recur rest-idents-to-cancel queues-to-refresh)))))]
      (doseq [queue-name queues-to-refresh]
        (start-next-in-queue this queue-name))))
  (on-cancel [_ promise]
    (let [on-cancel (:on-cancel opts)]
      (on-cancel promise)))
  (cancel-all [this idents]
    (doseq [ident idents]
      (cancel this ident)))
  (get-active [this]
    (let [state @state*]
      (reduce-kv
       (fn [m k v]
         (let [idents (:queue v)]
           (if (seq idents)
             (let [info (reduce
                         (fn [acc ident]
                           (assoc acc ident {:config (get-in state [:pipelines k :config])
                                             :state (get-in state [:instances ident :state])
                                             :args (get-in state [:instances ident :resumable :args])
                                             :ident ident}))
                         {}
                         idents)]
               (assoc m k info))
             m)))
       {}
       (:queues state))))
  (stop! [this]
    (remove-watch state* ::watcher)
    (let [instances (:instances @state*)
          cancellable-idents
          (->> instances
               (filter (fn [[_ v]] (get-in v [:resumable :config :cancel-on-shutdown])))
               (map first))]
      (cancel-all this cancellable-idents)
      (reset! state* ::stopped))))

(defn default-transactor [transaction]
  (transaction))

(def default-opts
  {:transactor default-transactor
   :watcher (fn [& args])
   :error-reporter (if goog.DEBUG js/console.error identity)
   :on-cancel identity})

(defn start!
  ([context] (start! context nil nil))
  ([context pipelines] (start! context pipelines nil))
  ([context pipelines opts]
   (let [opts'  (merge default-opts opts)
         {:keys [watcher]} opts'
         state* (atom {:pipelines (into {} (map process-pipeline pipelines))})]
     (add-watch state* ::watcher watcher)
     (->PipelineRuntime context state* pipelines opts'))))