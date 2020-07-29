# keechma/pipelines

[![Clojars Project](https://img.shields.io/clojars/v/keechma/pipelines.svg)](https://clojars.org/keechma/pipelines)

Keechma/pipelines library is a manager for asynchronous and concurrent ClojureScript code. **It is a part of the [Keechma/next](https://github.com/keechma/keechma-next) stack but is not dependent on Keechma/next and can be used in any ClojureScript codebase.** It uses no global state and has minimal dependencies.

## Motivation

The majority of single page apps communicate with the server in an asynchronous fashion. Each of these calls can fail at any time, can have timing issues, and in general, requires a lot of boilerplate code to make robust. In practice, this means that most of them will be implemented in a &quot;happy-go-lucky&quot; fashion without too much care about various failure scenarios. Each call also has an implied concurrent behavior, which is usually ignored and reserved only for a few exceptional cases like autocomplete or form submissions.

Keechma/pipelines library takes care of the asynchronous code management, allowing you to focus on your domain code.

## Features

Keechma/pipelines provides you with the following features:

- Cancellation semantics
- Concurrency helpers
  - use-existing - Reuse the current in-flight request if another one is started with the same arguments
  - restartable - Cancel the current in-flight request if another one is started
  - enqueued - Enqueue requests and execute them sequentially
  - dropping - Drop new requests while another one is in flight
  - keep-latest - Keep requests in flight and enqueue the last request, cancel everything else

Every app will usually have three types of functions in its domain layer:

1. Pure data processing functions
2. Functions that mutate application state
3. Functions that communicate with the &quot;outer&quot; world - usually with HTTP requests

Keechma/pipelines allows you to compose them, without any of them knowing the nature of other functions. This way, you can avoid &quot;coloring&quot; your functions ([https://journal.stuffwithstuff.com/2015/02/01/what-color-is-your-function/](https://journal.stuffwithstuff.com/2015/02/01/what-color-is-your-function/)) - each function cares only about its own concerns.

## Examples

Let's start with a trivial example. We will implement a pipeline that will increment its argument three times:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(def inc-3-pipeline (pipeline! [value ctx]
                      (inc value)
                      (inc value)
                      (inc value)))

(def pipelines
  {:inc-3 inc-3-pipeline})

(def runtime (start! {} pipelines))

(pipelines/invoke runtime :inc-3 0) ;; Returns 3
```

This is a lot of code that just calls `inc` three times. It gets better. For now, let's go through the code:

1. We define the pipeline with the help of the `pipeline!` macro
2. We define all pipelines available in the runtime
3. We create the runtime
4. We invoke the pipeline with the initial value of `0`

The pipeline will bind the return value of each form to its first argument (in this case, `value`). If the returned value is `nil`, it will ignore it, and retain the previous value (this is useful when you want to implement side-effects).

Let's make the example slightly more complicated, in this case we'll implement the `async-inc` function which will async the value after a timeout:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(defn async-inc [value]
  (p/create
    (fn [resolve _]
      (js/setTimeout #(resolve (inc value)) 100))))

(def inc-3-pipeline (pipeline! [value ctx]
                      (inc value)
                      (async-inc value)
                      (inc value)))

(def pipelines
  {:inc-3 inc-3-pipeline})

(def runtime (start! {} pipelines))

(pipelines/invoke runtime :inc-3 0) ;; Returns a promise which will resolve to 3
```

In this case, `async-inc` is called between the two synchronous `inc` calls, but the `inc` function doesn't know about it - pipeline runtime will ensure that promise is resolved before proceeding to the next step. 

Keechma/pipelines will always try to synchronously execute its body and will return promise only if one of the forms returns a promise.

### Mutating state

This is all cool, but real applications need to mutate the state somehow. In the next example, we'll update the state after each increment:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(defn pipeline-reset! [& args]
  (apply reset! args)
  nil)

(defn async-inc [value]
  (p/create
    (fn [resolve _]
      (js/setTimeout #(resolve (inc value)) 100))))

(def inc-3-pipeline (pipeline! [value {:keys [state*] :as ctx}]
                      (inc value)
                      (pipeline-reset! state* value)
                      (async-inc value)
                      (pipeline-reset! state* value)
                      (inc value)
                      (pipeline-reset! state* value)))

(def pipelines
  {:inc-3 inc-3-pipeline})

(def runtime (start! {:state* (atom nil)} pipelines))

(pipelines/invoke runtime :inc-3 0) ;; Returns a promise which will resolve to 3
```

`pipeline-reset!` function behaves like a normal `clojure.core/reset!` except it returns `nil`. Returning `nil` will ensure that the pipeline `value` is not changed. Keechma/pipelines provides you with the `reset!` and `swap!` functions that can be used inside pipelines - `keechma.pipelines.core.reset!` and `keechma.pipelines.core.swap!`.

### Handling errors

In the real world, functions can and will throw errors, and promises will sometimes be rejected. Keechma/pipelines provides you with a way to handle these cases:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))


(defn pipeline-reset! [& args]
  (apply reset! args)
  nil)

(defn async-inc [value]
  (p/create
    (fn [resolve _]
      (js/setTimeout #(resolve (inc value)) 100))))

(defn rejecting-promise []
  (p/create
    (fn [_ reject]
      (js/setTimeout #(reject (ex-info "Some Error" {})) 100))))

(def inc-3-pipeline (pipeline! [value {:keys [state*] :as ctx}]
                      (inc value)
                      (pipeline-reset! state* value)
                      (rejecting-promise)
                      (pipeline-reset! state* value)
                      (inc value)
                      (pipeline-reset! state* value)
                      (rescue! [error]
                        (inc value)
                        (pipeline-reset! state* error))))

(def pipelines
  {:inc-3 inc-3-pipeline})

(def runtime (start! {:state* (atom nil)} pipelines))

(pipelines/invoke runtime :inc-3 0) ;; Returns a promise that will resolve to 3
```

In this case, when pipeline encounters a rejected promise (or if a synchronous function throws), it will stop executing its main body, and switch to the `rescue!` block.

### Finally block

In some cases, you'll want to run some code regardless if the pipeline encountered an error or not. This can be used for any cleanup you might want to do.

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))


(defn pipeline-reset! [& args]
  (apply reset! args)
  nil)

(defn async-inc [value]
  (p/create
    (fn [resolve _]
      (js/setTimeout #(resolve (inc value)) 100))))

(defn rejecting-promise []
  (p/create
    (fn [_ reject]
      (js/setTimeout #(reject (ex-info "Some Error" {})) 100))))

(def inc-3-pipeline (pipeline! [value {:keys [state*] :as ctx}]
                      (inc value)
                      (pipeline-reset! state* value)
                      (rejecting-promise)
                      (pipeline-reset! state* value)
                      (inc value)
                      (pipeline-reset! state* value)
                      (rescue! [error]
                        (inc value)
                        (pipeline-reset! state* error))
                      (finally! [error]
                        ;; error might be nil
                        (inc value)
                        (js/console.log "Running in the end"))))

(def pipelines
  {:inc-3 inc-3-pipeline})

(def runtime (start! {:state* (atom nil)} pipelines))

(pipelines/invoke runtime :inc-3 0) ;; Returns a promise which will resolve to 3
```

### Composition

Pipelines can be composed with each other. This allows you to write small, focused pipelines that can be reused whenever needed.

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(defn pipeline-reset! [& args]
  (apply reset! args)
  nil)

(defn async-inc [value]
  (p/create
    (fn [resolve _]
      (js/setTimeout #(resolve (inc value)) 100))))

(def inc-2-pipeline (pipeline! [value _]
                      (async-inc value)
                      (inc value)))

(def inc-3-pipeline (pipeline! [value {:keys [state*] :as ctx}]
                      (inc value)
                      (pipeline-reset! state* value)
                      (async-inc)
                      (pipeline-reset! state* value)
                      inc-2-pipeline
                      (pipeline-reset! state* value)
                      (inc value)
                      (pipeline-reset! state* value)))

(def pipelines
  {:inc-3 inc-3-pipeline})

(def runtime (start! {:state* (atom nil)} pipelines))

(pipelines/invoke runtime :inc-3 0)                         ;; Returns a promise which will resolve to 5
```

In this case, we've placed the `inc-2-pipeline` inside the `inc-3-pipeline` body, and the runtime will run it as expected.

### Concurrency

_Concurrency helpers are inspired by the [ember-concurrency](http://ember-concurrency.com/docs/introduction/) library which has excellent [explanations and visualizations](http://ember-concurrency.com/docs/task-concurrency) of the implemented behaviors._

Keechma/pipelines provide the following behavior modifiers:

- `use-existing` - If there is an in-flight pipeline started with the same arguments, return its promise instead of starting a new one. It can be combined with any other concurrency behavior (`restartable`, `dropping`, `enqueued` and `keep-latest`)
- `restartable` - Cancel any running pipelines and start a new one
- `dropping` - Drop new request while another one is running
- `enqueued` - Enqueue requests and execute them sequentially
- `keep-latest` - Drop all intermediate requests, enqueue the last one
- `cancel-on-shutdown` - Should the pipeline be canceled when the runtime is shut down
- `set-queue` - Explicitly set the queue in which pipeline will run. It can accept a function and decide the queue name based on the arguments

#### Live search

Let's implement a very simple live search:

1. User is writing into an input field
2. 200ms after the last keypress, we want to trigger the request
3. If user starts typing again during the request, cancel the current request and start a new one

```clojure
(def live-search
  (-> (pipeline! [value {:keys [state*] :as ctx}]
        (p/delay 200)
        (server-api-request value)
        (pp/reset! state* value))
    pp/use-existing
    pp/restartable))
```

That's it. No boilerplate, no book-keeping.

**There is an extensive test suite, and you can consult it for more examples.**

#### Queue behavior

Each pipeline runs in a queue. By default, pipelines run as soon as they are invoked. This behavior is customizable through the concurrency helpers. **Pipelines retain their concurrency behavior and queue when they run inside another pipeline.** This means that you might have an outer pipeline that has a different concurrency behavior than the inner one.

When the pipeline is created (with the `pipeline!` macro) it will have an implicit queue name defined. When the pipeline is registered in the runtime, queue name will be updated (immutably) to the key under which the pipeline is registered. For example:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(def inc-pipeline (pipeline! [value {:keys [state*] :as ctx}]
                    (inc value)))

(def pipelines
  {:inc-1 inc-pipeline
   :inc-2 inc-pipeline})

(def runtime (start! {:state* (atom nil)} pipelines))
```

In this case, the same `inc-pipeline` is registered under `:inc-1` and `:inc-2` keys. These pipelines will run in their own queues (`:inc-1` and `:inc-2` queue). Original `inc-pipeline` still has its own implicit queue defined. Here's another example:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(def inc-pipeline (pipeline! [value {:keys [state*] :as ctx}]
                    (inc value)))

(def pipelines
  {:inc-1 (pipeline! [value ctx]
            inc-pipeline)
   :inc-2 (pipeline! [value ctx]
            inc-pipeline)})

(def runtime (start! {:state* (atom nil)} pipelines))
```

In this case `:inc-1` and `:inc-2` pipelines have its own queues, but they call `inc-pipeline` inside them, and for both cases when the `inc-pipeline` is called, it will run in the implicit `inc-pipeline` queue.

You can explicitly set the queue name with the `set-queue` helper. If you do this, this queue name will be retained in all cases.

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(def inc-pipeline (-> (pipeline! [value {:keys [state*] :as ctx}]
                        (inc value))
                    (pp/set-queue :my-explicit-queue)))

(def pipelines
  {:inc-1 inc-pipeline
   :inc-2 inc-pipeline
   :inc-3 (pipeline! [value ctx]
            inc-pipeline)})

(def runtime (start! {:state* (atom nil)} pipelines))
```

In this case, whenever and however the `inc-pipeline` is invoked it will always use the `:my-explicit-queue` queue. Concurrency behavior is enforced on the queue level, not on the pipeline level.

Sometimes you might have multiple pipelines that need to share a queue. You can set the same queue name for multiple pipelines, as long as they have identical concurrency behavior:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(defn async-inc [value]
  (p/create
    (fn [resolve _]
      (js/setTimeout #(resolve (inc value)) 100))))

(defn async-dec [value]
  (p/create
    (fn [resolve _]
      (js/setTimeout #(resolve (dec value)) 100))))

(def inc-pipeline (-> (pipeline! [value {:keys [state*] :as ctx}]
                        @state*
                        (async-inc value))
                    (pp/set-queue :my-explicit-queue)
                    pp/enqueued))

(def dec-pipeline (-> (pipeline! [value {:keys [state*] :as ctx}]
                        @state*
                        (async-dec value))
                    (pp/set-queue :my-explicit-queue)
                    pp/enqueued))

(def pipelines
  {:inc inc-pipeline
   :dec dec-pipeline})


(def runtime (start! {:state* (atom nil)} pipelines))

(invoke runtime :inc)
(invoke runtime :dec)
```

In this example, we're reading the _current_ value from the state. If these pipelines were running independently, you would probably have timing issues, because the state might change during the execution. By setting the same queue for both pipelines, we're ensuring that they will run sequentially.

In some cases, you will have pipelines that can work on a set of items. For instance, you could have an `:upvote` pipeline that will implement upvote for articles. In this case, you want to use a queue per article. It doesn't make sense to have a global queue where upvoting one article is blocking you from upvoting other articles. To implement this, you can pass a function to `set-queue`. This function will be called before the pipeline runs and you can return a different queue each time:

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))


(def upvote-pipeline (-> (pipeline! [value {:keys [state*] :as ctx}]
                           (upvote-article-by-id (:id value)))
                    (pp/set-queue (fn [{:keys [id]}] [:upvote id]))
                    pp/enqueued))

(def pipelines
  {:upvote upvote-pipeline})

(def runtime (start! {:state* (atom nil)} pipelines))

(invoke runtime :upvote {:id 1 :title "My first article"})
(invoke runtime :upvote {:id 2 :title "My second article"})
```

In this case, article 1 and article 2 will use separate queues.

### Stopping the runtime

Keechma/pipelines runtime can be stopped. When the runtime is stopped, all running pipelines will be canceled.

```clojure
(ns my-app.example
  (:require 
    [keechma.pipelines.core :as pp :refer [start! stop! invoke] :refer-macros [pipeline!]]
    [promesa.core :as p]))

(def inc-3-pipeline (pipeline! [value ctx]
                      (inc value)
                      (inc value)
                      (inc value)))

(def pipelines
  {:inc-3 inc-3-pipeline})

(def runtime (start! {} pipelines))

(pipelines/invoke runtime :inc-3 0) ;; Returns 3

(stop! runtime) ;; Stops the runtime and cancels all running pipelines 
```

## Conclusion

Keechma/pipelines is a very simple library, that brings a lot of value to the table. You can start using them today, they don't require a big commitment and will not affect the rest of your code. If you have any feedback, please reach out to us in the #keechma channel on the clojurians slack.

## License

Copyright © 2020 Mihael Konjevic, Tibor Kranjcec

Distributed under the MIT License.
