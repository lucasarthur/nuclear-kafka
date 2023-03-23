(ns nuclear-kafka.outbound
  (:require
   [nuclear-kafka.records.shape :refer [producer-shape]]
   [nuclear.protocols :as p]
   [nuclear.core :as nk]
   [nuclear.mono :as mono])
  (:import
   (reactor.kafka.sender KafkaOutbound)
   (org.reactivestreams Subscriber)))

(defn ->outbound [sender]
  (assoc sender :outbound (-> sender :sender .createOutbound)))

(defn send-many
  [records {:keys [shape] :as outbound}]
  (update outbound :outbound
          (fn [out]
            (->> (nk/map #(producer-shape shape %) records) (.send out)))))

(defn send-one [record outbound]
  (-> (mono/->flux record) (send-many outbound)))

(defn tx-send
  [tx-records {:keys [shape] :as outbound}]
  (update outbound :outbound
          (fn [out]
            (->> (nk/map
                  (fn [windows]
                    (nk/map #(producer-shape shape %) windows)) tx-records)
                 (.sendTransactionally out)))))

(defn then
  ([outbound]
   (update outbound :outbound #(.then %)))
  ([other outbound]
   (update outbound :outbound #(.then % other))))

(defn unwrap [{:keys [outbound]}] outbound)

(extend-type KafkaOutbound
  p/SubscribeOperator
  (-subscribe [outbound on-next on-error on-complete on-subscribe]
    (.subscribe
     outbound
     (reify Subscriber
       (onSubscribe [_ subscription] (on-subscribe subscription))
       (onNext [_ value] (on-next value))
       (onError [_ error] (on-error error))
       (onComplete [_] (on-complete))))))
