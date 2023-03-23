(ns nuclear-kafka.transaction-manager
  (:require
   [nuclear-kafka.records.topic-partition :refer [->topic-partition]]
   [nuclear.util.sam :refer [->consumer ->function]])
  (:import
   (org.apache.kafka.clients.consumer OffsetAndMetadata ConsumerGroupMetadata)
   (java.util Optional)))

(defn ->offset-and-metadata
  [{:keys [offset leader-epoch metadata]}]
  (let [le-opt (-> (Optional/ofNullable leader-epoch)
                   (.map (->function #(.intValue %))))]
    (OffsetAndMetadata. offset le-opt metadata)))

(defn ->consumer-group-metadata
  [{:keys [group-id gen-id member-id group-instance-id]}]
  (ConsumerGroupMetadata. group-id gen-id member-id (Optional/ofNullable group-instance-id)))

(defn begin-tx [txm]
  (.begin txm))

(defn commit-tx [txm]
  (.commit txm))

(defn abort-tx [txm]
  (.abort txm))

(defn tx-scheduler [txm]
  (.scheduler txm))

(defn on-tx-complete! [consumer txm]
  (->> consumer ->consumer (.transactionComplete txm)))

(defn send-offsets [tpms cgm txm]
  (.sendOffsets
   txm
   (->> (partition 3 tpms)
        (map (fn [tpm]
               [(->topic-partition (first tpm) (second tpm))
                (-> tpm last ->offset-and-metadata)]))
        (into {}))
   (->consumer-group-metadata cgm)))
