(ns nuclear-kafka.records.producer.sender-record
  (:refer-clojure :exclude [key partition])
  (:require
   [nuclear-kafka.records.headers :refer [->record-header-seq]])
  (:import
   (org.apache.kafka.clients.producer ProducerRecord)
   (reactor.kafka.sender SenderRecord)))

(defn ->sender-record
  [{:keys [topic key value headers partition timestamp correlation-metadata]}]
  (-> (ProducerRecord. topic partition timestamp key value (->record-header-seq headers))
      (SenderRecord/create correlation-metadata)))

(defn topic [record]
  (.topic record))

(defn headers [record]
  (into [] (.headers record)))

(defn key [record]
  (.key record))

(defn value [record]
  (.value record))

(defn timestamp [record]
  (.timestamp record))

(defn partition [record]
  (.partition record))

(defn correlation-metadata [record]
  (.correlationMetadata record))
