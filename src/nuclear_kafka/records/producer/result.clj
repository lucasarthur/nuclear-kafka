(ns nuclear-kafka.records.producer.result
  (:require
   [nuclear-kafka.util :refer [update-if-contains]]
   [nuclear-kafka.producer.record-metadata :refer [->record-metadata-map]]))

(defn exception [result]
  (.exception result))

(defn correlation-metadata [result]
  (.correlationMetadata result))

(defn record-metadata [result]
  (.recordMetadata result))

(defn ->result-map [result]
  (->> {:error (exception result)
        :correlation-metadata (correlation-metadata result)
        :record-metadata (record-metadata result)}
       (filter (comp some? val))
       (into {})
       (update-if-contains :record-metadata ->record-metadata-map)))
