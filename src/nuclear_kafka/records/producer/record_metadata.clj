(ns nuclear-kafka.records.producer.record-metadata
  (:refer-clojure :exclude [partition]))

(defn has-offset? [metadata]
  (.hasOffset metadata))

(defn has-timestamp? [metadata]
  (.hasTimestamp metadata))

(defn offset [metadata]
  (.offset metadata))

(defn timestamp [metadata]
  (.timestamp metadata))

(defn serialized-key-size [metadata]
  (.serializedKeySize metadata))

(defn serialized-value-size [metadata]
  (.serializedValueSize metadata))

(defn topic [metadata]
  (.topic metadata))

(defn partition [metadata]
  (.partition metadata))

(defn ->record-metadata-map [metadata]
  {:serialized-key-size (serialized-key-size metadata)
   :serialized-value-size (serialized-value-size metadata)
   :topic (topic metadata)
   :partition (partition metadata)
   :offset (offset metadata)
   :timestamp (timestamp metadata)})
