(ns nuclear-kafka.records.headers
  (:refer-clojure :exclude [key remove])
  (:import
   (org.apache.kafka.common.header.internals RecordHeader RecordHeaders)))

(defn ->record-header
  ([[k v]] (->record-header k v))
  ([k v] (RecordHeader. (name k) (.getBytes v "UTF-8"))))

(defn key [header]
  (.key header))

(defn value [header]
  (.value header))

(defn ->header-map [header]
  {:key (key header)
   :value (-> header value (String. "UTF-8"))})

(defn add [header headers]
  (->> header ->record-header (.add headers)))

(defn remove [key headers]
  (.remove headers (name key)))

(defn last-header [key headers]
  (->> key name (.lastHeader headers) ->header-map))

(defn header-seq [key headers]
  (->> key name (.headers headers) (into []) (map ->header-map)))

(defn headers-seq [headers]
  (->> (into [] headers) (map ->header-map)))

(defn ->record-headers [headers]
  (reduce #(add %2 %1) (RecordHeaders.) headers))

(defn ->record-header-seq [header-map]
  (when (seq header-map) (map ->record-header header-map)))
