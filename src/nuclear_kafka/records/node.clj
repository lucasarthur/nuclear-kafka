(ns nuclear-kafka.records.node
  (:refer-clojure :exclude [empty?])
  (:import
   (org.apache.kafka.common Node)))

(def no-node (Node/noNode))

(defn empty? [node]
  (.isEmpty node))

(defn has-rack? [node]
  (.hasRack node))

(defn id [node]
  (.id node))

(defn id-str [node]
  (.idString node))

(defn host [node]
  (.host node))

(defn port [node]
  (.port node))

(defn rack [node]
  (.rack node))

(defn node->map [node]
  (->> {:id (id node)
        :id-str (id-str node)
        :host (host node)
        :port (port node)
        :rack (rack node)}
       (filter (comp some? val))
       (into {})))
