;; Copyright (c) 2023 Lucas Arthur
;;
;; This file is part of Nuclear Kafka, an open-source library whose goal is to
;; use Reactor Kafka with Clojure in an idiomatic and simplified way.
;;
;; Nuclear Kafka is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.
;;
;; Nuclear Kafka is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with Nuclear Kafka. If not, see <http://www.gnu.org/licenses/>.

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
