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

(ns nuclear-kafka.config.common
  (:require
   [clojure.string :as str :refer [lower-case join]]
   [nuclear-kafka.security :refer [protocols mechanisms]]))

(defn- kv->bootstrap-servers [brokers]
  (->> (partition 2 brokers)
       (map #(str (first %) ":" (second %)))
       (join ",")))

(defn- kw->kafka-cfg [kw]
  (-> kw name lower-case (str/replace #"-" ".")))

(defn- ->jaas-config-str [module username password]
  (str module " required username=\"" username "\" password=\"" password "\";"))

(defn- ->kafka-jaas-cfg-map
  [{:keys [username
           password
           mechanism]}]
    {"sasl.mechanism" (mechanisms mechanism :name)
     "sasl.jaas.config" (-> mechanism mechanisms :module (->jaas-config-str username password))})

(defn ->kafka-cfg-map
  [{:keys [brokers
           protocol
           auth
           options]
    :or {brokers ["localhost" 9092]}}]
  (let [brokers-map {"bootstrap.servers" (kv->bootstrap-servers brokers)}
        protocol-map (if protocol {"security.protocol" (protocol protocols)} {})
        auth-map (if-not (empty? auth) (->kafka-jaas-cfg-map auth) {})
        options-map (update-keys options #(if (keyword? %) (kw->kafka-cfg %) %))]
    (merge options-map auth-map protocol-map brokers-map)))
