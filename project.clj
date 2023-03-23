(defproject nuclear-kafka "0.0.1"
  :description "A Clojure wrapper with batteries for Reactor Kafka"
  :url "https://github.com/lucasarthur/nuclear-kafka"
  :license {:name "GNU General Public License v3.0"
            :url "https://www.gnu.org/licenses/gpl-3.0.pt-br.html"}
  :main ^:skip-aot nuclear-kafka.core
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [cheshire "5.11.0"]
                 [nuclear "0.2.0"]
                 [io.projectreactor.kafka/reactor-kafka "1.3.17"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "1.7.36"]]}})
