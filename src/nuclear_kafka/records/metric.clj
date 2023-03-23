(ns nuclear-kafka.records.metric)

(defn metric-name->map [mn]
  {:name (.name mn)
   :group (.group mn)
   :description (.description mn)
   :tags (->> mn .tags (into {}))})

(defn metric->map [m]
  {:metric-name (-> m .metricName metric-name->map)
   :metric-value (.metricValue m)})
