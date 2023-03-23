(ns nuclear-kafka.util)

(defn assoc-if [should? k v coll]
  (if should? (assoc coll k v) coll))

(defn assoc-if-not-nil [k v coll]
  (assoc-if (not (nil? v)) k v coll))

(defn update-if-contains [k f m]
  (into m (for [[key v] (select-keys m [k])] [key (f v)])))
