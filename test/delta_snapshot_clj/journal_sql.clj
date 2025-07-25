(ns ^{:doc "Delta Snapshot library journal-sql"
      :author "Clay Lipscomb"}
 delta-snapshot-clj.journal-sql
  (:require
   [taoensso.timbre
    :refer [info]]
   [delta-snapshot-clj.journal :as jrnl]
   [next.jdbc :as jdbc]
   [next.jdbc.specs :as jdbc-specs]
   [next.jdbc.sql :as sql]
   [next.jdbc.result-set :as result-set])
  (:import
   (clojure.lang PersistentHashMap)
   [java.time Instant]
   (clojure.lang IReduceInit)
   [java.sql Timestamp]
   [java.time OffsetDateTime]))

(jdbc-specs/instrument)

; Used in the result set returned to a subscriber for a snapshot
(defrecord EntityDeltaMessage 
           [id
            ^String            delta
            ^Instant           inst
            ^PersistentHashMap cur
            ^PersistentHashMap prv])

; configuration
(extend-protocol result-set/ReadableColumn
  Timestamp
  (read-column-by-label [^Timestamp v _]
    (.toInstant v))
  (read-column-by-index [^Timestamp v _2 _3]
    (.toInstant v))
  OffsetDateTime
  (read-column-by-label [^OffsetDateTime v _]
    (.toInstant v))
  (read-column-by-index [^OffsetDateTime v _2 _3]
    (.toInstant v)))

; connection & transaction
(def opts jdbc/unqualified-snake-kebab-opts)
(def ds-opts
  (jdbc/with-options
    (jdbc/get-datasource {:dbtype "h2:mem" :dbname "journal"})
    opts))
(defn get-datasource []
  ds-opts)
(defn get-connection []
  (jdbc/with-options
    (jdbc/get-connection (get-datasource))
    opts))
(defn execute-as-transaction [conn f]
  (jdbc/with-transaction+options [tx conn opts] (f tx)))
;(defn execute-as-transaction-old [conn f]
;  (jdbc/with-transaction [tx conn] (f tx)))

; testing DDL
(defn drop-table
  []
  (jdbc/execute! ds-opts ["DROP TABLE journal"]))

(defn create-table-if-mot-exists
  []
  (do
    (jdbc/execute! ds-opts ["
      CREATE TABLE IF NOT EXISTS journal (
         id INT AUTO_INCREMENT PRIMARY KEY,
         subscription_data_set_id INT NOT NULL,
         snapshot_id INT NOT NULL,
         inserted_on_full BOOLEAN NOT NULL,
         entity_id int NOT NULL,
         entity_delta_code VARCHAR(3) NOT NULL,
         entity_delta_inst TIMESTAMP WITH TIME ZONE NOT NULL,
         entity_cur JAVA_OBJECT NULL,
         entity_prv JAVA_OBJECT NULL)"])))

(defn create-or-truncate-table
  []
  (do
    (create-table-if-mot-exists)
    (jdbc/execute! ds-opts ["TRUNCATE TABLE journal"])))

;; `JournalOperation` methods
(defn table-row-entity-id-val
  "Gets entity id value from a table row."
  [table-row]
  (:entity-id table-row))

(defn row->EntityDeltaMessage
  "Converts a 'table-row' to a delta message record"
  [table-row]
  (some->
   table-row
   ((fn [row] (->EntityDeltaMessage
               (:entity-id row)
               (:entity-delta-code row)
               (:entity-delta-inst row)
               (:entity-cur row)
               (:entity-prv row))))))

(defn row->JournalRow
  "Converts a 'table-row' to an delta-snapshot JournalRow record"
  [table-row]
  (some->
   table-row
   ((fn [row] (jrnl/->JournalRow
               (:subscription-data-set-id row)
               (:snapshot-id row)
               (:inserted-on-full row)
               (:entity-id row)
               (:entity-delta-code row)
               (:entity-delta-inst row)
               (:entity-cur row)
               (:entity-prv row))))))

(defn insert-tx!
  [transactable-conn journal-row]
  (jdbc/execute-one!
   transactable-conn
   ["INSERT INTO journal
      (subscription_data_set_id, snapshot_id, inserted_on_full, entity_id,
      entity_delta_code, entity_delta_inst, entity_cur, entity_prv)
      VALUES (?,?,?,?,?,?,?,?)"
    (:subscription-data-set-id journal-row) (:snapshot-id journal-row) (:inserted-on-full journal-row) (:entity-id journal-row)
    (:entity-delta-code journal-row) (:entity-delta-inst journal-row) (:entity-cur journal-row) (:entity-prv journal-row)]
   {:return-keys true}))

(defn retrieve-newest-data-set-entity-by-id
  [transactable-conn
   subscription-data-set-id
   entity-id]
  (jdbc/execute-one!
   transactable-conn
   ["SELECT * FROM journal
      WHERE subscription_data_set_id = ? AND entity_id = ?
      ORDER BY entity_delta_inst DESC
      FETCH FIRST ROW ONLY"
    subscription-data-set-id entity-id]))

(defn retrieve-snapshot
  [conn
   subscription-data-set-id
   snapshot-id]
  (jdbc/plan
   conn
   ["SELECT * FROM journal
      WHERE snapshot_id = ? 
        AND subscription_data_set_id = ?" ; safety predicate to prevent retrieving another subscrber's snapshot
    snapshot-id
    subscription-data-set-id]))

;(defn retrieve-snapshot-records
;  [conn
;   snapshot-id]
;  (->> (sql/query ; TODO use plan
;        conn
;        ["SELECT * FROM journal
;           WHERE snapshot_id = ?"
;         snapshot-id])
;       (map row->JournalRow)))

;(defn retrieve-snapshot-messages
;  [conn
;   snapshot-id]
;  (->> (sql/query ; TODO use plan
;        conn
;        ["SELECT * FROM journal
;           WHERE snapshot_id = ?"
;         snapshot-id])
;       (map row->EntityDeltaMessage)))

(defn retrieve-newest-entities-excl-delta
  "Note: The '<' in 'entity_delta_code < ?' is often more efficient that !=. The < works
   because add and modify codes will be less than the remove code."
  ^IReduceInit
  [conn
   subscription-data-set-id
   exclude-delta-code]
  (let [_ (info "journal-sql.retrieve-newest-entities-excl-delta")]
    (jdbc/plan
     conn
     ["SELECT * FROM journal j1
        WHERE subscription_data_set_id = ?
          AND entity_delta_code < ?
          AND entity_delta_inst = (SELECT MAX(entity_delta_inst) FROM journal j2 WHERE j2.entity_id = j1.entity_id)"
      subscription-data-set-id exclude-delta-code])))

(defn retrieve-newest-entity-ids-excl-delta
  ^IReduceInit
  [transactable-conn
   subscription-data-set-id
   exclude-delta-code]
  (jdbc/plan
   transactable-conn
   ["SELECT entity_id FROM journal j1
       WHERE subscription_data_set_id = ?
         AND entity_delta_code < ?
         AND entity_delta_inst = (SELECT MAX(entity_delta_inst) FROM journal j2 WHERE j2.entity_id = j1.entity_id)"
    subscription-data-set-id exclude-delta-code]))

;; testing DML
(defn select-all-JournalRow
  []
  (->>
   (sql/query
    ds-opts
    ["SELECT * FROM journal"])
   (map row->JournalRow)))

(defn select-all-EntityDeltaMessages
  []
  (->>
   (sql/query
    ds-opts
    ["SELECT * FROM journal"])
   (map row->EntityDeltaMessage)))

(defn select-all
  []
  (->>
   (sql/query
    ds-opts
    ["SELECT * FROM journal"])))

(defn retrieve-count-by-entity
  [conn
   subscription-data-set-id
   entity-id]
  (->
   (jdbc/execute-one!
    conn
    ["SELECT count(*) FROM journal
        WHERE subscription_data_set_id = ? AND entity_id = ?"
     subscription-data-set-id entity-id])
   ((keyword "count(*)"))))

(defn insert!
  [journal-row]
  (jdbc/execute-one!
   ds-opts
   ["INSERT INTO journal
      (subscription_data_set_id, snapshot_id, inserted_on_full, entity_id,
      entity_delta_code, entity_delta_inst, entity_cur, entity_prv)
      VALUES (?,?,?,?,?,?,?,?)"
    (:subscription-data-set-id journal-row) (:snapshot-id journal-row) (:inserted-on-full journal-row) (:entity-id journal-row)
    (:entity-delta-code journal-row) (:entity-delta-inst journal-row) (:entity-cur journal-row) (:entity-prv journal-row)]
   {:return-keys true}))