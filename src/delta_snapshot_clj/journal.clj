(ns ^{:doc "Delta Snapshot library journal."
      :author "Clay Lipscomb"}
 delta-snapshot-clj.journal
  (:import
   (clojure.lang PersistentHashMap IReduceInit)
   [java.time Instant]))

(defrecord JournalRow ; The minimum columns for a row that is inserted into the journal table, excluding the primary key.
 [                   subscription-data-set-id
                     snapshot-id
  ^boolean           inserted-on-full
                     entity-id
  ^String            entity-delta-code
  ^Instant           entity-delta-inst
  ^PersistentHashMap entity-cur
  ^PersistentHashMap entity-prv])

(defprotocol JournalOperation
  "All I/O operations for journal, that is hosted  the consumer of delta-snapshot."
  ;; mappers
  (table-row-entity-id-val
    [this table-row]
    "Given a row native to journal table, return the entity id value.")
  (table-row->row-record
    ^JournalRow [this table-row]
    "Map a row native to journal table to a delta-snapshot `JournalRow` record via its constructor `->JournalRow`.")
  (table-row->message
    [this table-row]
    "Map a row native to journal table to an equivalent message (record recommended) to be consumed by subscribers.
     The message should be limited to entity id, delta code, instant, current and previous.")
  ;; write operations
  (insert-row!
    [this transactable-conn ^JournalRow journal-row]
    "Given a `JournalRow` record, insert into journal within a transaction created for generating a snapshot.")
  ;; read operations
  (now
    ^Instant [this]
    "Return Instant value of the journal's 'now' (current date & time).")
  (generate-new-snapshot-id
    [this]
    "Generate id for a new snapshot id. A snapshot id must be unique across all subscription data sets.")
  (retrieve-newest-data-set-entity-by-id
    [this transactable-conn subscription-data-set-id entity-id]
    "Retrieve row native to journal table with the newest version of a data set entity within a transaction created 
     for generating a snapshot.")
  (retrieve-data-set-snapshot
    ^IReduceInit [this subscription-data-set-id snapshot-id]
    "Return IReduceInit that will retrieve rows of a specific data set snapshot from the journal. Used to return an 
     existing snapshot's deltas to the subscriber.")
  (retrieve-newest-data-set-entities-excluding-delta
    ^IReduceInit [this subscription-data-set-id exclude-delta-code]
    "Return IReduceInit that will retrieve rows from the journal that are the newest version of each entity in a 
     data set, excluding those entities where the newest version is given delta state. Used to retrieve a 'full' data set.")
  (retrieve-newest-data-set-entity-ids-excluding-delta
    ^IReduceInit [this transactable-conn subscription-data-set-id exclude-delta-code]
    "Return IReduceInit that will retrieve entity ids from the journal that are the newest version of each entity in a 
     data set, excluding those entities where the newest version is given delta state. Executed within a transaction 
     created for generating a snapshot."))