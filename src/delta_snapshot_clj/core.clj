(ns ^{:doc "Delta Snapshot library core."
      :author "Clay Lipscomb"}
  delta-snapshot-clj.core
  (:require
   [clojure.core.reducers :as r]
   [clojure.set :refer [union intersection]]
   [delta-snapshot-clj.journal :as jrnl]
   [delta-snapshot-clj.subscription :as sub])
  (:import
    (clojure.lang Keyword PersistentHashMap PersistentHashSet IFn IReduceInit)
    (delta_snapshot_clj.journal JournalRow)
    [java.time Instant]))

;; Possible deltas. TODO change to character?
(def ^:const delta-code-add
  "ADD signifies a new entity was added to the data set."
  "ADD")
(def ^:const delta-code-modify
  "MOD signifies an existing entity in the data set was modified."
  "MOD")
(def ^:const delta-code-remove
  "RMV signifies an entity was removed from the data set."
  "RMV")

(def empty-publisher-data-set-strategies
  "Strategies available to consumer that handle the scenario of an empty publisher data set while generating a snapshot"
  #{
    ;; Snapshot will be successful, resulting in Remove deltas being generated for all journaled rows.
    :snapshot-success-return-all-remove-deltas
    ;; Snapshot will fail and throw exception; journal transaction should be rolled back.
    :snapshot-failure})
(def ^:const empty-publisher-data-set-strategy-default :snapshot-failure)

(defrecord SnapshotRequest ; A request to generate a snapshot for a subscription data set
  [         snapshot-id
            subscription-data-set-id
   ^IFn     fn-pull-pub-data-set-for-sub ;; "Function to pull reducible of entire subscription data set from the publisher system."
   ^boolean return-full-data-set
   ^IFn     fn-subscription-entities-equal?
   ^Keyword empty-publisher-data-set-strategy
   ^Keyword logging-min-level])

(defrecord ^:private SnapshotResponse  ; Response of generating snapshot, including use by full request 
  [            snapshot-id
               subscription-data-set-id
  ^boolean     return-full-data-set
  ^IReduceInit delta-messages-reducible
  ^long        data-set-count
  ^long        delta-add-count
  ^long        delta-modify-count
  ^long        delta-remove-count])

(defrecord ProcessedResult  ; Summary of processing entities (for add or modify) in publisher data set
  [^long              delta-add-count
   ^long              delta-modify-count
   ^PersistentHashSet publisher-entity-ids])

(defrecord ^:private JournalSnapshotCrud ; Contains journal CRUD related to generating snapshot. TODO rename JournalSnapshotOperation
           [^IFn fn-insert!
            ^IFn fn-table-row-entity-id-val
            ^IFn fn-now
            ^IFn fn-retrieve-newest-entity
            ^IFn fn-retrieve-full-ids])

(defn mk-counts-map
  "Create a map of all snapshot counts from a `ProcessedResult` and a map containing a `:delta-remove-count`."
  ^PersistentHashMap
  [^ProcessedResult   processed-result
   ^PersistentHashMap delta-remove-count-m]
  {:data-set-count     (count (:publisher-entity-ids processed-result))
   :delta-add-count    (:delta-add-count processed-result)
   :delta-modify-count (:delta-modify-count processed-result)
   :delta-remove-count (:delta-remove-count delta-remove-count-m)})

(defn mk-snapshot-request
  "Construct a `SnapshotRequest`."
  ^SnapshotRequest
  [snapshot-id
   subscription-config
   return-full-data-set
   opts]
  (let [{:keys [logging-min-level]} opts]
    (->SnapshotRequest
     snapshot-id
     (sub/subscription-data-set-id subscription-config)
     (partial sub/pull-publisher-data-set subscription-config)
     return-full-data-set
     (partial sub/entities-equal? subscription-config)
     (or (sub/empty-publisher-data-set-strategy subscription-config)
         empty-publisher-data-set-strategy-default)
     logging-min-level)))

(defn mk-snapshot-response
  "Construct a `SnapshotResponse`."
  ^SnapshotResponse
  [^SnapshotRequest {:keys [snapshot-id subscription-data-set-id return-full-data-set]}
                    {:keys [data-set-count delta-add-count delta-modify-count delta-remove-count]}
                    delta-messages-reducible]
  (->SnapshotResponse
   snapshot-id
   subscription-data-set-id
   return-full-data-set
   delta-messages-reducible
   data-set-count
   delta-add-count
   delta-modify-count
   delta-remove-count))

(defn- JournalRow->add
  "Converts a journal row to an Add using an entity. The existing row must be a Remove, and have the same entity id as the entity."
  ^JournalRow
  [^SnapshotRequest   {:keys [snapshot-id return-full-data-set]}
   ^Instant           inst
   ^PersistentHashMap entity-new
   ^JournalRow        journal-row]
  (assoc journal-row
    :snapshot-id snapshot-id
    :inserted-on-full return-full-data-set
    :entity-delta-code delta-code-add
    :entity-delta-inst inst
    :entity-cur entity-new
    :entity-prv nil)) ; just like a new add, a re-add should have no previous entity

(defn- JournalRow->modify
  "Converts a journal row to a Modify using an entity. The existing row must be an Add, and have the same entity id as the entity."
  ^JournalRow
  [^SnapshotRequest   {:keys [snapshot-id return-full-data-set]}
   ^Instant           inst
   ^PersistentHashMap entity-new
   ^JournalRow        journal-row]
  (assoc journal-row
    :snapshot-id snapshot-id
    :inserted-on-full return-full-data-set
    :entity-delta-code delta-code-modify
    :entity-delta-inst inst
    :entity-cur entity-new
    :entity-prv (:entity-cur journal-row)))

(defn JournalRow->remove
  "Converts a journal row to a Remove. The existing row must be an Add or Modify."
  ^JournalRow
  [^SnapshotRequest  {:keys [snapshot-id return-full-data-set]}
   ^Instant          inst
   ^JournalRow       journal-row]
  (assoc journal-row
    :snapshot-id snapshot-id
    :inserted-on-full return-full-data-set
    :entity-delta-code delta-code-remove
    :entity-delta-inst inst
    :entity-cur nil
    :entity-prv (:entity-cur journal-row)))

(defn- entity->JournalRow-add
  "Create a journal row as an Add from an entity for a data set snapshot. The journal primary key will not be set since it is generated by consumer."
  ^JournalRow
  [^SnapshotRequest   {:keys [subscription-data-set-id snapshot-id return-full-data-set]}
   ^Instant           inst
   ^PersistentHashMap entity-new
                      entity-id]
  (jrnl/->JournalRow
    subscription-data-set-id
    snapshot-id
    return-full-data-set
    entity-id
    delta-code-add
    inst
    entity-new
    nil))

(defn mk-journal-snapshot-crud
  "Create record of functions for available journal CRUD operations, partially applied when possible."
  ^JournalSnapshotCrud
  [transactable-conn
   journal-operation
   subscription-data-set-id]
  (->> {:fn-insert!                 (fn [journal-row]
                                      ;(debug "JournalOperation.insert-row!" "entity id:" (:entity-id journal-row) "entity-delta-code:" (:entity-delta-code journal-row))
                                      (jrnl/insert-row! journal-operation transactable-conn journal-row))
        :fn-table-row-entity-id-val (fn [table-row]
                                      (jrnl/table-row-entity-id-val journal-operation table-row))
        :fn-now                     (fn []
                                      (jrnl/now journal-operation))
        :fn-retrieve-newest-entity  (fn [entity-id]
                                      ;(debug "JournalOperation.retrieve-newest-data-set-entity-by-id" "subscription-data-set-id:" subscription-data-set-id "entity-id:" entity-id)
                                      (some-> (jrnl/retrieve-newest-data-set-entity-by-id journal-operation transactable-conn subscription-data-set-id entity-id)
                                              (#(jrnl/table-row->row-record journal-operation %))))
        :fn-retrieve-full-ids       (fn []
                                      ;(info "JournalOperation.retrieve-newest-data-set-entity-ids-excluding-delta" "subscription-data-set-id:" subscription-data-set-id "exclude-delta-code:" delta-code-remove)
                                      (jrnl/retrieve-newest-data-set-entity-ids-excluding-delta journal-operation transactable-conn subscription-data-set-id delta-code-remove))}
       (map->JournalSnapshotCrud)))

(defn snapshot-failure-due-to-empty-data-set?
  "Determine if the snapshot should be considered a failure due to a empty data set."
  ^Boolean
  [^Keyword           empty-publisher-data-set-strategy
   ^PersistentHashSet publisher-entity-ids]
  {:pre [(empty-publisher-data-set-strategies empty-publisher-data-set-strategy)
         (set? publisher-entity-ids)]
   :post [(boolean? %)]}
  (and
    (= empty-publisher-data-set-strategy :snapshot-failure)
    (empty? publisher-entity-ids)))

(defn exists-in-data-set?
  "Determine if an entity exists in the publisher data set."
  ^Boolean
  [^PersistentHashSet publisher-entity-ids
   entity-id]
  (contains? publisher-entity-ids entity-id))

(defn contrast->add-mod-row
  "Determine if a data set entity for a snapshot should result in an Add, Modify or no delta
  by contrasting it against its respective journal row, if one is provided.
  1. If an Add or Modify is determined, return a new, transformed journal row to be inserted.
  2. If there is no delta, return nil."
  ^JournalRow
  [^SnapshotRequest   {:keys [fn-subscription-entities-equal?] :as subscription-snapshot-request}
   ^Instant           inst
   ^PersistentHashMap data-set-entity
                      entity-id
   ^JournalRow        journal-row]
  (letfn [(add? []    (not journal-row)) ;; no row found in journal means first ADD
          (re-add? [] (= delta-code-remove (:entity-delta-code journal-row))) ;; existing RMV row means re-ADD
          (modify? [] (not (fn-subscription-entities-equal? (:entity-cur journal-row) data-set-entity)))]
    (cond
      (add?) (entity->JournalRow-add subscription-snapshot-request inst data-set-entity entity-id)
      (re-add?) (JournalRow->add subscription-snapshot-request inst data-set-entity journal-row)
      (modify?) (JournalRow->modify subscription-snapshot-request inst data-set-entity journal-row)
      :else nil))) ;; entities in journal and data set are equal, so bypass

(def combine-ProcessedResult
  "Monoidal combine function used to fold collection of `ProcessedResult` in parallel."
  (r/monoid
   (fn [^ProcessedResult result1 ^ProcessedResult result2]
     (->ProcessedResult
      (+ (:delta-add-count result1) (:delta-add-count result2))
      (+ (:delta-modify-count result1) (:delta-modify-count result2))
      (let [publisher-entity-ids1 (:publisher-entity-ids result1)
            publisher-entity-ids2 (:publisher-entity-ids result2)
            duplicates            (intersection publisher-entity-ids1 publisher-entity-ids2)]
        (or (when (not-empty duplicates)
              (throw (ex-info "Entity duplicate id found in publisher data set: " {:id (first duplicates)})))
            (union publisher-entity-ids1 publisher-entity-ids2)))))
   (constantly (->ProcessedResult 0 0 #{}))))