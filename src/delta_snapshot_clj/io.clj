(ns ^{:doc "Delta Snapshot library I/O."
      :author "Clay Lipscomb"}
 delta-snapshot-clj.io
  (:require
   [delta-snapshot-clj.journal :as jrnl]
   [delta-snapshot-clj.subscription :as sub]
   [delta-snapshot-clj.core
    :as core
    :refer [exists-in-data-set? snapshot-failure-due-to-empty-data-set? mk-journal-snapshot-crud combine-ProcessedResult
            JournalRow->remove contrast->add-mod-row mk-snapshot-request mk-snapshot-response mk-counts-map
            ->ProcessedResult delta-code-add delta-code-modify delta-code-remove]]
   [taoensso.timbre :refer [trace debug info fatal set-min-level!]]
   [clojure.core.reducers :as r])
  (:import
   (clojure.lang PersistentHashMap IReduceInit)
   (delta_snapshot_clj.core SnapshotRequest SnapshotResponse JournalSnapshotCrud ProcessedResult)
   (delta_snapshot_clj.subscription SubscriptionConfig)
   (delta_snapshot_clj.journal JournalOperation)
   [java.time Instant]))

(defn- persist-entity-remove!
  "Retrieve entity's newest journal row, convert to Remove and insert into journal. Return nil."
  [^SnapshotRequest     snapshot-request
   ^JournalSnapshotCrud journal-journal-crud
   entity-id]
  (let [_log                  (debug "persist-entity-remove!" "entity-id:" entity-id)
        retrieve-newest       #((:fn-retrieve-newest-entity journal-journal-crud) %)
        convert-row-to-remove #(JournalRow->remove snapshot-request (Instant/now) %)
        insert-row!           #((:fn-insert! journal-journal-crud) %)]
    (do
      (->> (retrieve-newest entity-id)
           (convert-row-to-remove)
           (insert-row!)))))

(defn- sweep-data-set-removes!
  "Retrieve all current (i.e., Add or Modify) journal entity ids, ignoring ones found in `publisher-entity-ids` 
   of `processed-result`, and insert a corresponding Remove row into journal. Return map with `:delta-remove-count`."
  ^PersistentHashMap
  [^SnapshotRequest     snapshot-request
   ^JournalSnapshotCrud journal-snapshot-crud
   ^ProcessedResult     {:keys [publisher-entity-ids] :as processed-result}]
  (let [_log           (info "sweep-data-set-removes!")
        _log           (info processed-result)
        entity-exists? (fn [entity-id] (exists-in-data-set? publisher-entity-ids entity-id))
        insert-remove! (fn [entity-id] (persist-entity-remove! snapshot-request journal-snapshot-crud entity-id) 1)
        make-count-map (fn [delta-remove-count] {:delta-remove-count delta-remove-count})]
    (->> ((:fn-retrieve-full-ids journal-snapshot-crud))
         (r/map (:fn-table-row-entity-id-val journal-snapshot-crud))
         (r/remove entity-exists?)
         (r/map insert-remove!)
         (r/fold +) ; count the removes
         (make-count-map))))

(defn- process-entity-add-modify!
  "Evaluate `entity` against journal for add or modify and insert a corresponding row to journal if necessary. 
   Return `ProcessedResult`."
  ^ProcessedResult
  [^SnapshotRequest     snapshot-request
   ^JournalSnapshotCrud journal-snapshot-crud
   ^SubscriptionConfig  subscriptonConfig
   ^PersistentHashMap   {entity-id (sub/publisher-data-set-entity-id-keyword subscriptonConfig) :as entity}]
  (let [_log             (debug "process-entity-add-modify!")
        _log             (trace "entity:" entity)
        _                (when (nil? entity-id) (throw (ex-info "Entity with nil id found in publisher data set." {})))
        contrast->row    #(contrast->add-mod-row snapshot-request ((:fn-now journal-snapshot-crud)) entity entity-id %)
        retrieve-newest  #((:fn-retrieve-newest-entity journal-snapshot-crud) %)
        insert-if-row!   (fn [journal-row]
                           (when journal-row ((:fn-insert! journal-snapshot-crud) journal-row))
                           journal-row) ; always pass through row!
        calc-delta-count (fn [journal-row delta-code] (if (= (:entity-delta-code journal-row) delta-code) 1 0))
        build-result     (fn [journal-row] (->ProcessedResult (calc-delta-count journal-row delta-code-add)
                                                              (calc-delta-count journal-row delta-code-modify)
                                                              #{entity-id}))]
    (->> (retrieve-newest entity-id)
         (contrast->row)
         (insert-if-row!)
         (build-result))))

(defn- retrieve-full
  "Return IReduceInit that will retrieve the delta messages of a subscription's full data set from the journal. These 
   messages represent all entities that are currently in the publisher data set as of the last generated snapshot. 
   The delta codes in the full data set will be either Add or Modify. However, in the context of a retrieving a full,
   this delta code simply means the entity currently exists in the data set and not that it was just added or modified.
   
    `journal-operation` - instance of `JournalOperation` protocol reified by consumer
    `subscription-data-set-id` - identifies subscription data set to retrieved
    `logging-min-level` - Minimum log level of :trace, :debug, :info, :warn, :error, or :fatal (defaults to :fatal)"
  ^IReduceInit
  ([journal-operation
    subscription-data-set-id]
   (retrieve-full journal-operation subscription-data-set-id :fatal))
  ([journal-operation
    subscription-data-set-id
    logging-min-level]
   {:pre [(some? subscription-data-set-id)
          (keyword? logging-min-level)]}
   (let [_    (set-min-level! logging-min-level)
         _log (info "retrieve-full" "subscription-data-set-id:" subscription-data-set-id)]
     (->> (jrnl/retrieve-newest-data-set-entities-excluding-delta journal-operation subscription-data-set-id delta-code-remove)
          (r/map (fn [table-row] (jrnl/table-row->message journal-operation table-row)))))))

(defn retrieve-snapshot
  "Return IReduceInit that will retrieve the delta messages of a previously generated snapshot from the journal.
   
    `journal-operation` - instance of `JournalOperation` protocol reified by consumer
    `subscription-data-set-id` - identifies subscription data set of snapshot
    `snapshot-id` - identifies snapshot to be retrieved
    `logging-min-level` - Minimum log level of :trace, :debug, :info, :warn, :error, or :fatal (defaults to :fatal)"
  ^IReduceInit
  ([journal-operation
    subscription-data-set-id
    snapshot-id]
   (retrieve-snapshot journal-operation subscription-data-set-id snapshot-id :fatal))
  ([journal-operation
    subscription-data-set-id
    snapshot-id
    logging-min-level]
   {:pre [(some? subscription-data-set-id)
          (some? snapshot-id)
          (keyword? logging-min-level)]}
   (let [_    (set-min-level! logging-min-level)
         _log (info "retrieve-snapshot" "subscription-data-set-id:" subscription-data-set-id "snapshot-id:" snapshot-id)]
     (->> (jrnl/retrieve-data-set-snapshot journal-operation subscription-data-set-id snapshot-id)
          (r/map (fn [table-row] (jrnl/table-row->message journal-operation table-row)))))))

(def ^:const opts-defaults
  "Default values for the `opts` parameter in `generate-snapshot!`."
  {:logging-min-level :fatal})

(defn- mk-delta-messages-reducible
  "Return IReduceInit for either a `retrieve-full` or `retrieve-snapshot` from the journal."
  ^IReduceInit
  [                 journal-operation
   ^SnapshotRequest {:keys [subscription-data-set-id snapshot-id]}
                    return-full-data-set
                    {:keys [logging-min-level]}]
  (if return-full-data-set
    (retrieve-full journal-operation subscription-data-set-id logging-min-level)
    (retrieve-snapshot journal-operation subscription-data-set-id snapshot-id logging-min-level)))

(defn- generate-snapshot!
  "Generate and persist a snapshot for a subscription. Return `SnapshotResponse` that holds reducible
   of deltas for either the snapshot or the full data set."
  ^SnapshotResponse
  [^JournalOperation   journal-operation
   ^SubscriptionConfig subscription-config   
                       transactable-conn
   ^Boolean            return-full-data-set
                       opts]
  {:pre  [(some? transactable-conn)
          (boolean? return-full-data-set)
          (map? opts)]
   :post [(instance? SnapshotResponse %)]}
  (let [opts-defaulted         (merge opts-defaults opts)
        _                      (set-min-level! (:logging-min-level opts-defaulted))
        _log                   (info "generate-snapshot!" "return-full-data-set" return-full-data-set)
        snapshot-request       (mk-snapshot-request
                                (jrnl/generate-new-snapshot-id journal-operation)
                                subscription-config
                                return-full-data-set
                                (merge opts-defaults opts-defaulted))
        _log                   (info snapshot-request)
        journal-snapshot-crud  (mk-journal-snapshot-crud 
                                transactable-conn 
                                journal-operation 
                                (:subscription-data-set-id snapshot-request))        
        pub-data-set-reducible ((:fn-pull-pub-data-set-for-sub snapshot-request))
        validate-empty         #(if (snapshot-failure-due-to-empty-data-set?
                                     (:empty-publisher-data-set-strategy snapshot-request) 
                                     (:publisher-entity-ids %))
                                  (throw (ex-info "Publisher data set is empty or unavailable." (into {} snapshot-request)))
                                  %)
        process-entity!        #(process-entity-add-modify! snapshot-request journal-snapshot-crud subscription-config %)
        make-counts            (fn [result delta-remove-count-m]
                                 (info "generate-snapshot!.make-counts" ":delta-remove-count" (:delta-remove-count delta-remove-count-m))
                                 (mk-counts-map result delta-remove-count-m))        
        sweep-and-count        (fn [processed-result]
                                 (->> (sweep-data-set-removes! snapshot-request journal-snapshot-crud processed-result)
                                      (make-counts processed-result)))
        build-response         (fn [counts] 
                                 (mk-snapshot-response
                                  snapshot-request
                                  counts
                                  (mk-delta-messages-reducible journal-operation snapshot-request return-full-data-set opts-defaulted)))]
    (try
      (->> pub-data-set-reducible
           (r/map process-entity!)
           (r/fold combine-ProcessedResult)
           (validate-empty)
           (sweep-and-count)
           (build-response))
      (catch Exception ex
        (fatal "Exception:" (.getMessage ex))
        (fatal snapshot-request)
        (throw ex)))))

(defn snapshot!
  "Generates a new snapshot, persists it to the journal, and returns a `SnapshotResponse` with a `delta-messages-reducible` 
   that retrieves the delta messages of the new snapshot.

  `journal-operation` - instance of `JournalOperation` protocol reified by consumer
  `subscription-config` - instance of `SubscriptionConfig` protocol reified by consumer
  `transactable-conn` - a transactable connection (or container thereof) to be sent to `JournalOperation` methods with such parameter
  `opts` - map of the following optional parameters:
      `:logging-min-level` -
          Minimum log level of :trace, :debug, :info, :warn, :error, or :fatal (defaults to :fatal)"
  ([journal-operation subscription-config transactable-conn]
   (snapshot! journal-operation subscription-config transactable-conn opts-defaults))
  ([journal-operation subscription-config transactable-conn opts]   
   (generate-snapshot! journal-operation subscription-config transactable-conn false opts)))

(defn full!
  "Generates a new snapshot, persists it to the journal, and returns a `SnapshotResponse` with a `delta-messages-reducible` 
   that retrieves the delta messages of the full data set.

  `journal-operation` - instance of `JournalOperation` protocol reified by consumer
  `subscription-config` - instance of `SubscriptionConfig` protocol reified by consumer
  `transactable-conn` - a transactable connection (or container thereof) to be sent to `JournalOperation` methods with such parameter
  `opts` - map of the following optional parameters:
      `:logging-min-level` -
          Minimum log level of :trace, :debug, :info, :warn, :error, or :fatal (defaults to :fatal)"
  ([journal-operation subscription-config transactable-conn]
   (snapshot! journal-operation subscription-config transactable-conn opts-defaults))
  ([journal-operation subscription-config transactable-conn opts]
   (generate-snapshot! journal-operation subscription-config transactable-conn true opts)))