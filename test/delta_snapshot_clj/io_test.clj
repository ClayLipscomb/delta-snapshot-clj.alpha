(ns ^{:doc "Delta Snapshot library IO test."
      :author "Clay Lipscomb"}
 delta-snapshot-clj.io-test
  (:require
   [delta-snapshot-clj.journal :as jrnl]
   [delta-snapshot-clj.subscription :as sub]
   [delta-snapshot-clj.core :refer [delta-code-remove delta-code-add delta-code-modify]]
   [delta-snapshot-clj.io :refer [snapshot! full! retrieve-snapshot]]
   [delta-snapshot-clj.journal-sql :as journal-sql]
   [clojure.core.reducers :as r]
   [clojure.test :refer [deftest testing are is]]
   [clojure.spec.test.alpha :as stest])
  (:import
   (clojure.lang Keyword PersistentHashMap IReduceInit)
   [java.time Instant]))

(stest/instrument) ; instrument delta-snapshot-clj.io

(def logging-min-level
  (atom :report))
(defn get-logging-min-level []
  @logging-min-level)
(defn set-logging-min-level [^Keyword kw]
  (reset! kw {}))

(def ^:const subscription-data-set-id-test 77)
(def ^:const entity-id-simple 14)
(def ^:const entity-id-simple2 15)
(def entity-id-keyword :some-qualifier/id)

(def ^:const entity-simple-v1 {entity-id-keyword entity-id-simple :some-string "A"})
(def ^:const entity-simple-v2 {entity-id-keyword entity-id-simple :some-string "B"})
(def ^:const entity-simple-v3 {entity-id-keyword entity-id-simple :some-string "C"})
(def ^:const entity-simple2 {entity-id-keyword entity-id-simple2 :some-string "other"})

(def ^:const snapshot-failure-kw :snapshot-failure)
(def ^:const snapshot-success-kw :snapshot-success-return-all-remove-deltas)

(defn- =test ^Boolean [x y] (= x y))

(def gen-new-snapshot-id
  (let [snapshot-id-counter (atom 0)]
    (fn [] (swap! snapshot-id-counter inc))))

;; publisher data set
(def pub-data-set-for-sub
  (atom {}))
(defn- pub-data-set-for-sub-count []
  (count @pub-data-set-for-sub))
(defn- pull-pub-data-set-for-sub ^IReduceInit []
  ;(println "printing pull-pub-data-set-for-sub")
  (vec (vals @pub-data-set-for-sub)))
(defn- truncate-pub-data-set-for-sub []
  (reset! pub-data-set-for-sub {}))
(defn- put-pub-data-set-for-sub [^PersistentHashMap entry]
  (swap! pub-data-set-for-sub assoc (entity-id-keyword entry) entry))
(defn- add-pub-data-set-for-sub [id ^PersistentHashMap entry]
  (swap! pub-data-set-for-sub assoc id entry))
(defn- rmv-pub-data-set-for-sub [entity-id]
  (swap! pub-data-set-for-sub dissoc entity-id))

;; journal protocol
(defn- make-journal-operation
  [conn]
  (reify jrnl/JournalOperation
    ;; mappers
    (table-row-entity-id-val [_ table-row]
      (journal-sql/table-row-entity-id-val table-row))
    (table-row->row-record [_ table-row]
      (journal-sql/row->JournalRow table-row))
    (table-row->message [_ table-row]
      (journal-sql/row->EntityDeltaMessage table-row))
    ;; write operations
    (insert-row! [_ transactable-conn journal-row]
      (journal-sql/insert-tx! transactable-conn journal-row))
    ;; read operations
    (now [_]
      (Instant/now))
    (generate-new-snapshot-id [_]
      (gen-new-snapshot-id))
    (retrieve-newest-data-set-entity-by-id [_ transactable-conn subscription-data-set-id entity-id]
      (journal-sql/retrieve-newest-data-set-entity-by-id transactable-conn subscription-data-set-id entity-id))
    (retrieve-data-set-snapshot [_ subscription-data-set-id snapshot-id]
      (journal-sql/retrieve-snapshot conn subscription-data-set-id snapshot-id))
    (retrieve-newest-data-set-entities-excluding-delta [_ subscription-data-set-id exclude-delta-code]
      (journal-sql/retrieve-newest-entities-excl-delta conn subscription-data-set-id exclude-delta-code))
    (retrieve-newest-data-set-entity-ids-excluding-delta [_ transactable-conn subscription-data-set-id exclude-delta-code]
      (journal-sql/retrieve-newest-entity-ids-excl-delta transactable-conn subscription-data-set-id exclude-delta-code))))

;; subscription config protocol
(defn- make-subscription-config
  [& {:keys [empty-publisher-data-set-strategy]
      :or {empty-publisher-data-set-strategy snapshot-failure-kw}}]
  (reify sub/SubscriptionConfig
    (subscription-data-set-id [_]
      subscription-data-set-id-test)
    (publisher-data-set-entity-id-keyword [_]
      entity-id-keyword)
    (pull-publisher-data-set [_]
      (pull-pub-data-set-for-sub))
    (empty-publisher-data-set-strategy [_]
      empty-publisher-data-set-strategy)
    (entities-equal? [_ entity1 entity2]
      (=test entity1 entity2))))

;; journal maint
(defn- retrieve-count-by-entity [conn subscription-data-set-id entity-id]
  (journal-sql/retrieve-count-by-entity conn subscription-data-set-id entity-id))

(defn- truncate-journal []
  (journal-sql/create-or-truncate-table))

;; API operations
(defn- io-snapshot!
  [& {:keys [empty-publisher-data-set-strategy] :or {empty-publisher-data-set-strategy snapshot-failure-kw}}]
  (let [conn (journal-sql/get-connection)
        journal-operation (make-journal-operation conn)
        sub-config (make-subscription-config {:empty-publisher-data-set-strategy empty-publisher-data-set-strategy})
        snapshot-response (journal-sql/execute-as-transaction
                           conn
                           (fn [transactable-conn]
                             (snapshot! journal-operation sub-config transactable-conn {:logging-min-level (get-logging-min-level)})))]
    snapshot-response))

(defn- io-full!
  [& {:keys [empty-publisher-data-set-strategy] :or {empty-publisher-data-set-strategy snapshot-failure-kw}}]
  (let [conn (journal-sql/get-connection)
        journal-operation (make-journal-operation conn)
        sub-config (make-subscription-config {:empty-publisher-data-set-strategy empty-publisher-data-set-strategy})
        snapshot-response (journal-sql/execute-as-transaction
                           conn
                           (fn [transactable-conn]
                             (full! journal-operation sub-config transactable-conn {:logging-min-level (get-logging-min-level)})))]
    snapshot-response))

(defn- io-retrieve-snapshot
  [subscription-data-set-id snapshot-id]
  (let [conn (journal-sql/get-connection)
        journal-operation (make-journal-operation conn)]
    (retrieve-snapshot journal-operation subscription-data-set-id snapshot-id)))

(defn- io-retrieve-full
  [subscription-data-set-id]
  (let [conn (journal-sql/get-connection)
        journal-operation (make-journal-operation conn)]
    (#'delta-snapshot-clj.io/retrieve-full journal-operation subscription-data-set-id)))

;; (defn- io-generate-snapshot-opt-defaults!
;;   []
;;   (let [conn (journal/get-connection)
;;         journal-io (make-journal-io conn)
;;         snapshot-result (journal/execute-as-transaction
;;                           conn
;;                           (fn [transactable-conn]
;;                             (generate-snapshot!
;;                               journal-io
;;                               subscription-config
;;                               subscription-data-set-id-test
;;                               pull-pub-data-set-for-sub
;;                               (gen-new-snapshot-id)
;;                               transactable-conn)))]
;;     snapshot-result))

;(defn ds-generate-run-full! []
;  (let [run-id-new (gen-new-snapshot-id)
;        conn (journal/get-connection)]
;    (journal/execute-as-transaction
;      conn
;      (fn [transactable-conn]
;        (let [journal-io (make-journal-io conn)]
;          (generate-run!
;            journal-io
;            subscription-data-set-id-test
;            pull-pub-data-set-for-sub
;            run-id-new
;            transactable-conn
;            :return-full-data-set true
;            :fn-subscription-entities-equal? =test
;            :empty-publisher-data-set-strategy :run-success-return-all-remove-deltas
;            :logging-min-level (get-logging-min-level)))))))

;(defn- ds-retrieve-historical-run [run-id]
;  (let [journal-io (make-journal-io (journal/get-connection))]
;    (api-sub/retrieve-historical-run
;      journal-io
;      run-id)))

;(defn- ds-retrieve-newest-entity-by-id [subscription-data-set-id entity-id]
;  (let [conn (journal/get-connection)
;        journal-io (make-journal-io conn)]
;    (journal/execute-as-transaction
;      conn
;      (fn [transactable-conn]
;        (retrieve-newest-data-set-entity-by-id journal-io transactable-conn subscription-data-set-id entity-id)))))

(defn- truncate-pub-data-set-and-journal
  "Truncate pub data set and journal."
  []
  (do
    (truncate-pub-data-set-for-sub)
    (truncate-journal)))

(defn- init-pub-data-set-and-journal
  "Initialize pub data set and journal with one 'other' entity not directly used in testing."
  []
  (do
    (truncate-pub-data-set-and-journal)
    (put-pub-data-set-for-sub entity-simple2)
    (io-snapshot!)))

;; TESTS
(deftest test-snapshot!-pub-data-set-empty-failure
  (testing "snapshot! with empty publisher data set and :snapshot-failure fails with exception"
    (is (thrown-with-msg?
         Exception #"Publisher data set is empty or unavailable."
         (do
           (truncate-pub-data-set-and-journal)
           (io-snapshot! :empty-publisher-data-set-strategy snapshot-failure-kw))))))

(deftest test-snapshot!-pub-data-set-empty-success
  (testing "snapshot! with empty publisher data set and :snapshot-success-return-all-remove-deltas succeeds"
    (is (= (do
             (truncate-pub-data-set-and-journal)
             (io-snapshot! :empty-publisher-data-set-strategy snapshot-success-kw)
             nil)
           nil))))

;; Handled by spec
;(deftest test-generate-snapshot-pub-data-set-nil-entity-id-failure
;  (testing "generate-snapshot with nil entity id in the publisher data set fails with exception"
;    (is (thrown-with-msg?
;          ;ExecutionException #"Nil entity found in publisher data set." ; ExecutionException due to pmap Futures
;          Exception #"Entity with nil id found in publisher data set."
;          (do
;            (truncate-pub-data-set-for-sub)
;            (add-pub-data-set-for-sub nil entity-nil-id)
;            (ds-generate-snapshot!))))))

(deftest test-snapshot!-full!-single-delta-permutations
  (let [conn (journal-sql/get-connection)
        journal-count (fn [] (retrieve-count-by-entity conn subscription-data-set-id-test entity-id-simple))
        first-msg-by-id (fn [id coll] (->> coll (filter #(= (:id %) id)) (first)))
        _ (init-pub-data-set-and-journal)]

    (testing "snapshot! add"
      (let [snapshot-response (do (put-pub-data-set-for-sub entity-simple-v1)
                                  (io-snapshot!))
            {:keys [subscription-data-set-id snapshot-id]} snapshot-response
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-snapshot subscription-data-set-id snapshot-id))))
        (is (= 1 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 1 (:delta-add-count snapshot-response)))
        (is (= 0 (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 1 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple)
          entity-simple-v1 (:cur entity-message-simple)
          nil (:prv entity-message-simple))))

    (testing "full! after: snapshot! add"
      (let [snapshot-response (io-full!)
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)
            entity-message-simple2 (first-msg-by-id entity-id-simple2 delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-full (:subscription-data-set-id snapshot-response)))))
        (is (= 1 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 2 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple2)
          entity-simple2 (:cur entity-message-simple2)
          nil (:prv entity-message-simple2))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple)
          entity-simple-v1 (:cur entity-message-simple)
          nil (:prv entity-message-simple))))

    (testing "snapshot! remove following add"
      (let [snapshot-response (do (rmv-pub-data-set-for-sub entity-id-simple)
                                  (io-snapshot!))
            {:keys [subscription-data-set-id snapshot-id]} snapshot-response
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-snapshot subscription-data-set-id snapshot-id))))
        (is (= 2 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response)))
        (is (= 1 (:delta-remove-count snapshot-response)))
        (is (= 1 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-remove (:delta entity-message-simple)
          nil (:cur entity-message-simple)
          entity-simple-v1 (:prv entity-message-simple))))

    (testing "full! after: snapshot! remove following add"
      (let [snapshot-response (io-full!)
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple2 (first-msg-by-id entity-id-simple2 delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-full (:subscription-data-set-id snapshot-response)))))
        (is (= 2 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 1 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple2)
          entity-simple2 (:cur entity-message-simple2)
          nil (:prv entity-message-simple2))))

    (testing "snapshot! re-add following remove"
      (let [snapshot-response (do (put-pub-data-set-for-sub entity-simple-v1)
                                  (io-snapshot!))
            {:keys [subscription-data-set-id snapshot-id]} snapshot-response
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-snapshot subscription-data-set-id snapshot-id))))
        (is (= 3 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 1 (:delta-add-count snapshot-response)))
        (is (= 0 (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 1 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple)
          entity-simple-v1 (:cur entity-message-simple)
          nil (:prv entity-message-simple))))

    (testing "full! after: snapshot! re-add following remove"
      (let [snapshot-response (io-full!)
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)
            entity-message-simple2 (first-msg-by-id entity-id-simple2 delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-full (:subscription-data-set-id snapshot-response)))))
        (is (= 3 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 2 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple2)
          (:cur entity-message-simple2) entity-simple2
          nil (:prv entity-message-simple2))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple)
          entity-simple-v1 (:cur entity-message-simple)
          nil (:prv entity-message-simple))))

    (testing "snapshot! modify following add"
      (let [snapshot-response (do (put-pub-data-set-for-sub entity-simple-v2)
                                  (io-snapshot!))
            {:keys [subscription-data-set-id snapshot-id]} snapshot-response
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-snapshot subscription-data-set-id snapshot-id))))
        (is (= 4 (journal-count)))
        (is (= 1 (count delta-messages)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 1 (:delta-modify-count snapshot-response)))
        (are [x y] (= x y)
          delta-code-modify (:delta entity-message-simple)
          entity-simple-v2 (:cur entity-message-simple)
          entity-simple-v1 (:prv entity-message-simple))))

    (testing "full! after: snapshot! modify following add"
      (let [snapshot-response (io-full!)
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)
            entity-message-simple2 (first-msg-by-id entity-id-simple2 delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-full (:subscription-data-set-id snapshot-response)))))
        (is (= 4 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 2 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple2)
          (:cur entity-message-simple2) entity-simple2
          nil (:prv entity-message-simple2))
        (are [x y] (= x y)
          delta-code-modify (:delta entity-message-simple)
          entity-simple-v2 (:cur entity-message-simple)
          entity-simple-v1 (:prv entity-message-simple))))

    (testing "snapshot! modify following modify"
      (let [snapshot-response (do (put-pub-data-set-for-sub entity-simple-v3)
                                  (io-snapshot!))
            {:keys [subscription-data-set-id snapshot-id]} snapshot-response
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-snapshot subscription-data-set-id snapshot-id))))
        (is (= 5 (journal-count)))
        (is (= 1 (count delta-messages)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 1 (:delta-modify-count snapshot-response)))
        (are [x y] (= x y)
          delta-code-modify (:delta entity-message-simple)
          entity-simple-v3 (:cur entity-message-simple)
          entity-simple-v2 (:prv entity-message-simple))))

    (testing "full! after: snapshot! modify following modify"
      (let [snapshot-response (io-full!)
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)
            entity-message-simple2 (first-msg-by-id entity-id-simple2 delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-full (:subscription-data-set-id snapshot-response)))))
        (is (= 5 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 2 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple2)
          (:cur entity-message-simple2) entity-simple2
          nil (:prv entity-message-simple2))
        (are [x y] (= x y)
          delta-code-modify (:delta entity-message-simple)
          entity-simple-v3 (:cur entity-message-simple)
          entity-simple-v2 (:prv entity-message-simple))))

    (testing "snapshot! no delta following modify"
      (let [snapshot-response (io-snapshot!)
            {:keys [subscription-data-set-id snapshot-id]} snapshot-response
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))]
        (is (= delta-messages (r/foldcat (io-retrieve-snapshot subscription-data-set-id snapshot-id))))
        (is (= 5 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 0 (count delta-messages)))))

    (testing "full! after: snapshot! no delta following modify"
      (let [snapshot-response (io-full!)
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)
            entity-message-simple2 (first-msg-by-id entity-id-simple2 delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-full (:subscription-data-set-id snapshot-response)))))
        (is (= 5 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 2 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple2)
          (:cur entity-message-simple2) entity-simple2
          nil (:prv entity-message-simple2))
        (are [x y] (= x y)
          delta-code-modify (:delta entity-message-simple)
          entity-simple-v3 (:cur entity-message-simple)
          entity-simple-v2 (:prv entity-message-simple))))

    (testing "snapshot! remove following modify"
      (let [snapshot-response (do (rmv-pub-data-set-for-sub entity-id-simple)
                                  (io-snapshot!))
            {:keys [subscription-data-set-id snapshot-id]} snapshot-response
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple (first-msg-by-id entity-id-simple delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-snapshot subscription-data-set-id snapshot-id))))
        (is (= 6 (journal-count)))
        (is (= 1 (count delta-messages)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response)))
        (is (= 1 (:delta-remove-count snapshot-response)))
        (are [x y] (= x y)
          delta-code-remove (:delta entity-message-simple)
          nil (:cur entity-message-simple)
          entity-simple-v3 (:prv entity-message-simple))))

    (testing "full! after: snapshot! remove following modify"
      (let [snapshot-response (io-full!)
            delta-messages (r/foldcat (:delta-messages-reducible snapshot-response))
            entity-message-simple2 (first-msg-by-id entity-id-simple2 delta-messages)]
        (is (= delta-messages (r/foldcat (io-retrieve-full (:subscription-data-set-id snapshot-response)))))
        (is (= 6 (journal-count)))
        (is (= (pub-data-set-for-sub-count) (:data-set-count snapshot-response)))
        (is (= 0 (:delta-add-count snapshot-response) (:delta-modify-count snapshot-response) (:delta-remove-count snapshot-response)))
        (is (= 1 (count delta-messages)))
        (are [x y] (= x y)
          delta-code-add (:delta entity-message-simple2)
          (:cur entity-message-simple2) entity-simple2
          nil (:prv entity-message-simple2))))))

(clojure.test/run-tests)