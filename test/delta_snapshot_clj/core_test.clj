(ns ^{:doc "Delta Snapshot library core-test."
      :author "Clay Lipscomb"}
 delta-snapshot-clj.core-test
  (:require
   [delta-snapshot-clj.journal :as jrnl]
   [delta-snapshot-clj.core :as core :refer [delta-code-remove delta-code-add delta-code-modify
                                             snapshot-failure-due-to-empty-data-set? contrast->add-mod-row
                                             ->SnapshotRequest ->ProcessedResult combine-ProcessedResult]]
   [clojure.test :refer [deftest testing are is]]
   [clojure.spec.test.alpha :as stest])
  (:import
   (clojure.lang Keyword)
   [java.time Instant]))

(stest/instrument) ; instrument delta-snapshot.core

(def logging-min-level
  (atom :report))
(defn get-logging-min-level []
  @logging-min-level)
(defn set-logging-min-level [^Keyword kw]
  (reset! kw {}))

(def ^:const subscription-data-set-id-test 77)
(def ^:const entity-id-keyword :some-qualifier/id)
(def ^:const entity-id-simple 14)
(def ^:const entity-simple-v1 {entity-id-keyword entity-id-simple :some-string "A"})
(def ^:const entity-simple-v2 {entity-id-keyword entity-id-simple :some-string "B"})
(def ^:const entity-simple-v3 {entity-id-keyword entity-id-simple :some-string "C"})

(def ^:const snapshot-failure-kw :snapshot-failure)
(def ^:const snapshot-success-kw :snapshot-success-return-all-remove-deltas)

(defn- =test [x y] (= x y))

(def gen-new-snapshot-id
  (let [snapshot-id-counter (atom 0)]
    (fn [] (swap! snapshot-id-counter inc))))

;; subscription snapshot request
(defn- make-subscription-snapshot-request-simple []
  (->SnapshotRequest
   (gen-new-snapshot-id)
   subscription-data-set-id-test
   (fn [] [])
   false
   =test
   snapshot-failure-kw
   (get-logging-min-level)))

(defn- make-journal-row-test [delta-code entity-cur entity-prv]
  (jrnl/->JournalRow
   subscription-data-set-id-test
   (gen-new-snapshot-id)
   false
   entity-id-simple
   delta-code
   (Instant/now)
    ;#inst "2023-01-12T23:20:50.52Z"
   entity-cur
   entity-prv))

(defn make-journal-row-simple-add []
  (make-journal-row-test delta-code-add entity-simple-v1 nil))
(defn make-journal-row-simple-modify []
  (make-journal-row-test delta-code-modify entity-simple-v2 entity-simple-v1))
(defn make-journal-row-simple-remove []
  (make-journal-row-test delta-code-remove nil entity-simple-v1))

;; TESTS
;; (deftest test-JournalRow->add
;;   (let [f #'core/JournalRow->add]
;;     (testing "true"
;;       (are [x] (= true x)
;;                ;(f snapshot-failure-kw nil)
;;         (f snapshot-failure-kw #{})))
;;                ;(f snapshot-failure-kw [])))
;;     (testing "false"
;;       (are [x] (= false x)
;;         (f snapshot-failure-kw #{1})))))
;;                ;(f snapshot-success-kw nil)
;;                ;(f snapshot-success-kw ())))))

(deftest test-snapshot-failure-due-to-empty-data-set?
  (let [f #'snapshot-failure-due-to-empty-data-set?]
    (testing "true"
      (are [x] (= true x)
               ;(f snapshot-failure-kw nil)
        (f snapshot-failure-kw #{})))
               ;(f snapshot-failure-kw [])))
    (testing "false"
      (are [x] (= false x)
        (f snapshot-failure-kw #{1})))))
               ;(f snapshot-success-kw nil)
               ;(f snapshot-success-kw ())))))

(deftest test-contrast->add-mod-row
  (let [inst (Instant/now)
        {:keys [subscription-data-set-id snapshot-id return-full-data-set] :as srr} (make-subscription-snapshot-request-simple)
        entity-v1 entity-simple-v1
        entity-v2 entity-simple-v2
        entity-v3 entity-simple-v3
        id entity-id-simple
        contrast (partial #'contrast->add-mod-row srr inst)]
    (testing "initial add"
      (is (= (contrast entity-v1 id nil)
             (jrnl/->JournalRow subscription-data-set-id snapshot-id return-full-data-set id delta-code-add inst entity-v1 nil))))
    (testing "re-add"
      (is (= (contrast entity-v2 id (make-journal-row-simple-remove))
             (jrnl/->JournalRow subscription-data-set-id snapshot-id return-full-data-set id delta-code-add inst entity-v2 nil))))
    (testing "modify - following add"
      (is (= (contrast entity-v2 id (make-journal-row-simple-add))
             (jrnl/->JournalRow subscription-data-set-id snapshot-id return-full-data-set id delta-code-modify inst entity-v2 entity-v1))))
    (testing "modify - following modify"
      (is (= (contrast entity-v3 id (make-journal-row-simple-modify))
             (jrnl/->JournalRow subscription-data-set-id snapshot-id return-full-data-set id delta-code-modify inst entity-v3 entity-v2))))
    (testing "no change"
      (is (= (contrast entity-v1 id (make-journal-row-simple-add))
             nil)))))

(deftest test-combine-ProcessedResult-duplicate-ids-failure
  (let [id 99]
   (testing "monoid-ProcessedResult with duplicate ids fails with exception"
     (is (= ["Entity duplicate id found in publisher data set: " {:id id}]
            (try
              (combine-ProcessedResult
               (->ProcessedResult 0 0 #{1 id})
               (->ProcessedResult 0 0 #{2 id}))
              (catch Exception ex
                [(ex-message ex) (ex-data ex)])))))))

(deftest test-combine-ProcessedResult-identity
  (testing "monoid-ProcessedResult with no args returns ProcessedResult identity"
    (is (= (->ProcessedResult 0 0 #{})
           (combine-ProcessedResult)))))

; TODO
;(deftest test-row->add)
;(deftest test-row->modify)
;(deftest test-row->remove)
;(deftest test-entity->row-add)
;(deftest test-row->message)
;(deftest test-make-snapshot-result)

(clojure.test/run-tests)