(ns ^{:doc "Delta Snapshot library Specs."
      :author "Clay Lipscomb"}
 delta-snapshot-clj.specs
  (:require
  ;[clojure.spec.test.check :as stestcheck]
  ;[clojure.test.check.generators :as gen]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as gen] ;[clojure.spec.test.check :as stestcheck]
   [clojure.spec.test.alpha :as stest]
   [clojure.test.check.properties]
   [delta-snapshot-clj.core :as core :refer :all]
   [delta-snapshot-clj.io :as io]
   [delta-snapshot-clj.journal :as jrnl]
   [delta-snapshot-clj.subscription :as sub])
  (:import
   (clojure.lang IReduceInit)
   [java.time Instant]))

;; shared attributes
;(s/def ::count (s/and int? nat-int? #(<= % publisher-data-set-max-row-count)))
(s/def ::count (s/and int? nat-int?))
(s/def ::count-binary #{0 1})
(comment
  (gen/sample (s/gen ::count))
  (s/exercise ::count))

(s/def ::entity-delta-code #{delta-code-add delta-code-modify delta-code-remove})
(s/def ::entity-id some?)
(s/def ::snapshot-id some?)
(s/def ::subscription-data-set-id some?)
(def gen-subscription-data-set-id-memo 
  (memoize (fn [] (gen/generate (s/gen ::subscription-data-set-id)))))

(s/def ::inst inst?)

(s/def ::data-set-count ::count)
(s/def ::delta-add-count ::count)
(s/def ::delta-modify-count ::count)
(s/def ::delta-remove-count ::count)

(s/def ::delta-messages-reducible #(satisfies? IReduceInit %))
(s/def ::generate-opts-map (s/and (s/map-of keyword? any?)
                                  (s/keys :opt-un [::logging-min-level])))
(s/def ::transactable-conn some?)

;; SnapshotRequest record
;(s/def ::fn-pull-pub-data-set-for-sub ifn?)
(s/def ::return-full-data-set boolean?)
;(s/def ::fn-subscription-entities-equal? ifn?)
(s/def ::empty-publisher-data-set-strategy empty-publisher-data-set-strategies)
(def gen-empty-publisher-data-set-strategy-memo
  (memoize (fn [] (gen/generate (s/gen ::empty-publisher-data-set-strategy)))))

(s/def ::logging-min-level #{:trace :debug :info :warn :error :fatal :report})
(s/def ::snapshot-request-sans-fns
  (s/keys :req-un [::snapshot-id
                   ::subscription-data-set-id
                   ::return-full-data-set
                   ::empty-publisher-data-set-strategy
                   ::logging-min-level]))
(s/def ::snapshot-request
  (s/with-gen ::snapshot-request-sans-fns
    (fn [] (->> (s/gen ::snapshot-request-sans-fns)
                (gen/fmap #(assoc % 
                                  :fn-pull-pub-data-set-for-sub (fn [] [])
                                  :fn-subscription-entities-equal? =))))))
(comment
  (gen/sample (s/gen ::snapshot-request))
  (s/exercise  ::snapshot-request)
  ,)

; SnapshotResponse record
(s/def ::snapshot-response
  (s/and
   (s/keys :req-un [::snapshot-id
                    ::subscription-data-set-id
                    ::return-full-data-set
                    ::delta-messages-reducible
                    ::data-set-count
                    ::delta-add-count
                    ::delta-modify-count
                    ::delta-remove-count])
   #(every? #{:snapshot-id
              :subscription-data-set-id
              :return-full-data-set
              :delta-messages-reducible
              :data-set-count
              :delta-add-count
              :delta-modify-count
              :delta-remove-count} (keys %))))

;; Entity map
(s/def ::id ::entity-id)
(s/def ::entity-any-value some?)
(s/def ::entity
  (s/with-gen (s/and ;(s/keys :req-un [::id])
                     (s/map-of keyword? any?)
                     (fn [m] (> (-> m (keys) (count)) 1)))
    (fn [] (s/gen (s/keys :req-un [::id 
                                   ::entity-any-value])))))
(comment
  (gen/sample (s/gen ::entity))
  (gen/sample (s/gen ::entity-id)))

;; JournalRow record
(s/def ::inserted-on-full boolean?)
(s/def ::entity-delta-inst inst?)
(s/def ::entity-cur (s/nilable ::entity))
(s/def ::entity-prv (s/nilable ::entity))
(s/def ::journal-row-keys
  (s/keys :req-un [::subscription-data-set-id ::snapshot-id ::inserted-on-full ::entity-id
                   ::entity-delta-code ::entity-delta-inst ::entity-cur ::entity-prv]))
(s/def ::journal-row-add
  (s/with-gen ::journal-row-keys
    (fn [] (->> (s/gen ::journal-row-keys)
                (gen/fmap (fn [row] (-> row
                                        (assoc :entity-delta-code delta-code-add
                                               :entity-cur (gen/generate (s/gen ::entity))
                                               :entity-prv nil)
                                        (assoc-in [:entity-cur :id] (:entity-id row)))))))))
(s/def ::journal-row-remove
  (s/with-gen ::journal-row-keys
    (fn [] (->> (s/gen ::journal-row-keys)
                (gen/fmap (fn [row] (-> row
                                        (assoc :entity-delta-code delta-code-remove
                                               :entity-cur nil
                                               :entity-prv (gen/generate (s/gen ::entity)))
                                        (assoc-in [:entity-prv :id] (:entity-id row)))))))))
(s/def ::journal-row-modify
  (s/with-gen ::journal-row-keys
    (fn [] (->> (s/gen ::journal-row-keys)
                (gen/fmap (fn [row] (-> row
                                        (assoc :entity-delta-code delta-code-modify
                                               :entity-cur (gen/generate (s/gen ::entity))
                                               :entity-prv (gen/generate (s/gen ::entity)))
                                        (assoc-in [:entity-cur :entity-any-value2] (gen/generate (s/gen ::entity-any-value)))
                                        (assoc-in [:entity-cur :id] (:entity-id row))
                                        (assoc-in [:entity-prv :id] (:entity-id row)))))))))
(s/def ::journal-row-add-or-modify
  (s/with-gen ::journal-row-keys
    (fn [] (gen/fmap (fn [_] (gen/generate (s/gen (nth [::journal-row-add ::journal-row-modify] (rand-int 2)))))
                     (gen/return nil)))))
(s/def ::journal-row
  (s/with-gen ::journal-row-keys
    (fn [] (gen/fmap (fn [_] (gen/generate (s/gen (nth [::journal-row-add ::journal-row-modify ::journal-row-remove] (rand-int 3)))))
                     (gen/return nil)))))
(comment
  (gen/sample (s/gen ::journal-row-keys))
  (gen/sample (s/gen ::journal-row))
  (s/exercise ::journal-row-random 10)
  (gen/sample (s/gen ::journal-row-add))
  (gen/sample (s/gen ::journal-row-remove))
  (gen/sample (s/gen ::journal-row-modify))
  (gen/sample (s/gen ::journal-row-add-or-modify))
  (gen/generate (s/gen ::journal-row-add))
  (nth [::journal-row-add ::journal-row-modify] (rand-int 2))
  (s/exercise ::journal-row 5)
  (s/exercise ::entity-delta-inst 6)
  (-> {:subscription-data-set-id :a,
       :snapshot-id (),
       :inserted-on-full true,
       :entity-id [],
       :entity-delta-code "15SveKM",
       :entity-delta-inst #inst "1970-01-01T00:00:00.000-00:00",
       :entity-cur {:id 14, :entity-any-value [[:?+] true (:pk33/_:!T -3190518N)]},
       :entity-prv {:id #{}, :entity-any-value nil}}
      :entity-cur
      :id)
  :rcf
  )

;; ProcessedResult record
(s/def ::publisher-entity-ids
  (s/coll-of ::entity-id :kind set? :min-count 0))
(s/def ::processed-result
  (s/and
   (s/keys :req-un [::delta-add-count ::delta-modify-count ::publisher-entity-ids])
   #(every? #{:delta-add-count :delta-modify-count :publisher-entity-ids} (keys %))))
(comment
  (gen/sample (s/gen ::processed-result))
  (gen/sample (s/gen ::publisher-entity-ids))
  (s/exercise  ::processed-result))

;; ProcessedResult (binary) record
(s/def ::delta-add-count-binary ::count-binary)
(s/def ::delta-modify-count-binary ::count-binary)
(s/def ::publisher-entity-ids-binary
  (s/coll-of ::entity-id :kind set? :min-count 0 :max-count 1))
(s/def ::processed-result-binary
  (s/and
   (s/keys :req-un [::delta-add-count-binary ::delta-modify-count-binary ::publisher-entity-ids-binary])
   #(every? #{:delta-add-count-binary :delta-modify-count-binary :publisher-entity-ids-binary} (keys %))))

;; counts map
(s/def ::counts-map
  (s/and
   (s/keys :req-un [::data-set-count ::delta-add-count ::delta-modify-count ::delta-remove-count])
   #(every? #{:data-set-count :delta-add-count :delta-modify-count :delta-remove-count} (keys %))))

;; delta-remove-count map
(s/def ::delta-remove-count-m
  (s/and (s/keys :req-un [::delta-remove-count])
         (s/map-of keyword? ::count)))

(comment
  (gen/sample (s/gen ::counts-map))
  (gen/sample (s/gen ::delta-remove-count-m))
  (s/exercise  ::counts-map))

;; JournalSnapshotCrud record
(s/def ::fn-insert! ifn?)
(s/def ::fn-table-row-entity-id-val ifn?)
(s/def ::fn-now ifn?)
(s/def ::fn-retrieve-newest-entity ifn?)
(s/def ::fn-retrieve-full-ids ifn?)
(s/def ::journal-snapshot-crud
  (s/and
   (s/keys :req-un [::fn-insert! ::fn-table-row-entity-id-val ::fn-now ::fn-retrieve-newest-entity ::fn-retrieve-full-ids])
   #(every? #{:fn-insert! :fn-table-row-entity-id-val :fn-now :fn-retrieve-newest-entity :fn-retrieve-full-ids} (keys %))))

;; JournalOperation protocol
(s/def ::journal-operation
  (s/with-gen #(satisfies? jrnl/JournalOperation %)
    (fn [] (gen/fmap (fn [_] (reify jrnl/JournalOperation
                               ;; mappers
                               (table-row-entity-id-val [_ table-row]
                                 (gen/generate (s/gen ::entity-id)))
                               (table-row->row-record [_ table-row]
                                 (gen/generate (s/gen ::journal-row)))
                               (table-row->message [_ table-row]
                                 nil)
                               ;; write operations
                               (insert-row! [_ transactable-connection journal-row]
                                 nil)
                               ;; read operations
                               (now [_]
                                 (Instant/now))
                               (generate-new-snapshot-id [_]
                                 ;(gen/generate (s/gen ::snapshot-id)) TODO memoize
                                 88)
                               (retrieve-newest-data-set-entity-by-id [_ transactable-conn subscription-data-set-id entity-id]
                                 nil)
                               (retrieve-data-set-snapshot [_ subscription-data-set-id snapshot-id]
                                 [])
                               (retrieve-newest-data-set-entities-excluding-delta [_ subscription-data-set-id exclude-delta-code]
                                 [])
                               (retrieve-newest-data-set-entity-ids-excluding-delta [_ transactable-conn subscription-data-set-id exclude-delta-code]
                                 [])))
                     (gen/return nil)))))
(comment
  (gen/sample (s/gen ::journal-operation))
  (s/exercise  ::journal-operation)
  :rcf)

;; SubscriptionConfig protocol
(s/def ::publisher-data-set-entity-id-keyword keyword?)
(s/def ::subscription-config
  (s/with-gen #(satisfies? sub/SubscriptionConfig %)
    (fn [] (gen/fmap (fn [_] (reify sub/SubscriptionConfig
                               (subscription-data-set-id [_]
                                 (gen-subscription-data-set-id-memo))
                               (publisher-data-set-entity-id-keyword [_]
                                 ;(gen/generate (s/gen ::entity-id))
                                 (s/gen ::publisher-data-set-entity-id-keyword))
                               (pull-publisher-data-set [_]
                                 [])
                               (empty-publisher-data-set-strategy [_]
                                 (gen-empty-publisher-data-set-strategy-memo))
                               (entities-equal? [_ entity1 entity2]
                                 (= entity1 entity2))))
                     (gen/return nil)))))
(comment
  (gen/sample (s/gen ::subscription-config))
  (s/exercise  ::subscription-config)
  ,)

;; Core Functions
(s/fdef core/make-counts-map
  :args (s/cat :processed-result ::processed-result
               :delta-remove-count-m ::delta-remove-count-m)
  :ret ::counts-map)

(s/fdef core/make-snapshot-request
  :args (s/cat :snapshot-id ::snapshot-id
               :subscription-config ::subscription-config
               :return-full-data-set ::return-full-data-set
               :opts ::generate-opts-map)
  :ret some?
  :fn (s/and  #(= (-> % :ret :snapshot-id) (-> % :args :snapshot-id))
              #(= (-> % :ret :subscription-data-set-id) (sub/subscription-data-set-id (-> % :args :subscription-config)))
              #(= (-> % :ret :empty-publisher-data-set-strategy) (sub/empty-publisher-data-set-strategy (-> % :args :subscription-config)))
              ;#(= (-> % :ret :fn-pull-pub-data-set-for-sub) (partial sub/pull-publisher-data-set (-> % :args :subscription-config)))
              ;#(= (-> % :ret :fn-subscription-entities-equal?) (partial sub/entities-equal? (-> % :args :subscription-config)))
              #(= (-> % :ret :return-full-data-set) (-> % :args :return-full-data-set))))

(stest/check 'delta-snapshot-clj.core/mk-snapshot-request {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/mk-snapshot-response {:clojure.spec.test.check/opts {:num-tests 100}})

(s/fdef core/make-snapshot-response
  :args (s/cat :snapshot-request ::snapshot-request
               :counts-map ::counts-map
               :delta-messages-reducible some?)
  :ret some?
  :fn (s/and #(= (-> % :ret :snapshot-id) (-> % :args :snapshot-request :snapshot-id))
             #(= (-> % :ret :subscription-data-set-id) (-> % :args :snapshot-request :subscription-data-set-id))
             #(= (-> % :ret :return-full-data-set) (-> % :args :snapshot-request :return-full-data-set))
             #(= (-> % :ret :data-set-count) (-> % :args :counts-map :data-set-count))
             #(= (-> % :ret :delta-add-count) (-> % :args :counts-map :delta-add-count))
             #(= (-> % :ret :delta-modify-count) (-> % :args :counts-map :delta-modify-count))
             #(= (-> % :ret :delta-remove-count) (-> % :args :counts-map :delta-remove-count))
             #(= (-> % :ret :delta-messages-reducible) (-> % :args :delta-messages-reducible))))

(s/fdef core/JournalRow->add
  :args (s/cat :snapshot-request ::snapshot-request
               :inst ::inst
               :entity-new ::entity
               :journal-row ::journal-row-remove)
  :ret ::journal-row
  :fn  (s/and #(= (-> % :ret :subscription-data-set-id) (-> % :args :journal-row :subscription-data-set-id))
              #(= (-> % :ret :snapshot-id) (-> % :args :snapshot-request :snapshot-id))
              #(= (-> % :ret :inserted-on-full) (-> % :args :snapshot-request :return-full-data-set))
              #(= (-> % :ret :entity-id) (-> % :args :journal-row :entity-id))
              #(= (-> % :ret :entity-delta-inst) (-> % :args :inst))
              #(= (-> % :ret :entity-delta-code) delta-code-add)
              #(= (-> % :ret :entity-cur) (-> % :args :entity-new))
              #(= (-> % :ret :entity-prv) nil)))

(s/fdef core/JournalRow->modify
  :args (s/cat :snapshot-request ::snapshot-request
               :inst ::inst
               :entity-new ::entity
               :journal-row ::journal-row-add)
  :ret ::journal-row
  :fn (s/and #(= (-> % :ret :subscription-data-set-id) (-> % :args :journal-row :subscription-data-set-id))
             #(= (-> % :ret :snapshot-id) (-> % :args :snapshot-request :snapshot-id))
             #(= (-> % :ret :inserted-on-full) (-> % :args :snapshot-request :return-full-data-set))
             #(= (-> % :ret :entity-id) (-> % :args :journal-row :entity-id))
             #(= (-> % :ret :entity-delta-inst) (-> % :args :inst))
             #(= (-> % :ret :entity-delta-code) delta-code-modify)
             #(= (-> % :ret :entity-cur) (-> % :args :entity-new))
             #(= (-> % :ret :entity-prv) (-> % :args :journal-row :entity-cur))))

(s/fdef core/JournalRow->remove
  :args (s/cat :snapshot-request ::snapshot-request
               :inst ::inst
               :journal-row ::journal-row-add-or-modify)
  :ret ::journal-row
  :fn (s/and #(= (-> % :ret :subscription-data-set-id) (-> % :args :journal-row :subscription-data-set-id))
             #(= (-> % :ret :snapshot-id) (-> % :args :snapshot-request :snapshot-id))
             #(= (-> % :ret :inserted-on-full) (-> % :args :snapshot-request :return-full-data-set))
             #(= (-> % :ret :entity-id) (-> % :args :journal-row :entity-id))
             #(= (-> % :ret :entity-delta-inst) (-> % :args :inst))
             #(= (-> % :ret :entity-delta-code) delta-code-remove)
             #(= (-> % :ret :entity-cur) nil)
             #(= (-> % :ret :entity-prv) (-> % :args :journal-row :entity-cur))))

(s/fdef core/entity->JournalRow-add
  :args (s/cat :snapshot-request ::snapshot-request
               :inst ::inst
               :entity-new ::entity
               :entity-id ::entity-id)
  :ret ::journal-row
  :fn (s/and #(= (-> % :ret :subscription-data-set-id) (-> % :args :snapshot-request :subscription-data-set-id))
             #(= (-> % :ret :snapshot-id) (-> % :args :snapshot-request :snapshot-id))
             #(= (-> % :ret :inserted-on-full) (-> % :args :snapshot-request :return-full-data-set))
             #(= (-> % :ret :entity-id) (-> % :args :entity-id))
             #(= (-> % :ret :entity-delta-inst) (-> % :args :inst))
             #(= (-> % :ret :entity-delta-code) delta-code-add)
             #(= (-> % :ret :entity-cur) (-> % :args :entity-new))
             #(= (-> % :ret :entity-prv) nil)))

(s/fdef core/make-journal-snapshot-crud
  :args (s/cat :transactable-conn ::transactable-conn
               :journal-operation ::journal-operation
               :subscription-data-set-id ::subscription-data-set-id)
  :ret ::journal-snapshot-crud)

(s/fdef core/exists-in-data-set?
  :args (s/cat :publisher-entity-ids ::publisher-entity-ids
               :entity-id ::entity-id)
  :ret boolean?)

(s/fdef core/snapshot-failure-due-to-empty-data-set?
  :args (s/cat :empty-publisher-data-set-strategy ::empty-publisher-data-set-strategy
               :publisher-entity-ids ::publisher-entity-ids)
  :ret boolean?)

(s/fdef core/contrast->add-mod-row 
  :args (s/cat :snapshot-request ::snapshot-request
               :inst ::inst
               :data-set-entity ::entity
               :entity-id ::entity-id
               :journal-row (s/nilable ::journal-row))
  :ret (s/nilable ::journal-row)  
  :fn (s/or :nil?-ret-and-some?-args-journal-row ; no delta
            (s/and  #(nil? (-> % :ret))
                    #(some? (-> % :args :journal-row))
                    #((-> % :args :snapshot-request :fn-subscription-entities-equal?)
                      (-> % :args :data-set-entity)
                      (-> % :args :journal-row :entity-cur)))
            :some?-ret-and-nil?-args-journal-row ; add
            (s/and  #(some? (-> % :ret))
                    #(nil? (-> % :args :journal-row))
                    #(= delta-code-add (-> % :ret :entity-delta-code))
                    #(= (-> % :ret :snapshot-id) (-> % :args :snapshot-request :snapshot-id))
                    #(= (-> % :ret :entity-cur) (-> % :args :data-set-entity)))
            :some?-ret-and-some?-args-journal-row ; re-add, remove or modify
            (s/and #(some? (-> % :ret))
                   #(some? (-> % :args :journal-row))
                   (s/or :delta-code-add-ret
                         (s/and #(= delta-code-add (-> % :ret :entity-delta-code))
                                #(= delta-code-remove (-> % :args :journal-row :entity-delta-code)))
                         :delta-code-modify-ret
                         (s/and #(= delta-code-modify (-> % :ret :entity-delta-code))
                                #(some? (-> % :args :journal-row))
                                #((-> % :args :snapshot-request :fn-subscription-entities-equal? complement)
                                  (-> % :args :data-set-entity)
                                  (-> % :args :journal-row :entity-cur))))
                   #(= (-> % :ret :entity-cur) (-> % :args :data-set-entity)))))

;(s/fdef ds/merge-processed-results
;        :args (s/cat :processed-result (s/coll-of ::processed-result-unmerged :kind seq? :min-count 0 :max-count publisher-data-set-max-row-count))
;        :ret ::processed-result-merged)

;(s/fdef ds/merge-processed-results
;        :args (s/cat :processed-result (s/coll-of ::processed-result-unmerged :kind seq? :min-count 0 :max-count publisher-data-set-max-row-count))
;        :ret ::processed-result-merged)

;; IO functions
(s/fdef io/sweep-data-set-removes!
  :args (s/cat :snapshot-request ::snapshot-request
               :journal-table-crud ::journal-snapshot-crud
               :processed-result ::processed-result)
  :ret ::delta-remove-count-m)

(s/fdef io/process-entity-add-modify!
  :args (s/cat :snapshot-request ::snapshot-request
               :journal-table-crud ::journal-snapshot-crud
               :subscription-config ::subscription-config
               :entity ::entity)
  :ret ::processed-result-binary)

(s/fdef io/generate-snapshot!
  :args (s/cat :journal-operation ::journal-operation
               :subscription-config ::subscription-config
               :transactable-conn ::transactable-conn
               :return-full-data-set ::return-full-data-set
               :opts (s/? ::generate-opts-map))
  :ret ::snapshot-response)

(s/fdef io/retrieve-snapshot
  :args (s/cat :journal-operation ::journal-operation
               :subscription-data-set-id ::subscription-data-set-id
               :snapshot-id ::snapshot-id
               :logging-min-level (s/? ::logging-min-level))
  :ret ::delta-messages-reducible)

(s/fdef io/retrieve-full
  :args (s/cat :journal-operation ::journal-operation
               :subscription-data-set-id ::subscription-data-set-id
               :logging-min-level (s/? ::logging-min-level))
  :ret ::delta-messages-reducible)

(s/def ::args-snapshot!-and-full!
  (s/cat :journal-operation ::journal-operation
         :subscription-config ::subscription-config
         :transactable-conn ::transactable-conn
         :opts (s/? ::generate-opts-map)))

(s/fdef io/snapshot!
  :args ::args-snapshot!-and-full!
  :ret ::snapshot-response)

(s/fdef io/full!
  :args ::args-snapshot!-and-full!
  :ret ::snapshot-response)

(stest/unstrument (stest/enumerate-namespace ['delta-snapshot-clj.core
                                              'delta-snapshot-clj.io]))
(stest/instrument (stest/enumerate-namespace ['delta-snapshot-clj.core]))
(stest/instrument (stest/enumerate-namespace ['delta-snapshot-clj.io]))

;(stest/check 'delta-snapshot-clj.core/merge-processed-results {:clojure.spec.test.check/opts {:num-tests 10}}) ; TODO need generators
;(stest/check (stest/enumerate-namespace 'delta-snapshot-clj.core))

;(stest/check 'delta-snapshot-clj.core/make-journal-table-crud) ; TODO Unable to construct gen
(stest/check 'delta-snapshot-clj.core/exists-in-data-set? {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/snapshot-failure-due-to-empty-data-set? {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/mk-snapshot-request {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/mk-snapshot-response {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/mk-counts-map {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/JournalRow->add {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/JournalRow->modify {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/JournalRow->remove {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/entity->JournalRow-add {:clojure.spec.test.check/opts {:num-tests 100}})
(stest/check 'delta-snapshot-clj.core/contrast->add-mod-row {:clojure.spec.test.check/opts {:num-tests 100}})