(ns ^{:doc "Delta Snapshot library subscription."
      :author "Clay Lipscomb"}
 delta-snapshot-clj.subscription)

(defprotocol SubscriptionConfig
  "All configuration data necessary for a specific subscription."
  (subscription-data-set-id
    [this]
    "Return unique identifier for the subscription data set.")
  (publisher-data-set-entity-id-keyword
   ^Keyword [this]
   "Return the keyword used for the id in the entity map of the publisher data set.")
  (pull-publisher-data-set
    ^IReduceInit [this]
    "Returns an IReduceInit that pulls all entities in the publisher data set for the subscription. Each entity must
     be a hash map containing an keyword defined by `publisher-data-set-entity-id-keyword`.")
  (empty-publisher-data-set-strategy
    [this]
    "Return the strategy employed if the publisher data set is empty when generating a snapshot. Valid values are 
     `:snapshot-failure` (default) and `:snapshot-success-return-all-remove-deltas`. A nil will be treated as the 
     default.")
  (entities-equal?
    ^Boolean [this entity1 entity2]
    "Predicate that determines equality between two subscription entities. Should implement with `=` if no custom 
     entity comparison is required."))