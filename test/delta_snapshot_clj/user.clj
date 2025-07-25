(comment
  (ns user)
  ('user ns-aliases)
  (require
   '[delta-snapshot-clj.core-test :as core-test]
   '[delta-snapshot-clj.io-test :as io-test]
   '[clojure.tools.namespace.repl :refer [refresh]]
   '[delta-snapshot-clj.specs]
   '[clojure.test :as test])

  (refresh)
  *ns*
  (in-ns 'user)

  (io-test/set-logging-min-level :error)
  (io-test/set-logging-min-level :trace)
  (io-test/set-logging-min-level :report)

  (delta-snapshot-clj.core-test/test-snapshot-failure-due-to-empty-data-set?)
  (delta-snapshot-clj.core-test/test-contrast->add-mod-row)
  (delta-snapshot-clj.core-test/test-combine-ProcessedResult-duplicate-ids-failure)
  (delta-snapshot-clj.core-test/test-combine-ProcessedResult-identity)
  (test/run-tests 'delta-snapshot-clj.core-test)

  (delta-snapshot-clj.io-test/test-snapshot!-pub-data-set-empty-failure)
  (delta-snapshot-clj.io-test/test-snapshot!-pub-data-set-empty-success)
  (delta-snapshot-clj.io-test/test-snapshot!-full!-single-delta-permutations)
  (test/run-tests 'delta-snapshot-clj.io-test)

  (test/run-tests 'delta-snapshot-clj.core-test 'delta-snapshot-clj.io-test)

  :rcf)