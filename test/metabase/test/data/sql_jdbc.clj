(ns metabase.test.data.sql-jdbc
  "Common test extension functionality for SQL-JDBC drivers."
  (:require [clojure.java.jdbc :as jdbc]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.test.data
             [interface :as tx]
             [sql :as sql.tx]]
            [metabase.test.data.sql-jdbc.load-data :as load-data]))

(driver/register! :sql-jdbc/test-extensions, :abstract? true)

(sql.tx/add-test-extensions! :sql-jdbc/test-extensions)

(defn add-test-extensions! [driver]
  (driver/add-parent! driver :sql-jdbc/test-extensions))


(defmethod tx/create-db! :sql-jdbc/test-extensions [driver dbdef & options]
  (apply load-data/create-db! driver dbdef options))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                    Util Fns                                                    |
;;; +----------------------------------------------------------------------------------------------------------------+

;; TODO - not sure if these belong here or somewhere else

(defn- do-when-testing-engine {:style/indent 1} [engine f]
  (require 'metabase.test.data.datasets)
  ((resolve 'metabase.test.data.datasets/do-when-testing-engine) engine f))

(defn execute-when-testing!
  "Execute a prepared SQL-AND-ARGS against Database with spec returned by GET-CONNECTION-SPEC only when running tests
  against ENGINE. Useful for doing engine-specific setup or teardown."
  {:style/indent 2}
  [engine get-connection-spec & sql-and-args]
  (do-when-testing-engine engine
    (fn []
      (println (u/format-color 'blue "[%s] %s" (name engine) (first sql-and-args)))
      (jdbc/execute! (get-connection-spec) sql-and-args)
      (println (u/format-color 'blue "[OK]")))))

(defn query-when-testing!
  "Execute a prepared SQL-AND-ARGS **query** against Database with spec returned by GET-CONNECTION-SPEC only when
  running tests against ENGINE. Useful for doing engine-specific setup or teardown where `execute-when-testing!` won't
  work because the query returns results."
  {:style/indent 2}
  [engine get-connection-spec & sql-and-args]
  (do-when-testing-engine engine
    (fn []
      (println (u/format-color 'blue "[%s] %s" (name engine) (first sql-and-args)))
      (u/prog1 (jdbc/query (get-connection-spec) sql-and-args)
        (println (u/format-color 'blue "[OK] -> %s" (vec <>)))))))
