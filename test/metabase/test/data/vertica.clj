(ns metabase.test.data.vertica
  "Code for creating / destroying a Vertica database from a `DatabaseDefinition`."
  (:require [metabase.test.data.interface :as tx]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.test.data.sql :as sql.tx]
            [metabase.test.data.sql.ddl :as sql.ddl]
            [metabase.test.data.sql-jdbc.spec :as spec]
            [metabase.test.data.sql-jdbc.execute :as execute]
            [metabase.test.data.sql-jdbc.load-data :as load-data]
             [interface :as i]
            [metabase.util :as u]))

(def ^:private ^:const field-base-type->sql-type
  {:type/BigInteger "BIGINT"
   :type/Boolean    "BOOLEAN"
   :type/Char       "VARCHAR(254)"
   :type/Date       "DATE"
   :type/DateTime   "TIMESTAMP"
   :type/Decimal    "NUMERIC"
   :type/Float      "FLOAT"
   :type/Integer    "INTEGER"
   :type/Text       "VARCHAR(254)"
   :type/Time       "TIME"})


(defn- db-name []
  (tx/db-test-env-var-or-throw :vertica :db "docker"))

(def ^:private db-connection-details
  (delay {:host     (tx/db-test-env-var-or-throw :vertica :host "localhost")
          :port     (Integer/parseInt (tx/db-test-env-var-or-throw :vertica :port "5433"))
          :user     (tx/db-test-env-var :vertica :user "dbadmin")
          :password (tx/db-test-env-var :vertica :password)
          :db       (db-name)
          :timezone :America/Los_Angeles ; why?
          }))

(defn- qualified-name-components
  ([_]                             [(db-name)])
  ([db-name table-name]            ["public" (tx/db-qualified-table-name db-name table-name)])
  ([db-name table-name field-name] ["public" (tx/db-qualified-table-name db-name table-name) field-name]))


(u/strict-extend VerticaDriver
  generic/IGenericSQLTestExtensions
  (merge generic/DefaultsMixin
         {:create-db-sql             (constantly nil)
          :drop-db-if-exists-sql     (constantly nil)
          :drop-table-if-exists-sql  sql.tx/drop-table-if-exists-cascade-sql
          :field-base-type->sql-type (u/drop-first-arg field-base-type->sql-type)
          :load-data!                generic/load-data-one-at-a-time-parallel!
          :pk-sql-type               (constantly "INTEGER")
          :qualified-name-components (u/drop-first-arg qualified-name-components)
          :execute-sql!              execute/sequentially-execute-sql!})
  i/IDriverTestExtensions
  (merge generic/IDriverTestExtensionsMixin
         {:database->connection-details       (fn [& _] @db-connection-details)
          :engine                             (constantly :vertica)
          :has-questionable-timezone-support? (constantly true)}))



(defn- dbspec [& _]
  (sql-jdbc.conn/connection-details->spec :vertica @db-connection-details))

(defn- set-max-client-sessions!
  {:expectations-options :before-run}
  []
  ;; Close all existing sessions connected to our test DB
  (generic/query-when-testing! :vertica dbspec "SELECT CLOSE_ALL_SESSIONS();")
  ;; Increase the connection limit; the default is 5 or so which causes tests to fail when too many connections are made
  (sql-jdbc.tx/execute-when-testing! :vertica dbspec (format "ALTER DATABASE \"%s\" SET MaxClientSessions = 10000;" (db-name))))
