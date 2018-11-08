(ns metabase.test.data.h2
  "Code for creating / destroying an H2 database from a `DatabaseDefinition`."
  (:require [clojure.string :as str]
            [metabase.db.spec :as dbspec]
            [metabase.test.data.interface :as tx]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.test.data.sql :as sql.tx]
            [metabase.test.data.sql.ddl :as ddl]
            [metabase.test.data.sql-jdbc.spec :as spec]
            [metabase.test.data.sql-jdbc.execute :as execute]
            [metabase.test.data.sql-jdbc.load-data :as load-data]
            [metabase.util :as u]
            [metabase.test.data.sql-jdbc :as sql-jdbc.tx])
  (:import metabase.driver.h2.H2Driver))

(sql-jdbc.tx/add-test-extensions! :h2)

(sql.tx/add-inline-comment-extensions! :h2)

(def ^:private ^:const field-base-type->sql-type
  {:type/BigInteger "BIGINT"
   :type/Boolean    "BOOL"
   :type/Date       "DATE"
   :type/DateTime   "DATETIME"
   :type/Decimal    "DECIMAL"
   :type/Float      "FLOAT"
   :type/Integer    "INTEGER"
   :type/Text       "VARCHAR"
   :type/Time       "TIME"})


(defmethod tx/database->connection-details [context dbdef]
  {:db (str "mem:" (tx/escaped-name dbdef) (when (= context :db)
                                            ;; Return details with the GUEST user added so SQL queries are allowed.
                                            ";USER=GUEST;PASSWORD=guest"))})


(defn- quote-name [nm]
  (str \" (str/upper-case nm) \"))

(def ^:private ^:const ^String create-db-sql
  (str
   ;; We don't need to actually do anything to create a database here. Just disable the undo
   ;; log (i.e., transactions) for this DB session because the bulk operations to load data don't need to be atomic
   "SET UNDO_LOG = 0;\n"

   ;; Create a non-admin account 'GUEST' which will be used from here on out
   "CREATE USER IF NOT EXISTS GUEST PASSWORD 'guest';\n"

   ;; Set DB_CLOSE_DELAY here because only admins are allowed to do it, so we can't set it via the connection string.
   ;; Set it to to -1 (no automatic closing)
   "SET DB_CLOSE_DELAY -1;"))

(defmethod sql.tx/create-table-sql :h2 [_ dbdef {:keys [table-name], :as tabledef}]
  (str
   (sql.tx/create-table-sql :sql/test-extensions dbdef tabledef) ";\n"

   ;; Grant the GUEST account r/w permissions for this table
   (format "GRANT ALL ON %s TO GUEST;" (quote-name table-name))))

(defmethod tx/id-field-type :h2 [_] :type/BigInteger)

:create-db-sql                (constantly create-db-sql)
:create-table-sql             create-table-sql
;; Don't use the h2 driver implementation, which makes the connection string read-only & if-exists only
:database->spec               (comp dbspec/h2 tx/database->connection-details)
:drop-db-if-exists-sql        (constantly nil)
:execute-sql!                 (fn [this _ dbdef sql]
                                ;; we always want to use 'server' context when execute-sql! is called (never
                                ;; try connect as GUEST, since we're not giving them priviledges to create
                                ;; tables / etc)
                                (execute-sql! this :server dbdef sql))
:field-base-type->sql-type    (u/drop-first-arg field-base-type->sql-type)
:load-data!                   load-data/load-data-all-at-once!
:pk-field-name                (constantly "ID")
:pk-sql-type                  (constantly "BIGINT AUTO_INCREMENT")
:prepare-identifier           (u/drop-first-arg str/upper-case)
:quote-name                   (u/drop-first-arg quote-name)
