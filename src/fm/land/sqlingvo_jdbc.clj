(ns fm.land.sqlingvo-jdbc
  (:require [sqlingvo.core :as sqlingvo]
            [sqlingvo.db :as sqlingvo-db]
            [sqlingvo.util :as sqlingvo-util]
            [clojure.java.jdbc :as jdbc])
  (:import [org.postgresql.Driver]
           [java.sql PreparedStatement]))

(defn db-conn [conn]
  "Fetch the connection object to use"
  (::spec conn))

(defn swap-spec [db spec]
  (assoc db ::spec spec))

(defn add-connection [db con]
  (swap-spec db (jdbc/add-connection (::spec db) con)))

(defn- body-builder [name params]
  "Takes multiple paramter lists and builds bodies for our db-function.
   name - The name of the function we're building. We need this so we can call the proper jdbc function
   params - the list of parameters the function can take (e.g. ([db table] [db] [db table opts]))"
  (for [param params]
    (let [[db & args] param
          [args [amp rest]] (split-with #(not= % '&) args)]
      (cond
        rest
        `([~db ~@args & ~rest] (apply ~(symbol "jdbc" (str name)) (db-conn ~db) ~@args ~rest))
        :else
        `([~db ~@args] (~(symbol "jdbc" (str name)) (db-conn ~db) ~@args))))))

(defmacro db-function [name params & more-params]
  "Defines a function named `name` that simply calls the version in
  clojure.java.jdbc but looks up the proper connection object to use. (See
  fm.land.sqlingvo-jdbc/db)"
  `(defn ~name ~@(body-builder name (conj more-params params))))

(defmacro with-db-transaction
  "Evaluates body in the context of a transaction on the specified database connection.
  The binding provides the database connection for the transaction and the name to which
  that is bound for evaluation of the body. The binding may also specify the isolation
  level for the transaction, via the :isolation option and/or set the transaction to
  readonly via the :read-only? option.
  (with-db-transaction [t-con db-spec {:isolation level :read-only? true}]
    ... t-con ...)
  See db-transaction* for more details."
  [binding & body]
  (let [tx (first binding)
        db (second binding)]
    `(db-transaction* ~db
                      (^{:once true} fn* [~tx]
                                         (let [~tx (swap-spec ~db ~tx)]
                                           ~@body))
                      ~@(rest (rest binding)))))

(defmacro with-db-connection
  "Evaluates body in the context of an active connection to the database.
  (with-db-connection [con-db db-spec]
    ... con-db ...)"
  [binding & body]
  `(let [db# ~(second binding)]
     (with-open [con# (get-connection db#)]
       (let [~(first binding) (add-connection db# con#)]
         ~@body))))

(defmacro with-db-metadata
  "Evaluates body in the context of an active connection with metadata bound
   to the specified name. See also metadata-result for dealing with the results
   of operations that retrieve information from the metadata.
   (with-db-metadata [md db-spec]
     ... md ...)"
  [binding & body]
  `(with-open [con# (get-connection ~(second binding))]
     (let [~(first binding) (.getMetaData con#)]
       ~@body)))

(db-function get-connection [db])
(db-function db-find-connection [db])
(db-function db-connection [db])

(db-function db-set-rollback-only! [db])
(db-function db-unset-rollback-only! [db])
(db-function db-is-rollback-only [db])
(db-function db-transaction* [db & rest])
(db-function db-do-commands [db & rest])
(db-function db-do-prepared-return-keys [db & rest])
(db-function db-do-prepared [db & rest])
(db-function db-query-with-resultset [db sql-params func & rest])

(db-function query [db sql-params & opts])
(db-function find-by-keys [db table columns & opts])
(db-function get-by-id [db table pk-value & rest])
(db-function execute! [db sql-params & opts])
(db-function delete! [db table where-clause & rest])
(db-function insert! [db table cols-or-row & rest])
(db-function insert-multi! [db table cols-or-rows & rest])
(db-function update! [db table set-map where-clause & rest])

(defn sqlingvo-eval [statement]
  (let [op (:op statement)
        db (:db statement)]
    (case op
      :select (query db (sqlingvo/sql statement))
      :intersect (query db (sqlingvo/sql statement))
      :except (query db (sqlingvo/sql statement))
      :union (query db (sqlingvo/sql statement))
      :with (query db (sqlingvo/sql statement))
      :explain (query db (sqlingvo/sql statement))
      :insert (if (:returning statement)
                (query db (sqlingvo/sql statement))
                (execute! db (sqlingvo/sql statement)))
      :delete (if (:returning statement)
                (query db (sqlingvo/sql statement))
                (execute! db (sqlingvo/sql statement)))
      :update (if (:returning statement)
                (query db (sqlingvo/sql statement))
                (execute! db (sqlingvo/sql statement)))
      :copy (execute! db (sqlingvo/sql statement))
      :create-table (execute! db (sqlingvo/sql statement))
      :drop-table (execute! db (sqlingvo/sql statement))
      :drop-materialized-view (execute! db (sqlingvo/sql statement))
      :refresh-materialized-view (execute! db (sqlingvo/sql statement))
      :truncate (execute! db (sqlingvo/sql statement)))))

(defn db
  "Creates a new database handle for sqlingvo-jdbc.
  Arguments: spec - The database spec used to connect to your database with
  clojure.java.jdbc sqlingvo-opts - The options passed to sqlingvo's Database
  record (usually you can leave this blank, unless you are using MySQL and need
  to change the :sql-quote option to sqlingvo.util/sql-quote-backtick)"
  ([spec] (db spec {}))
  ([spec {:keys [sql-quote eval-fn]
          :or {sql-quote sqlingvo-util/sql-quote-double-quote
               sql-name sqlingvo-util/sql-name-underscore
               eval-fn #'sqlingvo-eval}
          :as sqlingvo-opts}]
   (sqlingvo-db/map->Database (merge {:sql-quote sql-quote
                                      :eval-fn eval-fn
                                      :sql-name sql-name
                                      ::spec spec}
                                     sqlingvo-opts))))

(comment
  (def d (db "jdbc:postgresql://postgres:gnome@localhost:5432/sqlingvo"))
  (sqlingvo/sql (sqlingvo/select d [:encrypted-password]
     (sqlingvo/from :products)))

  (sqlingvo/sql (sqlingvo/select (sqlingvo-db/postgresql) [:encrypted-password]
                  (sqlingvo/from :products)))

  (let [d (db "jdbc:postgresql://postgres:gnome@localhost:5432/sqlingvo")]
    @(sqlingvo/drop-table d [:products])
    @(sqlingvo/drop-table d [:films]))

  (let [d (db "jdbc:postgresql://postgres:gnome@localhost:5432/sqlingvo")]
    @(sqlingvo/create-table d :products
       (sqlingvo/column :id :bigserial :primary-key? true)
       (sqlingvo/column :name :varchar)
       (sqlingvo/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
       (sqlingvo/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now)))
    @(sqlingvo/create-table d :films
       (sqlingvo/column :code :char :length 5 :primary-key? true)
       (sqlingvo/column :title :varchar :length 40 :not-null? true)
       (sqlingvo/column :did :integer :not-null? true)
       (sqlingvo/column :date-prod :date)
       (sqlingvo/column :kind :varchar :length 10)
       (sqlingvo/column :len :interval)
       (sqlingvo/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
       (sqlingvo/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))))

  (let [spec {:subprotocol "postgresql"
              :subname "//localhost/sqlingvo?user=postgres&password=gnome"
              :classname "org.postgresql.Driver"}
        spec "jdbc:postgresql://postgres:gnome@localhost:5432/sqlingvo"
        d (db spec)]


    (execute! d ["DELETE FROM products"])
    (execute! d ["INSERT INTO products (name) VALUES ('Oh Hai Mark')"])
    (query d ["SELECT * FROM products"])

    (with-db-transaction [tx d]
      (execute! tx ["INSERT INTO products (name) VALUES ('Oh Hai Mark')"])
      (db-set-rollback-only! tx)
      (db-unset-rollback-only! tx)
      (query tx ["SELECT * FROM products"]))

    (with-db-connection [conn d]
      (execute! conn ["INSERT INTO products (name) VALUES ('Oh Hai Mark')"])
      (query conn ["SELECT * FROM products"]))

    (with-db-connection [conn d]
      (with-db-transaction [tx conn]
        (execute! tx ["INSERT INTO products (name) VALUES ('Oh Hai Mark')"])
        (db-set-rollback-only! tx)
        (query tx ["SELECT * FROM products"])))

    (query d ["SELECT * FROM products"])
    (get-connection d)

    @(sqlingvo/select d [:*] (sqlingvo/from :products))
    (with-db-transaction [tx d]
      @(sqlingvo/insert tx :products []
                        (sqlingvo/values [{:name "INSERT FROM SQLINGVO"}]))
      ;; (db-set-rollback-only! tx)
      @(sqlingvo/select tx [:*]
                        (sqlingvo/from :products)))

    (with-db-connection [conn d]
      (with-db-transaction [tx conn]
        @(sqlingvo/select tx [:*] (sqlingvo/from :products))
        @(sqlingvo/delete tx :products (sqlingvo/where '(ilike :name "%SQLINGVO")))
        @(sqlingvo/select tx [:*] (sqlingvo/from :products))))

    @(sqlingvo/intersect (sqlingvo/select d [:name] (sqlingvo/from :products))
                         (sqlingvo/select d [:title] (sqlingvo/from :films)))

    @(sqlingvo/explain d (sqlingvo/intersect (sqlingvo/select d [:name] (sqlingvo/from :products))
                                             (sqlingvo/select d [:title] (sqlingvo/from :films))))
    ;; @(sqlingvo/truncate d [:products :films])

    @(sqlingvo/union (sqlingvo/select d [:name] (sqlingvo/from :products))
                     (sqlingvo/select d [:title] (sqlingvo/from :films)))

    @(sqlingvo/with d [:bwah (sqlingvo/select d [:* (sqlingvo/as "Oh HAI MARK" :tommy)]
                                              (sqlingvo/from :products))]
       (sqlingvo/select d [:*] (sqlingvo/from :bwah)))

    (with-db-metadata [metadata d]
      (let [table-info (jdbc/metadata-query (.getTables metadata
                                                    nil nil nil
                                                    (into-array ["TABLE" "VIEW"])))]
        table-info))
    )

  (let [{:keys [bwah] :or {bwah "oh hai"}} {}]
    bwah))
