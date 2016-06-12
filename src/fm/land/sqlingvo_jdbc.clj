(ns fm.land.sqlingvo-jdbc
  (:require [sqlingvo.core :as sql]
            [sqlingvo.db :as sql-db]
            [sqlingvo.util :as sql-util]
            [clojure.java.jdbc :as jdbc]))

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
    (let [[db & args]        param
          [args [_amp rest]] (split-with #(not= % '&) args)]
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
(db-function db-transaction* [db func] [db func opts])
(db-function db-do-commands [db sql-commands] [db transaction? sql-commands])
(db-function db-do-prepared-return-keys [db sql-params] [db transaction? sql-params] [db transaction? sql-params opts])
(db-function db-do-prepared [db sql-params] [db transaction? sql-params] [db transaction? sql-params opts])
(db-function db-query-with-resultset [db sql-params func] [db sql-params func opts])

(defn query
  ([db sql-params] (query db sql-params {}))
  ([db sql-params opts]
   (jdbc/query (db-conn db) sql-params (merge (-> db ::jdbc-opts ::query-opts) opts))))
(db-function find-by-keys [db table columns] [db table columns opts])
(db-function get-by-id [db table pk-value] [db table pk-value pk-name-or-opts] [db table pk-value pk-name opts])
(db-function execute! [db sql-params] [db sql-params opts])
(db-function delete! [db table where-clause] [db table where-clause opts])
(db-function insert! [db table row] [db table cols-or-row values-or-opts] [db table cols values opts])
(db-function insert-multi! [db table rows] [db table cols-or-rows values-or-opts] [db table cols values opts])
(db-function update! [db table set-map where-clause] [db table set-map where-clause opts])

(defn sqlingvo-eval [statement]
  (let [op         (:op statement)
        db         (:db statement)
        query-opts (-> db ::jdbc-opts ::query-opts)
        sql        (sql/sql statement)]
    (case op
      :select                    (query db sql query-opts)
      :intersect                 (query db sql query-opts)
      :except                    (query db sql query-opts)
      :union                     (query db sql query-opts)
      :with                      (query db sql query-opts)
      :explain                   (query db sql query-opts)
      :insert                    (if (:returning statement)
                                   (query db sql query-opts)
                                   (execute! db sql))
      :delete                    (if (:returning statement)
                                   (query db sql query-opts)
                                   (execute! db sql))
      :update                    (if (:returning statement)
                                   (query db sql query-opts)
                                   (execute! db sql))
      :copy                      (execute! db sql)
      :create-table              (execute! db sql)
      :drop-table                (execute! db sql)
      :drop-materialized-view    (execute! db sql)
      :refresh-materialized-view (execute! db sql)
      :truncate                  (execute! db sql))))

(defn identifiers [identifier]
  (-> identifier
      (clojure.string/lower-case)
      (.replace \_ \-)))

(defn db
  "Creates a new database handle for sqlingvo-jdbc.

  Arguments:

  spec - The database spec used to connect to your database with clojure.java.jdbc

  opts - Options for configuring SQLingvo-JDBC.
  There are two keys for configuring how SQLingvo-JDBC works:

  ::sqlingvo-opts - The options passed to sqlingvo's Database
  record (usually you can leave this blank, unless you are using MySQL and need
  to change the :sql-quote option to sqlingvo.util/sql-quote-backtick)

  ::jdbc-opts - Options for clojure.java.jdbc functions.

  Default ::sqlingvo-opts:
  - :sql-quote sqlingvo.util/sql-double-quote-quote
  - :sql-name  sqlingvo.util/sql-name-underscore

  Default ::jdbc-opts:
  - ::query-opts These are options passed to clojure.java.jdbc/query when executing SQLingvo statements
    - :identifiers fm.land.sqlingvo-jdbc/identifiers - (this converts field names from the database to be kebob lower case)"
  ([spec] (db spec {}))
  ([spec opts]
   (let [sqlingvo-opts (merge {:sql-quote sql-util/sql-quote-double-quote
                               :eval-fn   #'sqlingvo-eval
                               :sql-name  sql-util/sql-name-underscore}
                              (::sqlingvo-opts opts))
         jdbc-opts     (merge {::query-opts {:identifiers identifiers}}
                              (::jdbc-opts opts))]
     (sql-db/map->Database (merge {::jdbc-opts jdbc-opts
                                   ::spec      spec}
                                  sqlingvo-opts)))))

(comment
  (def d (db "jdbc:postgresql://postgres:gnome@localhost:5432/sqlingvo"))
  (def uppercase-d (db "jdbc:postgresql://postgres:gnome@localhost:5432/sqlingvo" {::jdbc-opts {::query-opts {:identifiers #(clojure.string/upper-case %)}}}))

  @(sql/drop-table d [:products])
  @(sql/drop-table d [:films])

  @(sql/create-table d :products
     (sql/column :id :bigserial :primary-key? true)
     (sql/column :name :varchar)
     (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
     (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now)))

  @(sql/create-table d :films
     (sql/column :code :char :length 5 :primary-key? true)
     (sql/column :title :varchar :length 40 :not-null? true)
     (sql/column :did :integer :not-null? true)
     (sql/column :date-prod :date)
     (sql/column :UPPER-CASE-DATE :date)
     (sql/column :kind :varchar :length 10)
     (sql/column :len :interval)
     (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
     (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now)))

  (try
    @(sql/insert d :films []
       (sql/values [{:code (str (rand-int 1000)) :title "The Room" :did 42}]))
    (catch Exception e
      (println (.. e getNextException))))

  @(sql/select d [:*]
     (sql/from :films))

  (with-db-connection [conn d]
    (execute! conn ["INSERT INTO products (name) VALUES ('Oh Hai Mark')"])
    (query conn ["SELECT * FROM products"]))

  (with-db-connection [conn uppercase-d]
    (execute! conn ["INSERT INTO products (name) VALUES ('Oh Hai Mark')"])
    (query conn ["SELECT * FROM products"]))

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

  @(sql/select d [:*] (sql/from :products))
  (with-db-transaction [tx d]
    @(sql/insert tx :products []
       (sql/values [{:name "INSERT FROM SQLINGVO"}]))
    ;; (db-set-rollback-only! tx)
    @(sql/select tx [:*]
       (sql/from :products)))

  (with-db-connection [conn d]
    (with-db-transaction [tx conn]
      @(sql/select tx [:*] (sql/from :products))
      @(sql/delete tx :products (sql/where '(ilike :name "%SQLINGVO")))
      @(sql/select tx [:*] (sql/from :products))))

  @(sql/intersect (sql/select d [:name] (sql/from :products))
                  (sql/select d [:title] (sql/from :films)))

  @(sql/explain d (sql/intersect (sql/select d [:name] (sql/from :products))
                                 (sql/select d [:title] (sql/from :films))))
  @(sqlingvo/truncate d [:products :films])

  @(sql/union (sql/select d [:name] (sql/from :products))
              (sql/select d [:title] (sql/from :films)))

  @(sql/with d [:bwah (sql/select d [:* (sql/as "Oh HAI MARK" :tommy)]
                        (sql/from :products))]
     (sql/select d [:*] (sql/from :bwah)))

  (with-db-metadata [metadata d]
    (let [table-info (jdbc/metadata-query (.getTables metadata
                                                      nil nil nil
                                                      (into-array ["TABLE" "VIEW"])))]
      table-info))


  (let [{:keys [bwah] :or {bwah "oh hai"}} {}]
    bwah))
