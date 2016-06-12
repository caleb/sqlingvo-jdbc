# SQLingvo-JDBC

A Clojure library designed to allow you to execute your [SQLingvo][] queries
directly.

This library uses a feature of SQLingvo that enables you to specify a function
to execute when a query is dereferenced.

SQLingvo-JDBC has its own sqlingvo.db.Database record where it stores connection
information about your database, and we wrap each of the clojure.java.jdbc
functions that take a database so that you can use this record instead of a
plain db-spec.

Despite all the wrapping, this library is very simple. The core Database record
includes the standard `:sql-quote` option, and our own custom `eval-fn` option.
Other than that, we store the usual clojure.java.jdbc spec in
:fm.land.sqlingvo-jdbc/spec key.

The wrapped clojure.java.jdbc functions simple call the parent clojure.java.jdbc
version with this embedded spec.

## Field Names

By default SQLingvo-JDBC transforms dashes in column names from Clojure to
underscores when building SQL statements.

To change this specify a function in the `:sql-name` key of the sqlingvo
options. For example, if you wanted to change all fields to have uppercase
names:

```clojure
(sqlingvo-jdbc/db db-spec {:sql-name string/upper-case})
```



## Usage

Just use the fm.land.sqlingvo versions of the corresponding clojure.java.jdbc
functions and pass the database spec from the `db` function:

```clojure
(require '([fm.land.sqlingvo-jdbc :as sqlingvo-jdbc]
           [sqlingvo.core :as sql]))
;;
;; You can pass in a plain string to the `db` function, or a map like you would to clojure.java.jdbc/get-connection
;;
(let [d (sqlingvo-jdbc/db "jdbc:postgresql://username:password@localhost:5432/some_database")]
  ;; Just deref (use the @) on a SQLingvo statement to execute it
  @(sql/select d [:*]
     (sql/from :users))

  ;; This will start a transaction and rollback the changes
  (sqlingvo-jdbc/with-db-transaction [tx d]
    @(sql/insert tx :users []
       (sql/values [{:name "Ada Lovelace"}]))
    (sqlingvo-jdbc/db-set-rollback-only! tx))

   ;; You can use the same functions you'd expect from clojure.java.jdbc
   (sqlingvo-jdbc/query d ["SELECT * FROM users"]))
```

See the [JDBC Documentation][jdbc-spec-docs] for more information about setting
up a data source. Any data source configuration supported by clojure.java.jdbc
should work with SQLingvo-JDBC.

## License

Copyright © 2016 Caleb Land

Distributed under the MIT License.

[SQLingvo]: https://github.com/r0man/sqlingvo
[jdbc-spec-docs]: http://clojure-doc.org/articles/ecosystem/java_jdbc/home.html#setting-up-a-data-source
