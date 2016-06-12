(defproject fm.land.sqlingvo-jdbc "0.1.0-SNAPSHOT"
  :description "Integration library between SQLingvo and clojure.java.jdbc"
  :url "https://github.com/caleb/sqlingvo-jdbc"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [sqlingvo "0.8.13"]
                 [org.clojure/java.jdbc "0.6.1"]]
  :profiles {:dev {:dependencies [[org.postgresql/postgresql "9.4.1208.jre7"]]}})
