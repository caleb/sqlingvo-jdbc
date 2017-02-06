(defproject fm.land/sqlingvo-jdbc "0.1.7-SNAPSHOT"
  :description "Integration library between SQLingvo and clojure.java.jdbc"
  :url "https://github.com/caleb/sqlingvo-jdbc"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [sqlingvo "0.9.0"]
                 [org.clojure/java.jdbc "0.6.1"]]
  :profiles {:dev {:dependencies [[org.postgresql/postgresql "9.4.1208.jre7"]]}}

  :plugins [[s3-wagon-private "1.2.0"]]
  :repositories [["s3" {:url "http://caleb-maven2.s3.amazonaws.com/repo"}]
                 ["s3-releases" {:url "s3p://caleb-maven2/repo" :creds :gpg}]])
