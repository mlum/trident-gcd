(defproject trident-gcd "0.0.1-wip3"
  :description "This library implements a Trident state on top of the Google Cloud Datastore."
  :url "http://storm.apache.org/documentation/Trident-state.html"
  :license {:name "Apache License"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:unchecked"]
  :dependencies [
                 [com.google.apis/google-api-services-datastore-protobuf "v1beta2-rev1-2.1.0"]
                ]
  :profiles {
      :provided {
      :dependencies [[storm "0.9.0-wip15"]
                     [org.clojure/clojure "1.4.0"]
                    ]}}
  )
