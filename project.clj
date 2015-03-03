(defproject trident-gcd "0.0.1-wip1"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :dependencies [
                 [com.google.apis/google-api-services-datastore-protobuf "v1beta2-rev1-2.1.0"]
                ]
  :profiles {
      :provided {
      :dependencies [[storm "0.9.0-wip15"]
                     [org.clojure/clojure "1.4.0"]
                    ]}}
  )
