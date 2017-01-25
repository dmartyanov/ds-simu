scalaVersion := "2.11.8"

libraryDependencies ++= akka("2.4.16")

def akka(v: String) = Seq(
  "com.typesafe.akka" %% "akka-actor" % v,
  "com.typesafe.akka" %% "akka-stream" % v,
  "com.typesafe.akka" % "akka-cluster_2.11" % v,
  "com.typesafe.akka" % "akka-distributed-data-experimental_2.11" % v
)
