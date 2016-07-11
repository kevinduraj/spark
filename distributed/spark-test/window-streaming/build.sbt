import AssemblyKeys._

name := "kafka_window"

version := "0.1"

val sparkVersion = "1.6.0"
val sparkCassandraVersion = "1.5.0-M3"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion exclude ("org.spark-project.spark","unused")

resolvers += Resolver.sonatypeRepo("public")

//We do this so that Spark Dependencies will not be bundled with our fat jar but will still be included on the classpath
//When we do a sbt/run

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assemblySettings


