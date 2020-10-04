name := "TwitterSpark"
version := "1.0"
scalaVersion := "2.12.3"
libraryDependencies ++= Seq( "org.apache.spark" % "spark-streaming_2.12" % "3.0.1" % "provided", "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.0.1", "org.apache.spark" % "spark-core_2.12" % "3.0.1" % "provided" 
resolvers ++= Seq( "Maven Central" at "repo1.maven.org/maven2", "Kafka Central" at "mvnrepository.com/artifact" ) 