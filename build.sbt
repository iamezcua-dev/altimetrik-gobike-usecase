name := "altimetrik-gobike-usecase"
version := "0.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

resolvers ++= Seq(
	"MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	// logging
	"org.apache.logging.log4j" % "log4j-api" % "2.4.1",
	"org.apache.logging.log4j" % "log4j-core" % "2.4.1",
	"org.apache.hadoop" % "hadoop-hdfs" % "3.2.1",
	"org.scalatest" %% "scalatest" % "3.0.8" % "test",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
	"org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.12.1"

)