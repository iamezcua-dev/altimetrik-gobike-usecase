name := "altimetrik-gobike-usecase"
version := "0.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

resolvers ++= Seq(
	"MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
	// Testing
	"org.scalatest" %% "scalatest" % "3.0.8" % "test",
	// Apache Commons IO
	"commons-io" % "commons-io" % "2.6"

)