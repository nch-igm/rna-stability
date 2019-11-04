lazy val root = (project in file(".")).
  settings(
    name := "rna-stability",
    version := "2.0.0-" + sys.env.getOrElse("BUILD_NUMBER", "DEV"),
    scalaVersion := "2.11.8"
  )

val sparkVersion = "2.4.0" 

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.github.broadinstitute" % "picard" % "2.10.8"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

