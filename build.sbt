name := "mobile-telemetry"
version := "0.0.1"
scalaVersion := "2.10.6"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-optimize",
  "-Yinline-warnings"
)

fork := true

javaOptions += "-Xmx2G"

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", xs @ _*)         => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

libraryDependencies += 
	"org.scalatest" % "scalatest_2.10" % "2.2.6" % "test"
	
libraryDependencies += 
	"com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2" % "test"
	

libraryDependencies += "com.typesafe.akka" % "akka-testkit_2.10" % "2.3.2" % "test"

	
libraryDependencies += 
	"com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.2" % "test" excludeAll(
			ExclusionRule(organization = "javax.servlet.jsp")
	)
	
libraryDependencies += 	
	"org.apache.spark" % "spark-core_2.10" % "1.4.1"  % "provided" 

libraryDependencies +=	
	"org.apache.spark" % "spark-streaming_2.10" % "1.4.1" % "provided"

libraryDependencies +=	
	"org.apache.avro" % "avro" % "1.7.7"

libraryDependencies +=	
	"org.apache.avro" % "avro-mapred" % "1.7.7" % "provided" classifier("hadoop2")

libraryDependencies +=
	"org.apache.hadoop" % "hadoop-common" % "2.7.1"  % "provided"   excludeAll(
				ExclusionRule(organization = "javax.servlet"),
				ExclusionRule(organization = "javax.servlet.jsp"),
				ExclusionRule(organization = "org.eclipse.jetty.orbit"),
				ExclusionRule(organization = "org.mortbay.jetty")
		)
	
libraryDependencies += 
	"com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.7.2"
	
libraryDependencies += 
	"joda-time" % "joda-time" % "2.9.3"
	
libraryDependencies += 
	"com.typesafe" % "config" % "1.2.1"

libraryDependencies +=
	"org.apache.spark" % "spark-streaming-flume_2.10" % "1.4.1"
 
	


    

	
	
	
	
