name := "LogFile Data Analysis"
version := "1.0"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2" % "provided"
libraryDependencies += "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3" excludeAll(ExclusionRule(organization = "org.apache.hbase", name = "hbase-server"), ExclusionRule(organization = "javax.servlet", name = "javax.servlet-api"), ExclusionRule(organization = "org.mortbay.jetty"), ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-core-asl"), ExclusionRule(organization = "tomcat"), ExclusionRule(organization = "junit", name = "junit"), ExclusionRule(organization = "com.sun.jersey", name = "jersey-server"), ExclusionRule(organization = "com.sun.jersey", name = "jersey-core"), ExclusionRule(organization = "commons-io"), ExclusionRule(organization = "commons-cli"), ExclusionRule(organization = "commons-el"), ExclusionRule(organization = "org.apache.commons"), ExclusionRule(organization = "commons-lang"), ExclusionRule(organization = "commons-logging"), ExclusionRule(organization = "commons-codec"), ExclusionRule(organization = "commons-httpclient"), ExclusionRule(organization = "commons-collections"), ExclusionRule(organization = "org.jruby.jcodings"), ExclusionRule(organization = "org.jruby"), ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.yammer.metrics"), ExclusionRule(organization = "com.github.stephenc.findbugs"), ExclusionRule(organization = "log4j"))

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}