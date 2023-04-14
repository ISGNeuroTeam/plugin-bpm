
name := "plugin-bpm"

description := "Plugin for business process mining"

version := "1.0.2"

scalaVersion := "2.11.12"

resolvers +=
  ("Sonatype OSS Snapshots" at "http://storage.dev.isgneuro.com/repository/ot.platform-sbt-releases/")
    .withAllowInsecureProtocol(true)

val dependencies = new {
  private val smallPluginSdkVersion = "0.3.0"
  private val sparkVersion = "2.4.3"

  val smallPluginSdk = "ot.dispatcher.plugins.small" % "smallplugin-sdk_2.11" % smallPluginSdkVersion % Compile
  val sparkMlLib = "org.apache.spark" %% "spark-mllib" % sparkVersion % Compile
}

libraryDependencies ++= Seq(
  dependencies.smallPluginSdk,
  dependencies.sparkMlLib
)
//libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.1.1" % Compile
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case _                                             => MergeStrategy.concat
}

parallelExecution in Test := false
