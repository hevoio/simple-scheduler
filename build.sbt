name := "simple-scheduler"

scalaVersion := "2.11.12"

organization := "com.hevodata"
organizationName := "Hevo Data Inc"
organizationHomepage := Some(url("https://hevodata.com/"))

homepage := Some(url("https://github.com/hevoio/simple-scheduler"))
description := "A simple to use, lightweight clustered scheduler for Java/Scala. It can be used for job processing also, but it is recommended that the operations are restricted to light-weight processing only."
scmInfo := Some(ScmInfo(url("https://github.com/hevoio/simple-scheduler"), "git@github.com:hevoio/simple-scheduler.git"))
developers := List(Developer("tj---", "Trilok Jain", "trilok@hevodata.com", url("https://github.com/tj---")),Developer("sar009", "Sarad Mohanan", "sarad@hevodata.com", url("https://sarad.in")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

crossPaths := false

//publishTo := Some(
//  if (isSnapshot.value)
//    Opts.resolver.sonatypeSnapshots
//  else
//    Opts.resolver.sonatypeStaging
//)

publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("releases" at nexus + "service/local/staging/deploy/maven2/")
  else Some("snapshots" at nexus + "content/repositories/snapshots/")
}


publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
pomIncludeRepository := { _ => false }

isSnapshot := true

import ReleaseTransformations._
releaseCrossBuild := true
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
//  setNextVersion,
//  commitNextVersion,
//  releaseStepCommand("sonatypeRelease"),
//  pushChanges
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "com.cronutils" % "cron-utils" % "5.0.5",
  "redis.clients" % "jedis" % "2.9.0",
  "com.datadoghq" % "java-dogstatsd-client" % "2.3",

  "junit" % "junit" % "4.13.2" % Test,
  "com.wix" % "wix-embedded-mysql" % "4.6.1" % Test,
  "org.apache.commons" % "commons-dbcp2" % "2.8.0" % Test,
  "mysql" % "mysql-connector-java" % "8.0.16" % Test
)

scalacOptions += "-target:jvm-1.8"