name := "simple-scheduler"

scalaVersion := "2.11.12"

organization := "com.hevodata"
homepage := Some(url("https://github.com/hevoio/simple-scheduler"))
scmInfo := Some(ScmInfo(url("https://github.com/dataoperandz/cassper"), "git@github.com:hevoio/simple-scheduler.git"))
developers := List(Developer("tj---", "Trilok Jain", "trilok@hevodata.com", url("https://github.com/tj---")),Developer("sar009", "Sarad Mohanan", "sarad@hevodata.com", url("https://github.com/tj---")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

crossPaths := false

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

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
  setNextVersion,
  commitNextVersion,
  releaseStepCommand("sonatypeRelease"),
  pushChanges
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "com.cronutils" % "cron-utils" % "5.0.5",
  "redis.clients" % "jedis" % "2.9.0",

  "junit" % "junit" % "4.13.2" % Test,
  "com.wix" % "wix-embedded-mysql" % "4.6.1" % Test,
  "org.apache.commons" % "commons-dbcp2" % "2.8.0" % Test,
  "mysql" % "mysql-connector-java" % "8.0.16" % Test
)

scalacOptions += "-target:jvm-1.8"