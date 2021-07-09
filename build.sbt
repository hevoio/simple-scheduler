name := "simple-scheduler"

version := "0.1.3"

scalaVersion := "2.11.12"

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