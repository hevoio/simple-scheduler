name := "scheduler"

version := "0.1"

scalaVersion := "2.13.5"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "com.cronutils" % "cron-utils" % "9.1.5",
  "redis.clients" % "jedis" % "2.9.0",
  "org.jdbi" % "jdbi3-core" % "3.12.2",
  "org.jdbi" % "jdbi3-sqlobject" % "3.12.2",

  "junit" % "junit" % "4.13.2" % Test
)
