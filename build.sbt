lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "io.mindfulmachines",
      scalaVersion := "2.11.12",
      crossScalaVersions := Seq("2.10.4", "2.11.12"),
      version      := "1.0-SNAPSHOT"
    )),
    name := "wiki-parser",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.5",
      "org.apache.spark" %% "spark-core" % "1.5.2",
      "org.apache.spark" %% "spark-sql" % "1.5.2",
      "org.apache.spark" %% "spark-mllib" % "1.5.2",
      "org.apache.hadoop" % "hadoop-client" % "2.4.0",
      "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.21",
      "info.bliki.wiki" % "bliki-core" % "3.1.0",
      "net.java.dev.jets3t" % "jets3t" % "0.9.4",
      "org.apache.mahout" % "mahout-integration" % "0.13.0"
    )
  )