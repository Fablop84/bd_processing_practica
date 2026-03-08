ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "practica-fcll-entrega"
  )

libraryDependencies ++= Seq(
  // Librería para los tests
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,

  // Librerías de Spark (SIN "provided" para que IntelliJ las descargue ahora)
  "org.apache.spark" %% "spark-core" % "3.5.6",
  "org.apache.spark" %% "spark-sql" % "3.5.6"
)
