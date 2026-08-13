import sbtassembly.MergeStrategy

ThisBuild / organization := "fr.dpelab"
ThisBuild / version := "1.0.0"

// Spark 3.5 est publié pour Scala 2.12 et 2.13. On reste sur 2.12, encore la
// cible par défaut des distributions Spark et des clusters managés.
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "dpe-spark-jobs",
    libraryDependencies ++= Seq(
      // "provided" : ces jars sont fournis par le cluster, on ne les embarque pas
      // dans le fat jar (sinon ~300 Mo au lieu de ~15 Mo, et des conflits de version).
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % Provided,

      "com.github.scopt" %% "scopt" % "4.1.0",

      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    ),
    // Spark sur JDK 17 exige l'ouverture explicite de modules internes, sinon
    // les tests échouent sur des InaccessibleObjectException dans le sérialiseur.
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      // Requis dès qu'un test manipule une colonne de type date : Spark passe
      // par sun.util.calendar.ZoneInfo, encapsulé depuis le JDK 16.
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Xmx3g"
    ),
    // Les tests Spark partagent une SparkSession : les faire tourner en parallèle
    // provoque des conflits de port et de répertoire temporaire.
    Test / fork := true,
    Test / parallelExecution := false,

    assembly / assemblyJarName := "dpe-spark-jobs.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "reference.conf"                     => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    }
  )
