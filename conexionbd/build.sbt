ThisBuild / scalaVersion     := "2.13.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

// Versión de Spark
lazy val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "ConexionBD",
    
    libraryDependencies ++= Seq(
      // Spark SQL y Core
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      
      // Conector MySQL
      "mysql" % "mysql-connector-java" % "8.0.33",
      
      // Librería para tests
      "org.scalameta" %% "munit" % "0.7.29" % Test
    )
  )
