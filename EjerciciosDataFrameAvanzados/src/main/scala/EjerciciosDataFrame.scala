  import org.apache.spark.sql.{SparkSession, DataFrame}
  import org.apache.spark.sql.functions._

  object EjerciciosDataFrame {

    

    // ------EJERCICIOS------

    // EJERCICIO 1

    def ejercicio1(dfPlayStore : DataFrame): Unit = {

      val dfFiltrado =
        dfPlayStore.filter(col("App") =!= "Life Made WI-FI Touchscreen Photo Frame")

      dfFiltrado.show()

    }

    // EJERCICIO 2

    def ejercicio2(dfPlayStore : DataFrame): Unit = {

      // Rellenar valores nulos o NaN en Rating con 0.0
      val dfConCeros = dfPlayStore.na.fill(Map("Rating" -> 0.0))

      dfConCeros.show()

    }

    // EJERCICIO 3

    def ejercicio3(dfPlayStore : DataFrame): Unit = {

      val dfUnknown = dfPlayStore.na.fill(Map("Type" -> "Unknown"))

      dfUnknown.show()

    }

    // EJERCICIO 4

    def ejercicio4(dfPlayStore : DataFrame): Unit = {

      val df2 = dfPlayStore.withColumn(
        "Varia",
        when(col("Android Ver") === ("Varies with device"), true).otherwise(false)
      )
      df2.show()

    }

    // EJERCICIO 5
    def ejercicio5(dfPlayStore : DataFrame): Unit = {

      val dfFrec_DowLoads = dfPlayStore.withColumn(
        "Frec_DownLoads",
        when(col("Installs") >= 0 && col("Installs") < 50000, "Baja")
          .when(col("Installs") >= 50000 && col("Installs") < 1000000, "Media")
          .when(col("Installs") >= 1000000 && col("Installs") < 50000000, "Alta")
          .when(col("Installs") >= 50000000, "Muy alta")
          .otherwise("Algo ha ido mal")
      )
      dfFrec_DowLoads.show()

    }

    // EJERCICIO 6A
    def ejercicio6a(dfPlayStore : DataFrame): Unit = {

      val dfFrec_DowLoads = dfPlayStore.withColumn(
        "Frec_DownLoads",
        when(col("Installs") >= 0 && col("Installs") < 50000, "Baja")
          .when(col("Installs") >= 50000 && col("Installs") < 1000000, "Media")
          .when(col("Installs") >= 1000000 && col("Installs") < 50000000, "Alta")
          .when(col("Installs") >= 50000000, "Muy alta")
          .otherwise("Algo ha ido mal")
      )
      val bestApp = dfFrec_DowLoads
        .filter(
          col("Frec_DownLoads") === "Muy alta"
            && col("Rating") >= 4.5
        )
        .select("App", "Rating", "Frec_DownLoads")

      bestApp.show()

    }

    // EJERCICIO 6B
    def ejercicio6b(dfPlayStore : DataFrame): Unit = {

      val dfFrec_DowLoads = dfPlayStore.withColumn(
        "Frec_DownLoads",
        when(col("Installs") >= 0 && col("Installs") < 50000, "Baja")
          .when(col("Installs") >= 50000 && col("Installs") < 1000000, "Media")
          .when(col("Installs") >= 1000000 && col("Installs") < 50000000, "Alta")
          .when(col("Installs") >= 50000000, "Muy alta")
          .otherwise("Algo ha ido mal")
      )
      val bestFreeApp = dfFrec_DowLoads
        .filter(
          col("Frec_DownLoads") === "Muy alta"
            && col("Price") == 0
        )
        .select("App", "Price", "Frec_DownLoads")

      bestFreeApp.show()

    }

    // EJERCICIO 6C

    def ejercicio6c(dfPlayStore : DataFrame): Unit = {

      dfPlayStore.filter(col("Price") < 13).show()

    }
    // EJERCICIO 7
    def ejercicio7(dfPlayStore : DataFrame): Unit = {
      val dfSample = dfPlayStore.sample(withReplacement = false, fraction = 0.1, seed = 123)
  dfSample.show()
    }
  }
