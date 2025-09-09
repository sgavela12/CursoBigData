import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object MnMCount {
  
  private var dfMnM: DataFrame = _

  def init(spark: SparkSession, rutaCSV: String): Unit = {
    dfMnM = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaCSV)
  }

  // Ejercicio 1: Agrupar por estado y color
  def ejercicio1(): Unit = {
    val dfFiltrado = dfMnM
      .groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))
      .orderBy(desc("Total"))

    dfFiltrado.show()
  }

  // Ejercicio 2: Filtrar solo California
  def ejercicio2(): Unit = {
    val dfFiltrado = dfMnM
      .filter(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))
      .orderBy(desc("Total"))

    dfFiltrado.show()
  }
}
