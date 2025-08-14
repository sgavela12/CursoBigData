package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BankDataset {

  def ejercicios(numero: Int, spark: SparkSession): Unit = {
    numero match {
      case 1 => ejercicio1(spark)
      case 2 => ejercicio2(spark)
      case 3 => ejercicio3(spark)
      // case 4 => ejercicio4(spark)
      case _ => println("Número de ejercicio no válido")
    }
  }

  def leerCSV(spark: SparkSession): DataFrame = {
    val bankCSV = "data/bank_dataset.csv"

    spark.read
      .option("header", "true")
      .csv(bankCSV)
  }
  // EJERCICIO 1
// Muestra el número de clientes por estado civil

  def ejercicio1(spark: SparkSession): Unit = {
    val bankDf = leerCSV(spark)
    bankDf
      .groupBy("marital")
      .count()
      .show()
  }
  // EJERCICIO 2
// Promedio de balance por trabajo

  def ejercicio2(spark: SparkSession): Unit = {
    val bankDf =
      leerCSV(spark).withColumn("balance", col("balance").cast("int"))

    val resultado = bankDf
      .groupBy("job")
      .agg(round(avg("balance"), 2).alias("media_balance"))

    resultado.show()
  }

  // EJERCICIO 3
//Lee el dataset del banco desde el CSV.
// Convierte la columna balance a tipo numérico (Int).
// Crea una nueva columna llamada total_assets que represente los activos totales del cliente:
// Suma el balance con un valor fijo de 1000 si el cliente tiene housing = "yes" (de lo contrario suma 0).
// Filtra el DataFrame para quedarte solo con los clientes cuyo total_assets sea mayor a 1000.
// Muestra el resultado.

  def ejercicio3(spark: SparkSession): Unit = {
val bankDf = leerCSV(spark)
  .withColumn("balance", col("balance").cast("int"))

val bankDf2 = bankDf.withColumn(
  "total_assets", 
  col("balance") + when(col("housing") === "yes", 1000).otherwise(0)
)

val bankDfFiltered = bankDf2.filter(col("total_assets") > 1000)

bankDfFiltered.show()
  }
}
