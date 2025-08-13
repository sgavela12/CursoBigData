import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BaseDatos {

  def leerTablaPersonas(spark: SparkSession): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/prueba_spark"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "1234")
    connectionProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val df: DataFrame =
      spark.read.jdbc(jdbcUrl, "personas", connectionProperties)
    df.show()
  }

  def ejercicios(numero: Int, spark: SparkSession): Unit = {
    numero match {
      case 1 => ejercicio1(spark)
      case 2 => ejercicio2(spark)
      case 3 => ejercicio3(spark)
      case _ => println("Número de ejercicio no válido")
    }

  }

  def leeVentas(spark: SparkSession): DataFrame = {
    val ventas = spark.read
      .format("jdbc")
      .option(
        "url",
        "jdbc:mysql://localhost:3306/prueba_spark?useUnicode=true&characterEncoding=UTF-8"
      )
      .option("dbtable", "ventas_detalladas")
      .option("user", "root")
      .option("password", "1234")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load()
    ventas

  }

// EJERCICIO 1
// Top clientes por ciudad
// Para cada ciudad, muestra los 3 clientes con mayor total de compras (cantidad * precio), usando funciones de ventana.
  def ejercicio1(spark: SparkSession): Unit = {
    val ventas = leeVentas(spark)
    ventas
      .withColumn("total_ventas", col("cantidad") * col("precio"))
      .withColumn(
        "puesto",
        rank().over(
          Window.partitionBy("ciudad").orderBy(col("total_ventas").desc)
        )
      )
      .filter(col("rank") <= 3)
      .select("ciudad", "cliente", "total_ventas", "puesto")
      .show()

  }

// EJERCICIO 2
// Para cada producto, calcula la cantidad acumulada y el precio acumulado a lo largo del tiempo.
  def ejercicio2(spark: SparkSession): Unit = {
    val ventas = leeVentas(spark)

    val ventana = Window.partitionBy("producto").orderBy("fecha")

    ventas
      .withColumn("cantidad_acumulada", sum("cantidad").over(ventana))
      .withColumn("precio_acumulado", sum("precio").over(ventana))
      .show()
  }

// // EJERCICIO 3
// Para cada ciudad, calcula el total de compras de cada producto por cliente (cantidad * precio).

  def ejercicio3(spark: SparkSession): Unit = {

    val ventas = leeVentas(spark) // tu DataFrame de ventas_detalladas

    val resultado = ventas
      .withColumn("total_compra", col("cantidad") * col("precio"))
      .groupBy("ciudad", "cliente", "producto")
      .agg(sum("total_compra").alias("total_compra"))
      .orderBy("ciudad", "cliente", "producto")

    resultado.show()

  }
}
