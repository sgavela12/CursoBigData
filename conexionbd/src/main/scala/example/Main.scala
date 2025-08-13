import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ConexionMySQL")
      .master("local[*]")
      .getOrCreate()

    // primer parametro para cambiar el numero del ejercicio
    BaseDatos.ejercicios(4,spark)



    spark.stop()
  }
}
