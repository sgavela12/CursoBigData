import org.apache.spark.sql.SparkSession
import example.BankDataset

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ConexionMySQL")
      .master("local[*]")
      .getOrCreate()

    // BaseDatos.leerTabla(spark)
    BankDataset.ejercicios(3, spark)

    spark.stop()
  }
}
