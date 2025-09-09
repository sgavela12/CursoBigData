import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MnMcount Spark App")
      .master("local[*]") 
      .getOrCreate()

    val rutaCSV = "data/raw/mnm_dataset.csv"

    MnMCount.init(spark, rutaCSV)

    MnMCount.ejercicio1()
    MnMCount.ejercicio2()

    spark.stop()
  }
}
