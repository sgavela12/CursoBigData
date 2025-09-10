import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {



    // rutas
  val userCSV = "data/raw/googleplaystore_user_review.csv"
  val playStoreCSV = "data/raw/googleplaystore.csv"
  val quijote = "data/raw/el_quijote.txt"


  val spark = SparkSession
    .builder()
    .appName("TransformarCSV")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val dfUsers = spark.read.option("header", "true").csv(userCSV)
  val dfPlayStore = spark.read.option("header", "true").csv(playStoreCSV)
  val quijoteRDD = spark.sparkContext.textFile(quijote)
    

  val ejercicio = "8" 

    ejercicio match {                                                                   
      case "1"   => EjerciciosDataFrame.ejercicio1(dfPlayStore)
      case "2"   => EjerciciosDataFrame.ejercicio2(dfPlayStore)
      case "3"   => EjerciciosDataFrame.ejercicio3(dfPlayStore)
      case "4"   => EjerciciosDataFrame.ejercicio4(dfPlayStore)
      case "5"   => EjerciciosDataFrame.ejercicio5(dfPlayStore)
      case "6a"  => EjerciciosDataFrame.ejercicio6a(dfPlayStore)
      case "6b"  => EjerciciosDataFrame.ejercicio6b(dfPlayStore)
      case "6c"  => EjerciciosDataFrame.ejercicio6c(dfPlayStore)
      case "7"   => EjerciciosDataFrame.ejercicio7(dfPlayStore)
      case "8"   => LearningSpark2_ejercicios.ejercicioSpark1(quijoteRDD)
      case _     => println(s"Ejercicio '$ejercicio' no reconocido.")
    }
  }
}
