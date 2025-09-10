object LearningSpark2_ejercicios {

  import org.apache.spark.rdd.RDD

  def ejercicioSpark1(quijoteRDD: RDD[String]): Unit = {

    //  Contar el número de líneas
    val numLineas = quijoteRDD.count()
    println(s"-------El archivo tiene $numLineas lineas.-------")

    //  Obtener la primera línea
    val primeraFila = quijoteRDD.first()
    println(s"-------La primera linea del archivo es:  $primeraFila .-------")

    //  Obtener las primeras 5 líneas (equivalente a head(n) en DataFrame)
    val primeras5 = quijoteRDD.take(5)
    println("-------Primeras 5 líneas-------")
    primeras5.foreach(println)

    //  Mostrar 10 líneas (equivalente a show(10) en DataFrame)
    val primeras10 = quijoteRDD.take(10)
    println("-------Primeras 10 líneas-------")
    primeras10.foreach(println)
  }
}
