package practica_fcll

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Practica_entrega {

  /**
   * Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * - Muestra el esquema.
   * - Filtra estudiantes con calificación > 8.
   * - Selecciona nombres y ordena por calificación descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // 1. Mostrar el esquema en consola
    estudiantes.printSchema()

    // 2. Aplicar transformaciones
    estudiantes
      .filter(col("calificacion") > 8)
      .select("nombre", "calificacion") // Seleccionamos ambos para poder ordenar
      .sort(col("calificacion").desc)
  }
  /**
   * Ejercicio 2: UDF (User Defined Function)
   * Define una función que determine si un número es par o impar y
   * aplícala a una columna de un DataFrame.
   */
  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // 1. Definimos la lógica de la función (UDF)
    val determinarParOImpar = udf((n: Int) => if (n % 2 == 0) "Par" else "Impar")

    // 2. Aplicamos la UDF creando una nueva columna llamada "resultado"
    // Asumimos que la columna de entrada se llama "numero"
    numeros.withColumn("resultado", determinarParOImpar(col("numero")))
  }
  /**
   * Ejercicio 3: Joins y agregaciones
   * Une estudiantes con sus calificaciones y calcula el promedio por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    // 1. Realizar el Join usando las columnas de ID
    // Usamos el ID del estudiante para cruzar con el id_estudiante de las notas
    val unidoDF = estudiantes.join(
      calificaciones,
      estudiantes("id") === calificaciones("id_estudiante")
    )

    // 2. Agrupar por nombre y calcular el promedio (avg)
    unidoDF
      .groupBy("nombre")
      .agg(avg("calificacion").as("promedio_final"))
  }
  /**
   * Ejercicio 4: Uso de RDDs
   * Crea un RDD a partir de una lista de palabras y cuenta las ocurrencias.
   */
  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    // 1. Convertimos la lista de Scala en un RDD usando sparkContext
    val rddInicial: RDD[String] = spark.sparkContext.parallelize(palabras)

    // 2. Transformamos cada palabra en una tupla (palabra, 1)
    // 3. Agrupamos por la clave (la palabra) y sumamos los unos
    rddInicial
      .map(palabra => (palabra, 1))
      .reduceByKey(_ + _)
  }
  /**
   * Ejercicio 5: Procesamiento de archivos
   * Carga un CSV de ventas y calcula el ingreso total por producto.
   */
  def ejercicio5(rutaArchivo: String)(implicit spark: SparkSession): DataFrame = {
    // 1. Cargar el archivo CSV con cabecera e inferencia de tipos
    val ventasDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(rutaArchivo)

    // 2. Calcular el ingreso por fila y agrupar por producto
    ventasDF
      .withColumn("ingreso", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso").as("ingreso_total"))
  }
}
