package practica_fcll

import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class Practica_entregaTest extends FlatSpec {

  // 1. Configuración de Windows (Hadoop)
  System.setProperty("hadoop.home.dir", "C:\\Users\\fablo\\hadoop")

  // 2. Definición de la sesión (Solo esta, borra las demás)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("TestPractica")
    .getOrCreate()

  // 3. Import para habilitar .toDF()
  import spark.implicits._

  "Ejercicio 1" should "filtrar notas > 8 y ordenar de mayor a menor" in {
    // 1. Datos de prueba: Ana debería salir primero, Maria segunda, y Pedro fuera.
    val datos = Seq(
      ("Pedro", 22, 6.0),
      ("Ana", 20, 9.5),
      ("Maria", 21, 8.5)
    ).toDF("nombre", "edad", "calificacion")

    // 2. Ejecutamos tu función
    val resultado = Practica_entrega.ejercicio1(datos)

    // 3. Recogemos los nombres para comprobar el orden
    val nombres = resultado.select("nombre").as[String].collect()

    // 4. Verificaciones
    assert(nombres.length == 2, "Deberían quedar solo 2 estudiantes")
    assert(nombres(0) == "Ana", "Ana debería ser la primera por tener 9.5")
    assert(nombres(1) == "Maria", "Maria debería ser la segunda por tener 8.5")
  }
  "Ejercicio 2" should "identificar correctamente números pares e impares" in {
    // 1. Creamos datos de prueba
    val dfNumeros = Seq(1, 2, 10, 15).toDF("numero")

    // 2. Ejecutamos tu función
    val resultado = Practica_entrega.ejercicio2(dfNumeros)

    // 3. Recogemos los resultados para validar
    // Convertimos a un mapa (numero -> resultado) para que sea fácil de testear
    val mapaRes = resultado.collect().map(row =>
      row.getAs[Int]("numero") -> row.getAs[String]("resultado")
    ).toMap

    // 4. Verificaciones
    assert(mapaRes(1) == "Impar")
    assert(mapaRes(2) == "Par")
    assert(mapaRes(10) == "Par")
    assert(mapaRes(15) == "Impar")
  }
  "Ejercicio 3" should "hacer un join y calcular el promedio por estudiante" in {
    // 1. Tabla de estudiantes
    val dfEstudiantes = Seq(
      (1, "Ana"),
      (2, "Pedro"),
      (3, "Juan")
    ).toDF("id", "nombre")

    // 2. Tabla de calificaciones (Ana tiene dos notas, Pedro una)
    val dfNotas = Seq(
      (1, "Matematicas", 10.0),
      (1, "Programacion", 8.0),
      (2, "Matematicas", 6.0),
      (3, "Programación", 7.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    // 3. Ejecutar función
    val resultado = Practica_entrega.ejercicio3(dfEstudiantes, dfNotas)

    // 4. Verificaciones
    val mapaPromedios = resultado.collect().map(row =>
      row.getAs[String]("nombre") -> row.getAs[Double]("promedio_final")
    ).toMap

    assert(mapaPromedios("Ana") == 9.0, "El promedio de Ana debería ser 9.0")
    assert(mapaPromedios("Pedro") == 6.0, "El promedio de Pedro debería ser 6.0")
    assert(mapaPromedios("Juan") == 7.0, "El promedio de Pedro debería ser 7.0")
  }
  "Ejercicio 4" should "contar las ocurrencias de cada palabra en un RDD" in {
    // 1. Lista de palabras con repeticiones
    val listaPalabras = List("hola", "mundo", "hola", "spark", "mundo", "hola")

    // 2. Ejecutar función (importante pasar la lista)
    val rddResultado = Practica_entrega.ejercicio4(listaPalabras)

    // 3. Recogemos los resultados del RDD a una lista local (collect) y luego a un mapa
    val resultados = rddResultado.collect().toMap

    // 4. Verificaciones
    assert(resultados("hola") == 3, "La palabra 'hola' debería aparecer 3 veces")
    assert(resultados("mundo") == 2, "La palabra 'mundo' debería aparecer 2 veces")
    assert(resultados("spark") == 1, "La palabra 'spark' debería aparecer 1 vez")
  }
  "Ejercicio 5" should "cargar el CSV y calcular el ingreso total por producto" in {
    // Usamos una ruta relativa que funcione para cualquiera que descargue el repo
    // Esta ruta asume que el archivo está dentro de la estructura de carpetas del proyecto
    val rutaRelativa = "src/main/scala/examen_estructura/ventas.csv"

    val resultado = Practica_entrega.ejercicio5(rutaRelativa)

    // Mostrar el resultado en consola para verificar visualmente
    resultado.show()

    // Verificación básica: El DataFrame no debe estar vacío
    assert(resultado.count() > 0)
  }
}


