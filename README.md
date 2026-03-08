# bd_processing_practica 🚀

Este repositorio contiene la resolución de la práctica de bd-processing utilizando **Apache Spark** y **Scala**. 

La idea es mostrar cómo usar DataFrames y RDDs para procesar datos de forma sencilla.

## 📋 Contenido del Ejercicio

La práctica se divide en 5 ejercicios clave:

1. **Operaciones con DataFrames:** Creación de esquemas, filtrado de calificaciones (calificaciones > 8) y ordenamiento descendente.
2. **User Defined Functions (UDF):** Realizar una función propia (UDF) en Scala para clasificar números pares e impares en columnas de un DataFrame.
3. **Joins y Agregaciones:** Cruce de datos entre estudiantes y calificaciones mediante `join` para calcular promedios por alumno.
4. **RDDs (Low-level API):** Uso de la API de bajo nivel para implementar un conteo de palabras (*Word Count*) con `map` y `reduceByKey`.
5. **Procesamiento de Archivos:** Procesar datos de ventas para calcular el ingreso total por producto mediante agregaciones y sumas.

## 🛠️ Tecnologías utilizadas

* **Lenguaje:** Scala 2.12+
* **Framework:** Apache Spark 3.x (Spark SQL & RDD)
* **Plataforma:** IntelliJ / SBT para estructurar el proyecto.
* **Gestor de dependencias:** SBT

## 📂 Estructura del Proyecto

* `src/main/scala/practica_fcll/Practica_entrega.scala`: Contiene la lógica principal de los 5 ejercicios debidamente comentada.
* `src/main/scala/examen_estructura/ventas`: Contiene los datos para el ejercicio 5
* `build.sbt`: Configuración de dependencias del proyecto (Spark Core y Spark SQL).

---
**Autor:** [Fabian Camilo López L]  
**Bootcamp:** [KeepCoding BigData MachineLearning 16] - Materia: BD-Processing
**Fecha:** Marzo 2026
