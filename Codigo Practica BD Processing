//importamos las librerias necesarias para que se pueda ejecutar la aplicacion
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object PracticaBDProcessing {
  def main(args:Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //Muestra solamente logs de error por consola

    val spark = SparkSession  //levantamos sparksession
      .builder()
      .appName("practicaBDProcessing")
      .master("local[2]")
      .getOrCreate()

    val df = spark.readStream  //hacemos una lectura en formato streaming con kafka
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","topicPractica")
      .option("startingOffsets","earliest")
      .load()

    //casteamos los datos leidos en formato kafka para convertirlos en strings
    val res = df.selectExpr("CAST(value AS STRING)")

    //definimos campos en nuestra estructura con los campos del fichero json
    val schema = new StructType()
      .add("id",IntegerType)
      .add("first_name",StringType)
      .add("last_name",StringType)
      .add("email",StringType)
      .add("gender",StringType)
      .add("ip_address",StringType)

    //guardamos en una nueva variable el resultado anterior
    val persona = res.select(from_json(col("value"),schema)
      //asignamos un alias "data"
      .as("data"))
      //y seleccionando todos los datos de la tabla
      .select("data.*")
        //procedemos en esta parte a realizar el filtrado de los primeros registros del campo "id"
        .filter("data.id != '1' AND data.id != '2'")

    //mostramos el resultado por pantalla
    print("mostrar datos por consola: ")
    persona.writeStream
      .format("console")
      .outputMode("append")
      .start() //comienzo de la ejecucion
      .awaitTermination() //espera hasta que termine la ejecucion, es un proceso constante si no se detiene
  }
}
