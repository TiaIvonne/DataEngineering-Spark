import json
from datetime import timedelta
from loguru import logger
from pyspark.sql import SparkSession, functions as F
from .motor_ingesta import  MotorIngesta
from . import agregaciones


class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Iniciador de la clase FlujoDiario
        :param config_file: Fichero json de configuracion
        """
        # Leer como diccionario el fichero json indicado en la ruta config_file, usando json.load(f) del paquete json
        # y almacenarlo en self.config. Además, crear la SparkSession si no existiese usando
        # SparkSession.builder.getOrCreate() que devolverá la sesión existente, o creará una nueva si no existe ninguna

        with open(config_file) as f:
            self.config = json.load(f)
        if self.config["EXECUTION_ENVIRONMENT"] == "databricks":
            from databricks.connect import DatabricksSession
            self.spark = DatabricksSession.builder.getOrCreate()
        else:
            self.spark = SparkSession.builder.getOrCreate()


    def procesa_diario(self, data_file: str):
        """
        Funcion que procesa y agrega columnas adicionales a un dataframe generado a traves del motor de ingesta

        :param data_file: Archivo de entrada que debe ser procesado con el metodo ingesta fichero de la clase MotorIngesta
        :return: Un dataframe completo que incluye los proximos vuelos
        """
        try:
            # Procesamiento diario: crea un nuevo objeto motor de ingesta con self.config, invoca a ingesta_fichero,
            # después a las funciones que añaden columnas adicionales, y finalmente guarda el DF en la tabla indicada en
            # self.config["output_table"], que debe crearse como tabla manejada (gestionada), sin usar ningún path,
            # siempre particionando por FlightDate. Tendrás que usar .write.option("path", ...).saveAsTable(...) para
            # indicar que queremos crear una tabla externa en el momento de guardar.
            # Conviene cachear el DF flights_df así como utilizar el número de particiones indicado en
            # config["output_partitions"]

            # Crea objeto
            motor_ingesta = MotorIngesta(config=self.config)
            # Llama al metodo ingesta fichero
            flights_df = motor_ingesta.ingesta_fichero(data_file).cache()

            # Paso 1. Invocamos al método para añadir la hora de salida UTC
            flights_with_utc = agregaciones.aniade_hora_utc(self.spark, flights_df)
            df_with_next_flight = agregaciones.aniade_intervalos_por_aeropuerto(flights_with_utc)

            df_with_next_flight.coalesce(self.config["output_partitions"])\
                .write\
                .mode("overwrite") \
                .option("partitionOverwriteMode", "dynamic")\
                .partitionBy("FlightDate")\
                .saveAsTable(self.config["output_table"])


            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            # Paso 2. Para resolver el ejercicio 4 que arregla el intervalo faltante entre días,
            # hay que leer de la tabla self.config["output_table"] la partición del día previo si existiera. Podemos
            # obviar este código hasta llegar al ejercicio 4 del notebook
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(self.config["output_table"]).where(F.col("FlightDate") == dia_previo)
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
                flights_previo = None

            if flights_previo:
                # añadir columnas a F.lit(None) haciendo cast al tipo adecuado de cada una, y unirlo con flights_previo.
                # OJO: hacer select(flights_previo.columns) para tenerlas en el mismo orden antes de
                # la unión, ya que la columna de partición se había ido al final al escribir

                df_unido = flights_previo.withColumn("FlightTime_next", F.lit(None).cast("timestamp"))\
                            .withColumn("Airline_next",F.lit(None).cast("string"))\
                            .withColumn("diff_next", F.lit(None).cast("long"))\
                            .select(flights_previo.columns)

                flights_with_utc_extended = (flights_with_utc
                                             .withColumn("FlightTime_next", F.lit(None).cast("timestamp"))
                                             .withColumn("Airline_next", F.lit(None).cast("string"))
                                             .withColumn("diff_next", F.lit(None).cast("long"))
                                             )


                df_unido = df_unido.union(flights_with_utc_extended)

                # Spark no permite escribir en la misma tabla de la que estamos leyendo. Por eso salvamos
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc           # lo dejamos como está

            # Paso 3. Invocamos al método para añadir información del vuelo siguiente
            df_with_next_flight = agregaciones.aniade_intervalos_por_aeropuerto(df_unido)

            # Paso 4. Escribimos el DF en la tabla externa config["output_table"] con ubicación config["output_path"], con
            # el número de particiones indicado en config["output_partitions"]
            # df_with_next_flight.....(...)..write.mode("overwrite").option("partitionOverwriteMode", "dynamic")....
            (df_with_next_flight
                .coalesce(self.config["output_partitions"])
                .write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
                .partitionBy("FlightDate")
                .saveAsTable(self.config["output_table"]))


            # Borrar la tabla provisional si la hubiéramos creado
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e


if __name__ == '__main__':
    #spark = SparkSession.builder.getOrCreate()   # sólo si lo ejecutas localmente
    flujo = FlujoDiario(config_file="config/config.json")
    flujo.procesa_diario("abfss://datos@masterim001sta.dfs.core.windows.net/2023-01-01.json")

    # Recuerda que puedes crear el wheel ejecutando en la línea de comandos: python setup.py bdist_wheel

