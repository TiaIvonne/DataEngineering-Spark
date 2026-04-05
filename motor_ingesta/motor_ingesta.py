
import json
from importlib.metadata import metadata
from logging.config import dictConfig

from pyspark.sql import DataFrame as DF, functions as F, SparkSession


class MotorIngesta:
    """
    Esta clase procesa un archivo de entrada de tipo json, posteriormente lo aplana y devuelve un dataframe de spark
    """
    def __init__(self, config: dict):
        """
        Este es el iniciador de la clase MotorIngesta
        :param config_file: Un archivo de tipo json que contiene las definiciones de estructura del archivo a procesar
        """
        if self.config["EXECUTION_ENVIRONMENT"] == 'local':
            self.spark = SparkSession.builder.getOrCreate()
        else:
            from databricks.connect import DatabricksSession
            self.spark = DatabricksSession.builder.getOrCreate()



    def ingesta_fichero(self, json_path: str) -> DF:
        """
        Procesa un archivo de tipo json para posteriormente transformarlo en un dataset de tipo spark.

        :param json_path: Un fichero de tipo json que contiene el detalle de los vuelos que sera posteriormente procesado
        :return: Un dataset de spark que ha sido previamente procesado y aplanado
        """
        # Leemos el JSON como DF, tratando de inferir el esquema, y luego lo aplanamos.
        # Por último nos quedamos con las columnas indicadas en el fichero de configuración,
        # en la propiedad self.config["data_columns"], que es una lista de diccionarios. Debemos recorrer
        # esa lista, seleccionando la columna y convirtiendo cada columna al tipo indicado en el fichero.

        # PISTA: crear en lista_obj_column una lista de objetos Column como lista por comprensión a partir
        # de self.config["data_columns"], y luego usar dicha lista como argumento de select(...). El DF resultante
        # debe ser devuelto como resultado de la función.

        # Para incluir también el campo "comment" como metadatos de la columna, podemos hacer:
        # F.col(...).cast(...).alias(..., metadata={"comment": ...})

        flights_day_df = self.spark.read.format("json").option("inferSchema", "true").load(json_path)

        aplanado_df = MotorIngesta.aplana_df(flights_day_df)
        lista_obj_column = [ F.col(diccionario["name"])
                             .cast(diccionario["type"])
                             .alias(diccionario["name"], metadata={"comment": diccionario["comment"]})
                             for diccionario in self.config["data_columns"] ]
        resultado_df = aplanado_df.select(lista_obj_column)
        return resultado_df


    @staticmethod
    def aplana_df(df: DF) -> DF:
        """
        Aplana un DataFrame de Spark que tenga columnas de tipo array y de tipo estructura.

        :param df: DataFrame de Spark que contiene columnas de tipo array o columnas de tipo estructura, incluyendo
                   cualquier nivel de anidamiento y también arrays de estructuras. Asumimos que los nombres de los
                   campos anidados son todos distintos entre sí, y no van a coincidir cuando sean aplanados.
        :return: DataFrame de Spark donde todas las columnas de tipo array han sido explotadas y las estructuras
                 han sido aplanadas recursivamente.
        """
        to_select = []
        schema = df.schema.jsonValue()
        fields = schema["fields"]
        recurse = False

        for f in fields:
            if f["type"].__class__.__name__ != "dict":
                to_select.append(f["name"])
            else:
                if f["type"]["type"] == "array":
                    to_select.append(F.explode(f["name"]).alias(f["name"]))
                    recurse = True
                elif f["type"]["type"] == "struct":
                    to_select.append(f"{f['name']}.*")
                    recurse = True

        new_df = df.select(*to_select)
        return MotorIngesta.aplana_df(new_df) if recurse else new_df
