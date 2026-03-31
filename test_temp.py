# test_temp.py en la raíz del proyecto
import json
from motor_ingesta.motor_ingesta import MotorIngesta

with open("config/config.json") as f:
    config = json.load(f)

motor = MotorIngesta(config)
df = motor.ingesta_fichero("abfss://datos@masterim001sta.dfs.core.windows.net/2023-01-01.json")
df.printSchema()
df.show(5)