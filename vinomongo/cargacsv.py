import csv
from pymongo import MongoClient

# Conexión a la base de datos MongoDB
client = MongoClient('mongodb://localhost:27017/')
database = client['wines_planet']
idpo_collection = database['idpo']
vino_collection = database['vino']

# Carga de datos de idpo.csv
with open('idpo.csv', 'r') as file:
    reader = csv.DictReader(file)
    idpo_data = list(reader)

# Insertar datos en la colección idpo
idpo_collection.insert_many(idpo_data)
print("Datos de idpo.csv insertados correctamente.")

# Carga de datos de vino.csv
with open('vino.csv', 'r') as file:
    reader = csv.DictReader(file)
    vino_data = list(reader)

# Insertar datos en la colección vino
vino_collection.insert_many(vino_data)
print("Datos de vino.csv insertados correctamente.")