import pandas as pd
from pymongo import MongoClient

# Conexión al servidor local de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['nombre_de_tu_base_de_datos']
collection = db['vinos']

# Leer el archivo CSV
data = pd.read_csv('ruta_a_tu_archivo.csv')

# Convertir DataFrame a diccionario para la inserción en MongoDB
data_dict = data.to_dict("records")

# Insertar datos en la colección
collection.insert_many(data_dict)

print('Datos insertados correctamente.')
client.close()
