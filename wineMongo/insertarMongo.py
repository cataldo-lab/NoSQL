import pandas as pd
from pymongo import MongoClient
import chardet

# Conexión al servidor local de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['wines_world']
collection = db['vinos']

try:
    # Determinar la codificación del archivo CSV
    with open('/Users/benjamincataldolopez/Desktop/Proyectos/bdNoRelacional/ProyectoSem/Trabajofinal/winemag-data_first150k.csv', 'rb') as file:
        result = chardet.detect(file.read())
        encoding = result['encoding']

    # Leer el archivo CSV utilizando la codificación detectada
    data = pd.read_csv('/Users/benjamincataldolopez/Desktop/Proyectos/bdNoRelacional/ProyectoSem/Trabajofinal/winemag-data_first150k.csv', encoding=encoding, on_bad_lines='skip')
    data_dict = data.to_dict("records")
    collection.insert_many(data_dict)
    print('Datos insertados correctamente.')
    
except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    client.close()
