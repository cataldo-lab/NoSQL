import pandas as pd
from pymongo import MongoClient
import chardet

# Conexión al servidor local de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['wines_planet']
collection = db['vino']

#Insertar datawine.json
try:
    # Determinar la codificación del archivo CSV
    with open('./vino.json', 'rb') as file:
        result = chardet.detect(file.read())
        encoding = result['encoding']

    with open('./vino.json', encoding=encoding) as file:
        data = pd.read_json(file)
        data.reset_index(inplace=True)
        data_dict = data.to_dict("records")
        collection.insert_many(data_dict)
        print("Datos Vino, insertados correctamente")
    
except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    client.close()

#________________________________________________________________
#Insertar idpoipri.json
client = MongoClient('mongodb://localhost:27017/')
db = client['wines_planet']
collection = db['idpo']
try:
    # Determinar la codificación del archivo CSV
    with open('./idpo.json', 'rb') as file:
        result = chardet.detect(file.read())
        encoding = result['encoding']

    with open('./idpo.json', encoding=encoding) as file:
        data = pd.read_json(file)
        data.reset_index(inplace=True)
        data_dict = data.to_dict("records")
        collection.insert_many(data_dict)
        print("Datos points price, insertados correctamente")
    
except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    client.close()
