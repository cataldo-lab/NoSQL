import pandas as pd
import chardet
from py2neo import Graph
import os
from dotenv import load_dotenv

load_dotenv()
password = os.getenv('NEO4J_PASSWORD')
# Conexión al servidor local de Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j",password))

try:
    # Determinar la codificación del archivo CSV
    with open('/Users/benjamincataldolopez/Desktop/Proyectos/bdNoRelacional/ProyectoSem/Trabajofinal/winemag-data_first150k.csv', 'rb') as file:
        result = chardet.detect(file.read())
        encoding = result['encoding']

    # Leer el archivo CSV utilizando la codificación detectada
    data = pd.read_csv('/Users/benjamincataldolopez/Desktop/Proyectos/bdNoRelacional/ProyectoSem/Trabajofinal/winemag-data_first150k.csv', encoding=encoding, on_bad_lines='skip')

    # Insertar datos en Neo4j
    for index, row in data.iterrows():
        graph.run(
            "CREATE (w:Wine {title: $title, score: $score, description: $description})",
            parameters = {'title': row['title'], 'score': row['points'], 'description': row['description']}
        )
    print('Datos insertados correctamente en Neo4j.')

except Exception as e:
    print(f"Ocurrió un error: {e}")
