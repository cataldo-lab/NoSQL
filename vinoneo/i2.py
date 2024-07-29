import pandas as pd
from neo4j import GraphDatabase

# Datos de conexión
uri = "bolt://localhost:7687"
user = "neo4j"
password = "toroE2024"

driver = GraphDatabase.driver(uri, auth=(user, password))

def insert_data(tx, query, parameters):
    tx.run(query, parameters)

def create_points_batch(tx, batch):
    query = (
        "UNWIND $batch AS row "
        "CREATE (p:points {PII: row.PII, country: row.country, points: row.points, price: row.price})"
    )
    parameters = {"batch": batch}
    tx.run(query, parameters)

def load_data_from_csv(file_path):
    data = pd.read_csv(file_path)
    return data

def insert_csv_data_to_neo4j(file_path, batch_size=1000):
    data = load_data_from_csv(file_path)
    batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
    
    with driver.session() as session:
        for batch in batches:
            batch_dict = batch.to_dict(orient="records")
            session.execute_write(create_points_batch, batch_dict)

insert_csv_data_to_neo4j("./idpo.csv", batch_size=1000)

driver.close()
