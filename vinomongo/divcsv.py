import pandas as pd

def dividir_csv(nombre_archivo, columnas):
    df = pd.read_csv(nombre_archivo, usecols=columnas)
    total_filas = df.shape[0]
    numero_archivos = 1

    nuevo_nombre = f"idpo.csv"
    df.to_csv(nuevo_nombre, index=False)

    print(f"Se ha creado el archivo más pequeño: {nuevo_nombre}.")


def dividir2_csv(nombre_archivo, columnas):
    df = pd.read_csv(nombre_archivo, usecols=columnas)
    total_filas = df.shape[0]
    numero_archivos = 1

    nuevo_nombre = f"vino.csv"
    df.to_csv(nuevo_nombre, index=False)

    print(f"Se ha creado el archivo más pequeño: {nuevo_nombre}.")


# Ejemplo de uso
nombre_archivo = "winemag-data_first150k.csv"  
columnas = ["PII", "country", "points", "price"]
columnas2 = ["PII", "country", "designation"]

dividir_csv(nombre_archivo, columnas)
dividir2_csv(nombre_archivo, columnas2)