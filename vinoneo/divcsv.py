import pandas as pd

def dividir_csv(nombre_archivo, columnas):
    df = pd.read_csv(nombre_archivo, usecols=columnas)
    
    # Eliminar filas con valores nulos en las columnas seleccionadas
    df = df.dropna(subset=columnas)
    
    nuevo_nombre = "idpo.csv"
    df.to_csv(nuevo_nombre, index=False)

    print(f"Se ha creado el archivo más pequeño: {nuevo_nombre}.")


def dividir2_csv(nombre_archivo, columnas):
    df = pd.read_csv(nombre_archivo, usecols=columnas)
    
    # Eliminar filas con valores nulos en las columnas seleccionadas
    df = df.dropna(subset=columnas)
    
    nuevo_nombre = "vino.csv"
    df.to_csv(nuevo_nombre, index=False)

    print(f"Se ha creado el archivo más pequeño: {nuevo_nombre}.")


def paisesunicos():
    df = pd.read_csv('winemag-data_first150k.csv')

    print("Datos originales:\n", df.head())

    # Eliminar filas con valores nulos en la columna 'country'
    df = df.dropna(subset=['country'])

    print("\nDespués de eliminar registros sin país:\n", df.head())
    
    unique_countries = df['country'].drop_duplicates()

    print("\nPaíses únicos:\n", unique_countries.head())

    unique_countries.to_csv('ruta_de_paises_unicos.csv', index=False)

    print("\nPaíses únicos guardados en 'ruta_de_paises_unicos.csv'")

nombre_archivo = "winemag-data_first150k.csv"
columnas = ["PII", "country", "points", "price"]
columnas2 = ["PII", "country", "designation"]

dividir_csv(nombre_archivo, columnas)
dividir2_csv(nombre_archivo, columnas2)
paisesunicos()
