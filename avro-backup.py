import pandas as pd
import fastavro
from sqlalchemy import create_engine

# Configuración de la base de datos
db_config = {
    'host': '213.199.60.150',
    'user': 'smartbilluser',
    'password': '12345678',
    'database': 'company_data'
}

# Ruta al esquema AVRO
schema_path = "hired_employees_schema.avsc"

def connect_db():
    """
    Establece una conexión a la base de datos MySQL usando SQLAlchemy.
    """
    db_url = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(db_url)
    return engine

def backup_to_avro(table_name, avro_file_path):
    """
    Realiza un respaldo de la tabla especificada a un archivo AVRO.

    Args:
        table_name (str): Nombre de la tabla MySQL a respaldar.
        avro_file_path (str): Ruta del archivo AVRO donde se guardará el respaldo.
    """
    engine = connect_db()
    try:
        # Leer datos de la tabla
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)

        # Convertir la columna `datetime` a cadena (string) en formato adecuado
        df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Convertir los datos en un formato compatible con AVRO
        records = df.to_dict(orient="records")

        # Cargar el esquema desde el archivo
        with open(schema_path, "r") as schema_file:
            schema = fastavro.schema.load_schema(schema_path)

        # Escribir los datos en el archivo AVRO
        with open(avro_file_path, "wb") as f:
            fastavro.writer(f, schema, records)
        print(f"Respaldo de {table_name} completado en {avro_file_path}")
    except Exception as e:
        print(f"Error al realizar el respaldo: {e}")

if __name__ == "__main__":
    # Configurar los valores para el respaldo
    table_name = "hired_employees"
    avro_file = "hired_employees_backup.avro"

    # Ejecutar el respaldo
    backup_to_avro(table_name, avro_file)
