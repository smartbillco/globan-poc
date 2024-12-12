import fastavro
from sqlalchemy import create_engine
from datetime import datetime

# Configuración de la base de datos
db_config = {
    'host': '213.199.60.150',
    'user': 'smartbilluser',
    'password': '12345678',
    'database': 'company_data'
}

def connect_db():
    """
    Establece una conexión a la base de datos MySQL usando SQLAlchemy.
    """
    db_url = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(db_url)
    return engine

def format_datetime(datetime_str):
    """
    Formatea el campo datetime al formato adecuado ('YYYY-MM-DD HH:MM:SS').
    """
    try:
        # Convertir a datetime y formatear a cadena en el formato adecuado
        return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None  # Si la fecha no es válida, retornamos None

def restore_from_avro(avro_file_path, table_name):
    """
    Restaura datos desde un archivo AVRO a la tabla especificada en MySQL.

    Args:
        avro_file_path (str): Ruta del archivo AVRO que contiene los datos.
        table_name (str): Nombre de la tabla MySQL donde se restaurarán los datos.
    """
    engine = connect_db()
    conn = engine.connect()

    try:
        # Leer el archivo AVRO
        with open(avro_file_path, "rb") as f:
            records = list(fastavro.reader(f))

        # Convertir los registros en una lista de tuplas
        records_to_insert = []
        for record in records:
            formatted_datetime = format_datetime(record["datetime"])
            records_to_insert.append((
                record["id"],
                record["name"],
                formatted_datetime,  # Usamos el datetime formateado
                record["department_id"],
                record["job_id"]
            ))

        # Insertar los registros en la tabla usando ejecutando múltiples registros a la vez
        query = f"""
            INSERT INTO {table_name} (id, name, datetime, department_id, job_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        # Ejecutar la inserción en batch (múltiples registros)
        conn.execute(query, *records_to_insert)

        print(f"Restauración de {len(records)} registros a la tabla {table_name} completada.")
    except Exception as e:
        print(f"Error al restaurar desde el archivo AVRO: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    avro_file = "hired_employees_backup.avro"
    table_name = "hired_employees"

    # Ejecutar restauración
    restore_from_avro(avro_file, table_name)
