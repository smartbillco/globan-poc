import datetime
import mysql.connector
import pandas as pd

# Configuración de la conexión a MySQL
db_config = {
    'host': '213.199.60.150',  #IP
    'user': 'smartbilluser',   #Usuario de MySQL
    'password': '12345678',    #Contraseña de MySQL
    'database': 'company_data'
}

# Conectar a MySQL
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

def format_iso_date(iso_date):
    """
    Convierte una fecha en formato ISO 8601 (con T y Z) a un formato compatible con MySQL.
    
    Args:
        iso_date (str): Fecha en formato ISO 8601, e.g., '2021-11-07T02:48:42Z'
    
    Returns:
        str: Fecha en formato 'YYYY-MM-DD HH:MM:SS'
    """
    if pd.isna(iso_date) or not iso_date.strip() or iso_date.lower() == 'nan':
        return '1970-01-01 00:00:00'  # Retorna None si la fecha es inválida
    # Reemplazar 'T' con espacio y eliminar 'Z' o espacios en blanco
    formatted_date = iso_date.replace('T', ' ').replace('Z', '').strip()
    return formatted_date

# Función para convertir las fechas al formato correcto de MySQL
def convert_datetime(datetime_str):
    try:
        # Eliminar el sufijo 'Z' y convertir al formato adecuado
        formatted_date = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
        return datetime_str
    except Exception as e:
        print(f"Error al convertir la fecha '{datetime_str}': {e}")
        return None  # Si falla, devuelve None
   
   

# Cargar departamentos
departments_columns = ['id', 'department']
departments_df = pd.read_csv('/Users/elkindelcastilloblanco/Globant/departments.csv', names=departments_columns, header=None)
    
cursor.execute("INSERT INTO departments (id, department) VALUES (%s, %s)", (-999, 'Sin informacion'))

for _, row in departments_df.iterrows():
    cursor.execute("INSERT INTO departments (id, department) VALUES (%s, %s)", (row['id'], row['department']))

 # Cargar trabajos
jobs_columns = ['id', 'job']
jobs_df = pd.read_csv('/Users/elkindelcastilloblanco/Globant/jobs.csv', names=jobs_columns, header=None)
cursor.execute("INSERT INTO jobs (id, job) VALUES (%s, %s)", (-999, "Sin informacion"))

for _, row in jobs_df.iterrows():
    cursor.execute("INSERT INTO jobs (id, job) VALUES (%s, %s)", (row['id'], row['job']))


# Cargar empleados contratados
hired_employees_columns = ['id', 'name', 'datetime_iso', 'department_id', 'job_id']
hired_employees_df = pd.read_csv('/Users/elkindelcastilloblanco/Globant/hired_employees.csv', names=hired_employees_columns, header=None)

for _, row in hired_employees_df.iterrows():
    #datetime_value = row['datetime_iso'] if pd.notnull(row['datetime_iso']) else '1970-01-01 00:00:00'
    department_id_value = row['department_id'] if pd.notnull(row['department_id']) else -999  # Asignar valor predeterminado (ej. -999)
    job_id_value        = row['job_id'] if pd.notnull(row['job_id'])                      else -999  # Asignar valor predeterminado (ej. -999)
    name_value          = row['name'] if pd.notnull(row['name'])                          else '***'  # Asignar valor predeterminado (ej. -999)
    converted_datetime = format_iso_date(row['datetime_iso'])

    cursor.execute("""
        INSERT INTO hired_employees (id, name, datetime, department_id, job_id)
        VALUES (%s, %s, %s, %s, %s)
        """, (row['id'], name_value, converted_datetime,department_id_value, job_id_value))
      

# Confirmar cambios y cerrar la conexión
conn.commit()
cursor.close()
conn.close()

print("Datos cargados exitosamente.")
