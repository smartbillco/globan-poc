import mysql.connector
import pandas as pd

# Configuración de la conexión a MySQL
db_config = {
    'host': '213.199.60.150',  # IP
    'user': 'smartbilluser',    # Usuario de MySQL
    'password': '12345678',     # Contraseña de MySQL
    'database': 'company_data',
    'allow_local_infile': True  # Habilitar local_infile para la conexión
}

# Conectar a MySQL
conn = mysql.connector.connect(**db_config)

# Crear un cursor para ejecutar las consultas de inserción
cursor = conn.cursor()

# Insertar un valor por defecto en la tabla departments
cursor.execute("INSERT INTO departments (id, department) VALUES (%s, %s)", (-999, 'Sin informacion'))

# Cargar los datos de la tabla departments
departments_columns = ['id', 'department']
departments_df = pd.read_csv('/Users/elkindelcastilloblanco/Globant/departments.csv', names=departments_columns, header=None)

# Guardar el archivo CSV para cargarlo usando LOAD DATA INFILE
departments_df.to_csv('/tmp/departments_temp.csv', header=False, index=False)

# Cargar los datos en la tabla departments usando LOAD DATA INFILE
cursor.execute("""
    LOAD DATA LOCAL INFILE '/tmp/departments_temp.csv'
    INTO TABLE departments
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    (id, department)
""")

# Insertar un valor por defecto en la tabla jobs
cursor.execute("INSERT INTO jobs (id, job) VALUES (%s, %s)", (-999, "Sin informacion"))

# Cargar los datos de la tabla jobs
jobs_columns = ['id', 'job']
jobs_df = pd.read_csv('/Users/elkindelcastilloblanco/Globant/jobs.csv', names=jobs_columns, header=None)

# Guardar el archivo CSV para cargarlo usando LOAD DATA INFILE
jobs_df.to_csv('/tmp/jobs_temp.csv', header=False, index=False)

# Cargar los datos en la tabla jobs usando LOAD DATA INFILE
cursor.execute("""
    LOAD DATA LOCAL INFILE '/tmp/jobs_temp.csv'
    INTO TABLE jobs
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    (id, job)
""")

# Cargar empleados contratados
hired_employees_columns = ['id', 'name', 'datetime_iso', 'department_id', 'job_id']
hired_employees_df = pd.read_csv('/Users/elkindelcastilloblanco/Globant/hired_employees.csv', names=hired_employees_columns, header=None)

# Reemplazar valores nulos y dar valores predeterminados para campos específicos
hired_employees_df['datetime_iso'] = hired_employees_df['datetime_iso'].fillna('1970-01-01 00:00:00')
hired_employees_df['department_id'] = hired_employees_df['department_id'].fillna(-999)
hired_employees_df['job_id'] = hired_employees_df['job_id'].fillna(-999)
hired_employees_df['name'] = hired_employees_df['name'].fillna('***')

# Guardar el archivo CSV para cargarlo usando LOAD DATA INFILE
hired_employees_df.to_csv('/tmp/hired_employees_temp.csv', header=False, index=False)

# Cargar los datos en la tabla hired_employees usando LOAD DATA INFILE
cursor.execute("""
    LOAD DATA LOCAL INFILE '/tmp/hired_employees_temp.csv'
    INTO TABLE hired_employees
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    (id, name, datetime, department_id, job_id)
""")

# Confirmar cambios y cerrar la conexión
conn.commit()

# Cerrar cursor y conexión
cursor.close()
conn.close()

print("Datos cargados exitosamente.")
