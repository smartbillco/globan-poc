from flask import Flask, request, jsonify
import mysql.connector

app = Flask(__name__)

# Configuración de la base de datos
db_config = {
    'host': '213.199.60.150',
    'user': 'smartbilluser',
    'password': '12345678',
    'database': 'company_data',
    'allow_local_infile': True
}

# Función para conectar a la base de datos
def connect_db():
    return mysql.connector.connect(**db_config)

# Validar un registro antes de la inserción
def validate_record(record):
    required_fields = ['id', 'name', 'datetime', 'department_id', 'job_id']
    for field in required_fields:
        if field not in record or not record[field]:
            return False
    return True

# Endpoint para insertar un único registro
@app.route('/api/hired_employees', methods=['POST'])
def add_hired_employee():
    data = request.get_json()
    
    # Validar el registro
    if not validate_record(data):
        return jsonify({"error": "Invalid data. All fields are required."}), 400

    # Conectar a la base de datos
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Insertar el registro
        query = """
            INSERT INTO hired_employees (id, name, datetime, department_id, job_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (data['id'], data['name'], data['datetime'], data['department_id'], data['job_id']))
        conn.commit()
        return jsonify({"message": "Hired employee added successfully."}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        cursor.close()
        conn.close()

# Endpoint para insertar registros por lotes
@app.route('/api/hired_employees/batch', methods=['POST'])
def add_hired_employees_batch():
    data = request.get_json()

    # Validar que la solicitud contiene una lista de registros
    if not isinstance(data, list) or len(data) == 0:
        return jsonify({"error": "Invalid data. Expected a list of records."}), 400

    # Validar tamaño del lote
    if len(data) > 1000:
        return jsonify({"error": "Batch size exceeds the limit of 1000 records."}), 400

    # Conectar a la base de datos
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Insertar cada registro en el lote
        query = """
            INSERT INTO hired_employees (id, name, datetime, department_id, job_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        for record in data:
            if not validate_record(record):
                return jsonify({"error": "Invalid data in batch. All fields are required."}), 400
            cursor.execute(query, (record['id'], record['name'], record['datetime'], record['department_id'], record['job_id']))
        
        conn.commit()
        return jsonify({"message": f"{len(data)} records added successfully."}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        cursor.close()
        conn.close()

# Iniciar la aplicación Flask
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
