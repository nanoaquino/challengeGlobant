import os
import csv
import io
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from models import db, Department, Job, Employee

load_dotenv()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

@app.cli.command("create-db")
def create_db():
    db.create_all()
    print("Base de datos y tablas creadas exitosamente.")

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files: return jsonify({"error": "No se encontró el archivo"}), 400
    file = request.files['file']
    table_name = request.form.get('table_name')
    if file.filename == '' or table_name is None: return jsonify({"error": "Faltan parámetros: 'file' o 'table_name'"}), 400
    if not file.filename.endswith('.csv'): return jsonify({"error": "Formato de archivo inválido. Debe ser .csv"}), 400
    
    stream = io.StringIO(file.stream.read().decode("UTF-8"), newline=None)
    model_map = {'departments': Department, 'jobs': Job}
    column_map = {'departments': ['id', 'department'], 'jobs': ['id', 'job']}
    if table_name not in model_map: return jsonify({"error": f"Tabla '{table_name}' no soportada."}), 400
    
    Model = model_map[table_name]
    columns = column_map[table_name]

    # --- VALIDACIÓN DE NÚMERO DE FILAS ---
    csv_reader = csv.reader(stream)
    rows = list(csv_reader)
    num_rows = len(rows)

    if not (1 <= num_rows <= 1000):
        return jsonify({
            "error": f"El archivo debe contener entre 1 y 1000 filas. Se encontraron {num_rows}."
        }), 400
    # --- FIN DE LA VALIDACIÓN ---

    try:
        existing_ids = {item.id for item in Model.query.all()}
        objects_to_add = []
        skipped_rows = []

        for line_number, row in enumerate(rows, 1):
            if not row: continue
            if len(row) != len(columns):
                skipped_rows.append({"line": line_number, "data": row, "error": f"Número de campos incorrecto."})
                continue
            data_dict = dict(zip(columns, row))
            try:
                record_id = int(data_dict['id'])
                if record_id in existing_ids:
                    skipped_rows.append({"line": line_number, "data": row, "error": f"El ID '{record_id}' ya existe."})
                    continue
            except (ValueError, KeyError):
                skipped_rows.append({"line": line_number, "data": row, "error": "El campo 'id' es inválido."})
                continue
            objects_to_add.append(Model(**data_dict))
            existing_ids.add(record_id)
        
        if objects_to_add:
            db.session.bulk_save_objects(objects_to_add)
            db.session.commit()

        response_data = {"status": "Proceso finalizado.", "loaded_records": len(objects_to_add), "skipped_records": len(skipped_rows), "details_of_skipped": skipped_rows}
        return jsonify(response_data), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Error procesando el archivo: {str(e)}"}), 500

@app.route('/employees/upload-csv', methods=['POST'])
def upload_employees_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No se encontró el archivo"}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "Formato de archivo inválido. Debe ser .csv"}), 400

    stream = io.StringIO(file.stream.read().decode("UTF-8"), newline=None)
    columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
    
    # --- VALIDACIÓN DE NÚMERO DE FILAS ---
    csv_reader = csv.reader(stream)
    rows = list(csv_reader)
    num_rows = len(rows)

    if not (1 <= num_rows <= 1000):
        return jsonify({
            "error": f"El archivo debe contener entre 1 y 1000 filas. Se encontraron {num_rows}."
        }), 400
    # --- FIN DE LA VALIDACIÓN ---
    
    try:
        existing_employee_ids = {e.id for e in Employee.query.all()}
        existing_dept_ids = {d.id for d in Department.query.all()}
        existing_job_ids = {j.id for j in Job.query.all()}
        objects_to_add = []
        skipped_rows = []
        
        for line_number, row in enumerate(rows, 1):
            if not row: continue
            if len(row) != len(columns):
                skipped_rows.append({"line": line_number, "data": row, "error": "Número de campos incorrecto."})
                continue
            data_dict = dict(zip(columns, row))
            try:
                emp_id = int(data_dict['id'])
                dept_id = int(data_dict['department_id'])
                job_id = int(data_dict['job_id'])
                if emp_id in existing_employee_ids:
                    skipped_rows.append({"line": line_number, "data": row, "error": f"El ID de empleado '{emp_id}' ya existe."})
                    continue
                if dept_id not in existing_dept_ids:
                    skipped_rows.append({"line": line_number, "data": row, "error": f"El department_id '{dept_id}' no existe."})
                    continue
                if job_id not in existing_job_ids:
                    skipped_rows.append({"line": line_number, "data": row, "error": f"El job_id '{job_id}' no existe."})
                    continue
            except (ValueError, KeyError):
                skipped_rows.append({"line": line_number, "data": row, "error": "Uno o más campos de ID son inválidos o están vacíos."})
                continue
            objects_to_add.append(Employee(**data_dict))
            existing_employee_ids.add(emp_id)

        if objects_to_add:
            db.session.bulk_save_objects(objects_to_add)
            db.session.commit()

        response_data = {"status": "Proceso finalizado.", "loaded_records": len(objects_to_add), "skipped_records": len(skipped_rows), "details_of_skipped": skipped_rows}
        return jsonify(response_data), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Error procesando el archivo de empleados: {str(e)}"}), 500

@app.route('/employees/batch', methods=['POST'])
def add_employees_batch():
    records = request.get_json()
    if not isinstance(records, list): return jsonify({"error": "El payload debe ser una lista"}), 400
    if not (1 <= len(records) <= 1000): return jsonify({"error": "El número de registros debe estar entre 1 y 1000"}), 400
    employees_to_add = [Employee(**r) for r in records]
    try:
        db.session.bulk_save_objects(employees_to_add)
        db.session.commit()
        return jsonify({"message": f"{len(employees_to_add)} empleados insertados"}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500