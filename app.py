import os
import csv
import io
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from models import db, Department, Job, Employee
from sqlalchemy import text

 # Load environment variables from .env file
load_dotenv()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)



@app.cli.command("create-db")
def create_db():
    """
    Create the database and all tables.

    This command is used to initialize the database.
    It will create the database and all tables if they do not exist.
    If the database already exists, it will not be recreated.
    If the tables already exist, they will not be recreated.

    Example:
        $ flask create-db
    """
    
    db.create_all()
    print("Database and tables created successfully.")

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    """
    Upload a CSV file to insert records into a table.

    Parameters:
        file: CSV file to be uploaded.
        table_name: Name of the table to insert records into.

    Returns:
        A JSON object with the following structure:
            {
                'status': str,  # Process status message.
                'loaded_records': int,  # Number of successfully loaded records.
                'skipped_records': int,  # Number of records skipped due to error.
                'details_of_skipped': list  # List of objects with details of skipped records.
            }

    Returns status code 400 if there is a parameter or file error.
    Returns status code 500 if there is an unexpected error.
    """
    if 'file' not in request.files: return jsonify({"error": "File not found."}), 400
    file = request.files['file']
    table_name = request.form.get('table_name')
    if file.filename == '' or table_name is None: return jsonify({"error": "Missing parameters: 'file' or 'table_name'"}), 400
    if not file.filename.endswith('.csv'): return jsonify({"error": "Invalid file format. Must be .csv"}), 400

    stream = io.StringIO(file.stream.read().decode("UTF-8"), newline=None)
    model_map = {'departments': Department, 'jobs': Job}
    column_map = {'departments': ['id', 'department'], 'jobs': ['id', 'job']}
    if table_name not in model_map: return jsonify({"error": f"Table '{table_name}' not supported."}), 400
    
    Model = model_map[table_name]
    columns = column_map[table_name]

    csv_reader = csv.reader(stream)
    rows = list(csv_reader)
    num_rows = len(rows)

    if not (1 <= num_rows <= 1000):
        return jsonify({
            "error": f"The number of records must be between 1 and 1000. Found: {num_rows}."
        }), 400


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

        response_data = {"status": "Process finished.", "loaded_records": len(objects_to_add), "skipped_records": len(skipped_rows), "details_of_skipped": skipped_rows}
        return jsonify(response_data), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Error proccesing the file: {str(e)}"}), 500

@app.route('/employees/upload-csv', methods=['POST'])
def upload_employees_csv():
    """
    Upload a CSV file with employees to the 'employees' table.

    Parameters:
        file: CSV file to be uploaded.

    Returns:
        A JSON object with the following structure:
            {
                'status': str,  # Process status message.
                'loaded_records': int,  # Number of successfully loaded records.
                'skipped_records': int,  # Number of records skipped due to error.
                'details_of_skipped': list  # List of objects with details of skipped records.
            }

    Returns status code 400 if there is a parameter or file error.
    Returns status code 500 if there is an unexpected error.
    """
    if 'file' not in request.files:
        return jsonify({"error": "File not found."}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "Invalid file format. Must be .csv"}), 400

    stream = io.StringIO(file.stream.read().decode("UTF-8"), newline=None)
    columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
    
    csv_reader = csv.reader(stream)
    rows = list(csv_reader)
    num_rows = len(rows)

    if not (1 <= num_rows <= 1000):
        return jsonify({
            "error": f"The number of records must be between 1 and 1000. Found: {num_rows}."
        }), 400
    
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

        response_data = {"status": "Process finished.", "loaded_records": len(objects_to_add), "skipped_records": len(skipped_rows), "details_of_skipped": skipped_rows}
        return jsonify(response_data), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Error processing the file: {str(e)}"}), 500

@app.route('/employees/batch', methods=['POST'])
def add_employees_batch():
    """
    Add multiple employee records to the database in a single batch.

    This endpoint accepts a JSON payload containing a list of employee records
    and adds them to the 'employees' table. The number of records must be between
    1 and 1000. Each record should follow the Employee model schema.

    Returns:
        A JSON response with a success message and the number of employees inserted
        with status code 201 on success. In case of errors, returns an appropriate
        error message with status code 400 for client errors and 500 for server errors.
    """

    records = request.get_json()
    if not isinstance(records, list): return jsonify({"error": "The payload must be a list"}), 400
    if not (1 <= len(records) <= 1000): return jsonify({"error": "The number of records must be between 1 and 1000."}), 400
    employees_to_add = [Employee(**r) for r in records]
    try:
        db.session.bulk_save_objects(employees_to_add)
        db.session.commit()
        return jsonify({"message": f"{len(employees_to_add)} inserted employees"}), 201
    except Exception as e:
        db.session.rollback()


@app.route('/reports/hires-quarterly', methods=['GET'])
def hires_per_job_department_quarter():
    """
    Retrieve the number of employees hired per job and department for each quarter of a given year (default: 2021).

    This endpoint accepts an optional 'year' query parameter to filter hires by year. It returns a JSON response containing
    the number of hires for each job and department for each quarter (Q1, Q2, Q3, Q4) of the specified year.

    Returns:
        A JSON object with:
        - "status": Message indicating the result of the query.
        - "result": List of dictionaries, each containing:
            - 'department': Department name.
            - 'job': Job name.
            - 'Q1', 'Q2', 'Q3', 'Q4': Number of hires in each quarter.
        If the year parameter is invalid, returns an error message with status code 400.
        If no records are found for the specified year, returns an appropriate message with status code 200.
    """

    year = request.args.get('year', default='2021')
    try:
        year_int = int(year)
    except ValueError:
        return jsonify({
            "status": "invalid parameter 'year'.",
            "result": []
        }), 400

    query = f'''
        SELECT d.department, j.job,
            SUM(CASE WHEN substr(e.datetime,1,4) = '{year_int}' AND substr(e.datetime,6,2) IN ('01','02','03') THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN substr(e.datetime,1,4) = '{year_int}' AND substr(e.datetime,6,2) IN ('04','05','06') THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN substr(e.datetime,1,4) = '{year_int}' AND substr(e.datetime,6,2) IN ('07','08','09') THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN substr(e.datetime,1,4) = '{year_int}' AND substr(e.datetime,6,2) IN ('10','11','12') THEN 1 ELSE 0 END) AS Q4
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN jobs j ON e.job_id = j.id
        WHERE substr(e.datetime,1,4) = '{year_int}'
        GROUP BY d.department, j.job
        ORDER BY d.department, j.job
    '''
    results = db.session.execute(text(query))
    data = [
        {
            'department': row[0],
            'job': row[1],
            'Q1': int(row[2]),
            'Q2': int(row[3]),
            'Q3': int(row[4]),
            'Q4': int(row[5])
        }
        for row in results
    ]

    if not data:
        return jsonify({
            "status": f"No records found for year {year_int}.",
            "result": []
        }), 200
    else:
        return jsonify({
            "status": f"Number of employees hired per job for year {year_int}.",
            "result": data
        }), 200



@app.route('/reports/above-mean-hires', methods=['GET'])
def departments_above_mean_hires():
    """
    Retrieve departments with above-average employee hires for a given year (default: 2021).

    This endpoint accepts an optional 'year' query parameter to filter results by year. It calculates the number of employees hired
    per department and returns those departments where the number of hires is above the average for that year.

    Returns:
        A JSON response containing:
        - "status": Message indicating the result of the query.
        - "result": List of dictionaries, each containing:
            - 'id': Department ID.
            - 'department': Department name.
            - 'hired': Number of employees hired in that department.
        If the year parameter is invalid, returns an error message with status code 400.
        If no departments are found above the average, returns an appropriate message with status code 200.
    """

    year = request.args.get('year', default='2021')
    try:
        year_int = int(year)
    except ValueError:
        return jsonify({
            "status": "Parameter 'year' is invalid.",
            "result": []
        }), 400

    query = f'''
    WITH hires_by_department AS (
        SELECT
            department_id,
            COUNT(id) as hired_count
        FROM
            employees
        WHERE
            datetime IS NOT NULL AND datetime != ''
            AND EXTRACT(YEAR FROM datetime::timestamp) = {year_int}
        GROUP BY
            department_id
    )
    SELECT
        d.id,
        d.department,
        h.hired_count AS hired
    FROM
        departments d
    JOIN
        hires_by_department h ON d.id = h.department_id
    WHERE
        h.hired_count > (
            SELECT AVG(hired_count) FROM hires_by_department
        )
    ORDER BY
        hired DESC
    '''
    results = db.session.execute(text(query))

    data = [
        {
            'id': row[0],
            'department': row[1],
            'hired': int(row[2])
        }
        for row in results
    ]

    if not data:
        return jsonify({
            "status": f"No departments found above the mean for year {year_int}.",
            "result": []
        }), 200
    else:
        return jsonify({
            "status": f"Departments found above the mean for year {year_int}.",
            "result": data
        }), 200