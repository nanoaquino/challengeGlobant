
# challengeGlobant

## Project Overview
This project is a Data Engineering API for Globant’s coding challenge. It provides endpoints to upload CSV data, batch insert employees, and generate reports on employee hires by department and job.


## Environment Setup

Before starting the project, create a `.env` file in the root directory with the following parameters:

```env
POSTGRES_USER=xxx
POSTGRES_PASSWORD=xxxx123
POSTGRES_DB=dbmigration

DATABASE_URL=postgresql://xxx:xxxx123@db:5432/dbmigration
```

## How to Start the Project

1. **Build and Start Docker Containers**
   - Make sure Docker and Docker Compose are installed.
   - Run:
     ```sh
     docker-compose up --build
     ```
   - This will start both the Flask API and the PostgreSQL database.

2. **Create Database Tables**
   - After the containers are running, create the tables by executing:
     ```sh
     docker-compose run api flask create-db
     ```
   - This will initialize all required tables in the database.

## Endpoints

### Upload CSV to Departments or Jobs
`POST /upload-csv`
Parameters:
  - `file`: CSV file (.csv)
  - `table_name`: 'departments' or 'jobs'

### Upload CSV to Employees
`POST /employees/upload-csv`
Parameters:
  - `file`: CSV file (.csv)

### Batch Insert Employees
`POST /employees/batch`
Body: JSON array of employee records

### Reports

#### Employees Hired per Job and Department by Quarter
`GET /reports/hires-quarterly?year=YYYY`
Returns the number of employees hired for each job and department in the specified year, divided by quarter.

#### Departments Above Mean Hires
`GET /reports/above-mean-hires?year=YYYY`
Returns departments that hired more employees than the mean for the specified year.

## Example Requests

- Employees hired per job and department (2021):
  - [http://localhost:5000/reports/hires-quarterly?year=2021](http://localhost:5000/reports/hires-quarterly?year=2021)
- Departments above mean hires (2021):
  - [http://localhost:5000/reports/above-mean-hires?year=2021](http://localhost:5000/reports/above-mean-hires?year=2021)

## Missing Features / To Do

- **Swagger Documentation:** The API documentation UI (Swagger) is not yet enabled. Flasgger was attempted but not working; needs troubleshooting or alternative.
- **CSV Upload Limit:** If more than 1000 records are uploaded for employees, the process should be stopped or handled in batches. Currently, only up to 1000 records are accepted per request.

## Data Folder

The `data` folder contains sample CSV files used for testing the API endpoints. These files can be used to verify the upload and batch insert functionalities:

- `departments.csv`: Example data for departments.
- `jobs.csv`: Example data for jobs.
- `employees.csv`: Example data for employees.



## Notes

- All error and status messages are returned in English.
- The project uses Flask, SQLAlchemy, and PostgreSQL. All configuration is managed via Docker Compose and environment variables in `.env`.