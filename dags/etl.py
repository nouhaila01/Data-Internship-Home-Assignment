from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import pandas as pd
import json
import os

TABLES_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""
# Définir le chemin du répertoire pour les fichiers extraits
extracted_dir = 'staging/extracted'

# Définir le chemin du répertoire pour les fichiers transformés
transformed_dir = 'staging/transformed'

# Définir le nom de la colonne contenant les données JSON dans votre fichier CSV
context_column = 'context'

@task()
def extract():
    """Extract data from jobs.csv."""
    df = pd.read_csv('source/jobs.csv')
    # Assuming context_column contains JSON data and you want to save it as text files
    os.makedirs(extracted_dir, exist_ok=True)
    for index, row in df.iterrows():
        context_data = row[context_column]
        with open(f'{extracted_dir}/extracted_{index}.txt', 'w') as file:
            file.write(str(context_data))

@task()
def transform():
    """Clean and convert extracted elements to json."""
    os.makedirs(transformed_dir, exist_ok=True)
    for filename in os.listdir(extracted_dir):
        with open(os.path.join(extracted_dir, filename), 'r') as file:
            extracted_data = json.load(file)

        # Your cleaning and transformation logic 
        transformed_data = {
            "job": {
                "title": extracted_data.get('title', ''),
                "industry": extracted_data.get('industry', ''),
                "description": extracted_data.get('description', ''),
                "employment_type": extracted_data.get('employment_type', ''),
                "date_posted": extracted_data.get('date_posted', ''),
            },
            "company": {
                "name": extracted_data.get('company_name', ''),
                "link": extracted_data.get('company_linkedin_link', ''),
            },
            "education": {
                "required_credential": extracted_data.get('job_required_credential', ''),
            },
            "experience": {
                "months_of_experience": extracted_data.get('job_months_of_experience', ''),
                "seniority_level": extracted_data.get('seniority_level', ''),
            },
            "salary": {
                "currency": extracted_data.get('salary_currency', ''),
                "min_value": extracted_data.get('salary_min_value', ''),
                "max_value": extracted_data.get('salary_max_value', ''),
                "unit": extracted_data.get('salary_unit', ''),
            },
            "location": {
                "country": extracted_data.get('country', ''),
                "locality": extracted_data.get('locality', ''),
                "region": extracted_data.get('region', ''),
                "postal_code": extracted_data.get('postal_code', ''),
                "street_address": extracted_data.get('street_address', ''),
                "latitude": extracted_data.get('latitude', ''),
                "longitude": extracted_data.get('longitude', ''),
            },
        }

        with open(f'{transformed_dir}/transformed_{filename[:-4]}.json', 'w') as file:
            json.dump(transformed_data, file)


@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    # Ajouter le chemin du répertoire transformé (assurez-vous que la variable transformed_dir est définie)
    transformed_dir = 'staging/transformed'
    
    for filename in os.listdir(transformed_dir):
        with open(os.path.join(transformed_dir, filename), 'r') as file:
            transformed_data = json.load(file)

        # Extraire les informations nécessaires de transformed_data pour chaque table
        job_data = transformed_data.get('job', {})
        company_data = transformed_data.get('company', {})
        education_data = transformed_data.get('education', {})
        experience_data = transformed_data.get('experience', {})
        salary_data = transformed_data.get('salary', {})
        location_data = transformed_data.get('location', {})

        # Insérer les données dans les tables respectives

        # Table 'job'
        job_insert_query = f"""
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES ('{job_data.get('title', '')}', '{job_data.get('industry', '')}',
                    '{job_data.get('description', '')}', '{job_data.get('employment_type', '')}',
                    '{job_data.get('date_posted', '')}');
        """
        sqlite_hook.run(job_insert_query)

        # Table 'company'
        company_insert_query = f"""
            INSERT INTO company (job_id, name, link)
            VALUES ((SELECT id FROM job WHERE title = '{job_data.get('title', '')}'), 
                    '{company_data.get('name', '')}', '{company_data.get('link', '')}');
        """
        sqlite_hook.run(company_insert_query)

        # Table 'education'
        education_insert_query = f"""
            INSERT INTO education (job_id, required_credential)
            VALUES ((SELECT id FROM job WHERE title = '{job_data.get('title', '')}'),
                    '{education_data.get('required_credential', '')}');
        """
        sqlite_hook.run(education_insert_query)

        # Table 'experience'
        experience_insert_query = f"""
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES ((SELECT id FROM job WHERE title = '{job_data.get('title', '')}'),
                    {experience_data.get('months_of_experience', 0)},
                    '{experience_data.get('seniority_level', '')}');
        """
        sqlite_hook.run(experience_insert_query)

        # Table 'salary'
        salary_insert_query = f"""
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES ((SELECT id FROM job WHERE title = '{job_data.get('title', '')}'),
                    '{salary_data.get('currency', '')}',
                    {salary_data.get('min_value', 0)},
                    {salary_data.get('max_value', 0)},
                    '{salary_data.get('unit', '')}');
        """
        sqlite_hook.run(salary_insert_query)

        # Table 'location'
        location_insert_query = f"""
            INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES ((SELECT id FROM job WHERE title = '{job_data.get('title', '')}'),
                    '{location_data.get('country', '')}',
                    '{location_data.get('locality', '')}',
                    '{location_data.get('region', '')}',
                    '{location_data.get('postal_code', '')}',
                    '{location_data.get('street_address', '')}',
                    {location_data.get('latitude', 0)},
                    {location_data.get('longitude', 0)});
        """
        sqlite_hook.run(location_insert_query)


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""
    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extract_data = extract()
    transform_data = transform()
    load_data = load()

    create_tables >> extract_data >> transform_data >> load_data

etl_dag()
