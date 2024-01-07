import os
import json
from unittest.mock import patch, mock_open
import pytest
from dags.etl import transform, load, SqliteHook
from unittest.mock import patch, Mock, mock_open

@pytest.fixture

def sample_extracted_data():
    return {
        "title": "Sample Job Title",
        "industry": "IT",
        "description": "Sample job description",
        "employment_type": "Full-time",
        "date_posted": "2022-01-07",
        "company_name": "Sample Company",
        "company_linkedin_link": "https://www.linkedin.com/company/sample",
        "job_required_credential": "Sample Credential",
        "job_months_of_experience": "60",
        "seniority_level": "Senior",
        "salary_currency": "USD",
        "salary_min_value": "80000",
        "salary_max_value": "120000",
        "salary_unit": "Year",
        "country": "Sample Country",
        "locality": "Sample Locality",
        "region": "Sample Region",
        "postal_code": "12345",
        "street_address": "123 Main St",
        "latitude": "12.3456",
        "longitude": "-78.9012",
    }

def test_transform(sample_extracted_data):
    # Mocking external dependencies
    with patch('os.makedirs') as mock_makedirs, \
         patch('os.listdir', return_value=['staging/transformed/file1.json', 'staging/transformed/file2.json']), \
         patch('builtins.open', mock_open(read_data=json.dumps(sample_extracted_data))) as mock_open, \
         patch('json.dump') as mock_json_dump:

        # Call the function
        transform()

    # Assertions
    mock_makedirs.assert_called_once_with('staging/transformed', exist_ok=True)
    mock_open.assert_called_once_with('staging/extracted/sample_file.txt', 'r')
    mock_json_dump.assert_called_once()

    # Additional assertions based on your transformation logic
    mock_json_dump.assert_called_once_with({
        "job": {
            "title": "Sample Job Title",
            "industry": "IT",
            "description": "Sample job description",
            "employment_type": "Full-time",
            "date_posted": "2022-01-07",
        },
        "company": {
            "name": "Sample Company",
            "link": "https://www.linkedin.com/company/sample",
        },
        "education": {
            "required_credential": "Sample Credential",
        },
        "experience": {
            "months_of_experience": "60",
            "seniority_level": "Senior",
        },
        "salary": {
            "currency": "USD",
            "min_value": "80000",
            "max_value": "120000",
            "unit": "Year",
        },
        "location": {
            "country": "Sample Country",
            "locality": "Sample Locality",
            "region": "Sample Region",
            "postal_code": "12345",
            "street_address": "123 Main St",
            "latitude": "12.3456",
            "longitude": "-78.9012",
        },
    }, mock_open.return_value.__enter__.return_value)


# pytest -v C:\Users\Nouha\Desktop\pfeDNA\Data-Internship-Home-Assignment-main\test\__init__.py


@patch('dags.etl.SqliteHook')
@patch('builtins.open', new_callable=mock_open, read_data='{}')
def test_load(mock_open, mock_sqlite_hook):
    # Set up your test data and directory
    transformed_dir = 'staging/transformed'
    mock_open.return_value = ['file1.json', 'file2.json'] # Adjust the file names as needed

    # Create a mock for sqlite_hook
    mock_run = Mock()
    mock_sqlite_hook.return_value.run = mock_run

    # Call the function
    load()

    # Verify that the expected queries were executed
    expected_queries = [
        # Adjust the expected queries based on your function's logic
        "INSERT INTO job",
        "INSERT INTO company",
        "INSERT INTO education",
        "INSERT INTO experience",
        "INSERT INTO salary",
        "INSERT INTO location",
    ]

    for query in expected_queries:
        mock_run.assert_any_call(query)

    # Verify that the correct files were opened
    expected_files = [os.path.join(transformed_dir, 'file1.json'), os.path.join(transformed_dir, 'file2.json')]
    for expected_file in expected_files:
        mock_open.assert_any_call(expected_file, 'r')

# pytest -v C:\Users\Nouha\Desktop\pfeDNA\Data-Internship-Home-Assignment-main\test\__init__.py
