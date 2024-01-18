import pytest
from pytest_lazyfixture import lazy_fixture
import pandas as pd
from packages.suppression_check import DataAnonymizer

@pytest.fixture
def sample_no_user_redact():
    """Creates a sample dataframe without a user specified redaction column"""
    user_data = {
        'Subgroup1': ['STEM', 'STEM', 'STEM', 'STEM', 'STEM', 'Business', 'Business', 'Business', 'Business', 'Business', 'Education', 'Education', 'Education', 'Education', 'Education', 'Health', 'Health', 'Health', 'Health', 'Health', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral'],
        'Subgroup2': ['Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate'],
        'Counts': [10, 9, 20, 100, 40, 15, 40, 15, 90, 11, 50, 30, 12, 6, 44, 100, 20, 100, 30, 70, 25, 11, 60, 50, 10],
        'RedactBinary':[1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 1]
        }   
    
    return pd.DataFrame(user_data)

@pytest.fixture
def sample_user_redact():
    """Creates a sample dataframe with a user specified redaction column"""
    user_redact_data = {
        'Subgroup1': ['STEM', 'STEM', 'STEM', 'STEM', 'STEM', 'Business', 'Business', 'Business', 'Business', 'Business', 'Education', 'Education', 'Education', 'Education', 'Education', 'Health', 'Health', 'Health', 'Health', 'Health', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral'],
        'Subgroup2': ['Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate'],
        'Counts': [10, 9, 20, 100, 40, 15, 40, 15, 90, 11, 50, 30, 12, 6, 44, 100, 20, 100, 30, 70, 25, 11, 60, 50, 10],
        'UserRedact': [1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]    
    }

    return pd.DataFrame(user_redact_data)

@pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_no_user_redact'), None), (lazy_fixture('sample_user_redact'), 'UserRedact')])
def test_apply_anonymization_return_dataframe_with_redact_column(sample_dataframe, redact_column):
    """
    Test if 'Redact' column is added to the DataFrame.
    """
    anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='Counts', redact_column=redact_column)
    anonymizer.create_log()
    result_df = anonymizer.apply_anonymization()
    assert 'Redact' in result_df.columns

@pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_no_user_redact'), None), (lazy_fixture('sample_user_redact'), 'UserRedact')])
def test_apply_anonymization_redacts_at_least_two_rows_per_sensitive_column(sample_dataframe, redact_column):
    """
    Test if at least two rows per sensitive column are redacted.
    """
    anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='Counts', redact_column=redact_column)
    result_df = anonymizer.apply_anonymization()
    redacted = result_df[result_df['Redact'] != 'Not Redacted']

    for column in anonymizer.sensitive_columns:
        assert (redacted.groupby(column)['Redact'].count()>=2).all()

@pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_no_user_redact'), None), (lazy_fixture('sample_user_redact'), 'UserRedact')])
def test_correct_redaction_method(sample_dataframe, redact_column):
    """
    Test if at least two rows per sensitive column are redacted.
    """
    anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='Counts', redact_column=redact_column) 
    result_df = anonymizer.apply_anonymization()

    review_df = sample_dataframe.merge(result_df, on =['Subgroup1', 'Subgroup2', 'Counts'])

    query_result = review_df[(review_df['RedactBinaryTest'] != review_df['RedactBinary'])]

    assert query_result.empty, "Query returned results, test failed"
       
        

