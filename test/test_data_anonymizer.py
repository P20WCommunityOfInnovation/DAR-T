import pytest
from pytest_lazyfixture import lazy_fixture
import pandas as pd
from packages.suppression_check import DataAnonymizer

@pytest.fixture
def sample_data():
    
    return(pd.read_csv('./data/UserData.csv'))

@pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_data'), None), (lazy_fixture('sample_data'), 'UserRedact')])
def test_apply_anonymization_return_dataframe_with_redact_column(sample_dataframe, redact_column):
    """
    Test if 'Redact' column is added to the DataFrame.
    """
    anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='Counts', redact_column=redact_column)
    anonymizer.create_log()
    result_df = anonymizer.apply_anonymization()
    assert 'Redact' in result_df.columns

@pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_data'), None), (lazy_fixture('sample_data'), 'UserRedact')])
def test_apply_anonymization_redacts_at_least_two_rows_per_sensitive_column(sample_dataframe, redact_column):
    """
    Test if at least two rows per sensitive column are redacted.
    """
    anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='Counts', redact_column=redact_column)
    result_df = anonymizer.apply_anonymization()
    redacted = result_df[result_df['Redact'] != 'Not Redacted']

    for column in anonymizer.sensitive_columns:
        assert (redacted.groupby(column)['Redact'].count()>=2).all()

# @pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_no_user_redact'))])
# def test_correct_redaction_method(sample_dataframe):
#     """
#     Test if at least two rows per sensitive column are redacted.
#     """
#     anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='Counts')
#     result_df = anonymizer.apply_anonymization()

#     review_df = sample_dataframe.merge(result_df, on =['Subgroup1', 'Subgroup2', 'Counts'])

#     query_result = review_df[(review_df['TestRedactBinary'] != review_df['RedactBinary'])]

#     assert query_result.empty, "Query returned results, test failed"
       
        

