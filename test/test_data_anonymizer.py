import pytest
from pytest_lazyfixture import lazy_fixture
import pandas as pd
from packages.suppression_check import DataAnonymizer

@pytest.fixture
def sample_data():
    
    return(pd.read_csv('./data/TestingData.csv'))

###Unit tests###
def test_validate_input_data_type_check():
    """ Test if validate input function catches a non-dataframe input for the data object."""
    with pytest.raises(TypeError):
        DataAnonymizer('fake_input.csv', parent_organization= 'ParentEntity', child_organization= 'ChildEntity', sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='UserRedaction')

def test_validate_input_key_error_checks(sample_data):
    """ Test that validate input properly raises key errors for incorrect inputs."""
    #Test that function catches wrong parent organization entry.
    with pytest.raises(KeyError):
        DataAnonymizer(sample_data, parent_organization='fake_parent_org', child_organization='ChildEntity',  sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='UserRedaction')

    #Test that function catches wrong child organization entry. 
    with pytest.raises(KeyError):
        DataAnonymizer(sample_data, parent_organization='ParentEntity', child_organization='fake_child_org',  sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='UserRedaction')

    #Test that function catches wrong subgroup entry.
    with pytest.raises(KeyError):
        DataAnonymizer(sample_data, parent_organization= 'ParentEntity', child_organization= 'ChildEntity', sensitive_columns= ['Subgroup1', 'fake_subgroup'], redact_column='UserRedaction')

    #Test that function catches wrong redact column entry. 
    with pytest.raises(KeyError):
        DataAnonymizer(sample_data, parent_organization= 'ParentEntity', child_organization= 'ChildEntity', sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='fake_user_redact')


@pytest.mark.parametrize("sample_dataframe, parent_org, child_org, redact_column", [(lazy_fixture('sample_data'), None, None, None),  (lazy_fixture('sample_data'), 'ParentEntity', 'ChildEntity', None)])
def test_create_log(sample_dataframe, parent_org, child_org, redact_column):
    """ Test that the create_log function adds in the columns required for the rest of the class to work."""

    anonymizer = DataAnonymizer(sample_dataframe, parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column, minimum_threshold=10)
    
    if anonymizer.organization_columns[0] is not None:
        log_df = anonymizer.create_log()
        required_columns = {'Grouping', 'MinimumValue', 'MinimumValueSubgroup1', 'MinimumValueSubgroup2', 'RedactBinary', 'Redact', 'RedactBreakdown'}
        assert required_columns.issubset(set(log_df.columns))
    
    if anonymizer.organization_columns[0] is None:
        log_df = anonymizer.create_log()
        required_columns = {'Grouping', 'MinimumValueSubgroup1', 'MinimumValueSubgroup2', 'RedactBinary', 'Redact', 'RedactBreakdown'}
        assert required_columns.issubset(set(log_df.columns))

@pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_data'), 'UserRedaction')])
def test_redact_user_requested_records(sample_dataframe, redact_column):
    """
    Test if 'User-requested redaction' is added to all columns where user specifies redaction
    """

    anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column)
    
    result_df = anonymizer.apply_anonymization()
    
    assert (result_df.loc[(result_df['UserRedact'] == 1), 'Redact'] == 'User-requested redaction').all()

    # assert(result_df.loc[(result_df['UserRedact'] == 1), 'RedactBreakdown'].str.contains('User-requested redaction')).all()


@pytest.mark.parametrize("sample_dataframe, parent_org, child_org, redact_column", [(lazy_fixture('sample_data'), None, None, None), (lazy_fixture('sample_data'), None, None, 'UserRedaction'), (lazy_fixture('sample_data'), 'ParentEntity', 'ChildEntity', None)])
def test_less_than_threshold_no_redact_zero(sample_dataframe, parent_org, child_org, redact_column):
    """
    Test that values less than threshold are properly redacted
    """
    anonymizer = DataAnonymizer(sample_dataframe, parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column, minimum_threshold=10)
    anonymizer.create_log()
    anonymizer.redact_user_requested_records()
    result_df = anonymizer.less_than_threshold()

    print(result_df[['GraduationCount','RedactBreakdown']])

    #Test all values less than or equal to minimum threshold are marked for Primary supression 
    assert (result_df.loc[((result_df['GraduationCount']<= anonymizer.minimum_threshold) & (result_df['GraduationCount'] != 0) ), 'Redact'] == 'Primary Suppression').all()
    #Test that none of the values where minimum threshold are equal to zero are marked for Primary Supression 
    assert not (result_df.loc[((result_df['GraduationCount']<= anonymizer.minimum_threshold) & (result_df['GraduationCount'] == 0) ), 'Redact'] == 'Primary Suppression').all()
    #Test that Redact breakdown contains an indicator of why the values were redacted
    assert (result_df.loc[((result_df['GraduationCount']<= anonymizer.minimum_threshold) & (result_df['GraduationCount'] != 0) ), 'RedactBreakdown'].str.contains(f"Less Than or equal to {anonymizer.minimum_threshold} and not equal to zero")).all()


@pytest.mark.parametrize("sample_dataframe, parent_org, child_org, redact_column, redact_zero", [(lazy_fixture('sample_data'), None, None, None, True), (lazy_fixture('sample_data'), None, None, 'UserRedaction', True), (lazy_fixture('sample_data'), 'ParentEntity', 'ChildEntity', None, True)])
def test_less_than_threshold_redact_zero(sample_dataframe, parent_org, child_org, redact_column, redact_zero):
    """
    Test that values less than threshold are properly redacted
    """
    anonymizer = DataAnonymizer(sample_dataframe, parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column, redact_zero = redact_zero, minimum_threshold=10)
    anonymizer.create_log()
    anonymizer.redact_user_requested_records()
    result_df = anonymizer.less_than_threshold()

    #Test all values less than or equal to minimum threshold, including zero, are marked for Primary supression. 
    assert (result_df.loc[(result_df['GraduationCount']<= anonymizer.minimum_threshold), 'Redact'] == 'Primary Suppression').all()

    #Test that Redact breakdown contains an indicator of why the values were redacted, and that it references zeroes being applicable for redaction. 
    assert (result_df.loc[(result_df['GraduationCount']<= anonymizer.minimum_threshold), 'RedactBreakdown'].str.contains(f"Less Than or equal to {anonymizer.minimum_threshold} or zero")).all()


# I think the below currently does not work as we set RedactBinary to 1 in redact_threshold() when the redaction is applied. I need to think of a way to check for proper redaction of overlap without using RedactBinary. 

# @pytest.mark.parametrize("sample_dataframe, parent_org, child_org, redact_column, redact_zero", [(lazy_fixture('sample_data'), None, None, None, True), (lazy_fixture('sample_data'), None, None, 'UserRedaction', True), (lazy_fixture('sample_data'), 'ParentEntity', 'ChildEntity', None, True)])
# def test_redact_threshold(sample_dataframe, parent_org, child_org, redact_column, redact_zero):
#     """
#     Test that values less than threshold are properly redacted
#     """
#     anonymizer = DataAnonymizer(sample_dataframe, parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column, redact_zero = redact_zero, minimum_threshold=10)
#     anonymizer.create_log()
#     anonymizer.redact_user_requested_records()
#     anonymizer.less_than_threshold()
#     result_df = anonymizer.redact_threshold()

#     #Test that values with the highest overlapping count are marked for secondary supression.
#     assert (result_df.loc[(result_df['Overlapping'] == result_df['Overlapping'].max()) & (result_df['RedactBinary'] == 0), 'Redact'] == 'Overlapping threshold secondary supression').all()

#     #Test that Redact breakdown has an indicator added that secondary supression was applied based on overlap.
#     assert (result_df.loc[(result_df['Overlapping'] == result_df['Overlapping'].max()) & (result_df['RedactBinary'] == 0), 'Redact'].str.contains('Overlapping threshold secondary supression')).all()

###Functional tests###

@pytest.mark.parametrize("sample_dataframe, parent_org, child_org, redact_column", [(lazy_fixture('sample_data'), None, None, None), (lazy_fixture('sample_data'), None, None, 'UserRedaction'), (lazy_fixture('sample_data'), 'ParentEntity', 'ChildEntity', None)])
def test_apply_anonymization_return_dataframe_with_redact_column(sample_dataframe, parent_org, child_org, redact_column):
    """
    Test if 'Redact' column is added to the DataFrame.
    """
    anonymizer = DataAnonymizer(sample_dataframe, parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column)
    anonymizer.create_log()
    result_df = anonymizer.apply_anonymization()
    assert 'Redact' in result_df.columns

@pytest.mark.parametrize("sample_dataframe, parent_org, child_org, redact_column", [(lazy_fixture('sample_data'), None, None, None), (lazy_fixture('sample_data'), None, None, 'UserRedaction'), (lazy_fixture('sample_data'), 'ParentEntity', 'ChildEntity', None)])
def test_apply_anonymization_redacts_at_least_two_rows_per_sensitive_column(sample_dataframe, parent_org, child_org, redact_column):
    """
    Test if at least two rows per sensitive column are redacted.
    """
    anonymizer = DataAnonymizer(sample_dataframe, parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column)
    result_df = anonymizer.apply_anonymization()
    redacted = result_df[result_df['Redact'] != 'Not Redacted']

    for column in anonymizer.sensitive_columns:
        assert (redacted.groupby(column)['Redact'].count()>=2).all()

# @pytest.mark.parametrize("sample_dataframe, redact_column", [(lazy_fixture('sample_data'), None), (lazy_fixture('sample_data'), 'UserRedaction')])
# def test_correct_redaction_method(sample_dataframe, redact_column):
#     """
#     Test if at least two rows per sensitive column are redacted.
#     """
#     anonymizer = DataAnonymizer(sample_dataframe, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column)
#     result_df = anonymizer.apply_anonymization()

#     review_df = sample_dataframe.merge(result_df, on =['Subgroup1', 'Subgroup2', 'Counts'])

#     query_result = review_df[(review_df['TestRedactBinary'] != review_df['RedactBinary'])]

#     print(query_result)

#     assert query_result.empty, "Query returned results, test failed"
       
        

