from collections import OrderedDict

import numpy as np
import pytest
import pandas as pd

from dar_tool.suppression_check import DataAnonymizer


###Unit tests### 
        
def test_validate_input_data_type_check():
    """ Test if validate input function catches a non-dataframe input for the data object."""
    with pytest.raises(AttributeError):
        DataAnonymizer('fake_input.csv', parent_organization= 'ParentEntity', child_organization= 'ChildEntity', sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='UserRedaction')

def test_validate_input_key_error_checks():
    sample_data = pd.read_csv('./data/TestingData.csv')

    """ Test that validate input properly raises key errors for incorrect inputs."""
    #Test that function catches wrong parent organization entry.
    with pytest.raises(KeyError):
        da = DataAnonymizer(sample_data, parent_organization='fake_parent_org', child_organization='ChildEntity',  sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='UserRedaction')
        da.apply_anonymization()

    #Test that function catches wrong child organization entry. 
    with pytest.raises(KeyError):
        da = DataAnonymizer(sample_data, parent_organization='ParentEntity', child_organization='fake_child_org',  sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='UserRedaction')
        da.apply_anonymization()

    #Test that function catches wrong subgroup entry.
    with pytest.raises(KeyError):
        da = DataAnonymizer(sample_data, parent_organization= 'ParentEntity', child_organization= 'ChildEntity', sensitive_columns= ['Subgroup1', 'fake_subgroup'], redact_column='UserRedaction')
        da.apply_anonymization()

    #Test that function catches wrong redact column entry. 
    with pytest.raises(KeyError):
        da = DataAnonymizer(sample_data, parent_organization= 'ParentEntity', child_organization= 'ChildEntity', sensitive_columns= ['Subgroup1', 'Subgroup2'], redact_column='fake_user_redact')
        da.apply_anonymization()


@pytest.mark.parametrize(" parent_org, child_org, redact_column", [ ('ParentEntity', 'ChildEntity', None)])
def test_create_log(parent_org, child_org, redact_column):
    """ Test that the create_log function adds in the columns required for the rest of the class to work."""

    anonymizer = DataAnonymizer(pd.read_csv('./data/TestingData.csv'), parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column, minimum_threshold=10)
    
    if anonymizer.organization_columns[0] is not None:
        log_df = anonymizer.create_log()
        required_columns = {'Grouping', 'MinimumValue', 'MinimumValueSubgroup1', 'MinimumValueSubgroup2', 'RedactBinary', 'Redact', 'RedactBreakdown'}
        assert required_columns.issubset(set(log_df.columns))
    
    if anonymizer.organization_columns[0] is None:
        log_df = anonymizer.create_log()
        required_columns = {'Grouping', 'MinimumValueSubgroup1', 'MinimumValueSubgroup2', 'RedactBinary', 'Redact', 'RedactBreakdown'}
        assert required_columns.issubset(set(log_df.columns))

@pytest.mark.parametrize("parent_org, child_org, redact_column", [('ParentEntity', 'ChildEntity', 'UserRedaction')])
def test_redact_user_requested_records(parent_org, child_org, redact_column):
    """
    Test if 'User-requested redaction' is added to all columns where user specifies redaction
    """
    frequency = 'CohortCount'
    anonymizer = DataAnonymizer(pd.read_csv('./data/TestingData.csv'), parent_organization = parent_org, child_organization = child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column)
    
    result_df = anonymizer.apply_anonymization()

    print(result_df[['RedactBreakdown','UserRedaction']])
    
    assert(result_df.loc[(result_df[redact_column] == 1), 'RedactBreakdown'].str.contains('User-requested redaction')).all()

@pytest.mark.parametrize("parent_org, child_org, redact_column", [('ParentEntity', 'ChildEntity', 'UserRedaction')])
def test_redact_user_requested_records_multiple_frequency(parent_org, child_org, redact_column):
    df = pd.read_csv('./data/TestingData.csv')
    sensitive_columns = ['Subgroup1', 'Subgroup2']
    frequency = ['GraduationCount','CohortCount']
    redact_column=redact_column
    redact_value = 'xx'
    anonymizer = DataAnonymizer(pd.read_csv('./data/TestingData.csv'), parent_organization = parent_org, child_organization = child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], redact_column=redact_column)
    df_merged = anonymizer.process_multiple_frequency_col(['GraduationCount','CohortCount'])
    print("done test_redact_user_requested_records_multiple_frequency")
    print(df_merged)


@pytest.mark.parametrize("parent_org, child_org, redact_column", [('ParentEntity', 'ChildEntity', None)])
def test_less_than_threshold_no_redact_zero(parent_org, child_org, redact_column):
    """
    Test that values less than threshold are properly redacted
    """
    anonymizer = DataAnonymizer(pd.read_csv('./data/TestingData.csv'), parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column, minimum_threshold=10)
    anonymizer.create_log()
    anonymizer.redact_user_requested_records()
    result_df = anonymizer.less_than_threshold()

    #Test all values less than or equal to minimum threshold are marked for Primary supression 
    assert (result_df.loc[((result_df['GraduationCount']<= anonymizer.minimum_threshold) & (result_df['GraduationCount'] != 0) ), 'Redact'] == 'Primary Suppression').all()
    #Test that none of the values where minimum threshold are equal to zero are marked for Primary Supression 
    assert not (result_df.loc[((result_df['GraduationCount']<= anonymizer.minimum_threshold) & (result_df['GraduationCount'] == 0) ), 'Redact'] == 'Primary Suppression').all()
    #Test that Redact breakdown contains an indicator of why the values were redacted
    assert (result_df.loc[((result_df['GraduationCount']<= anonymizer.minimum_threshold) & (result_df['GraduationCount'] != 0) ), 'RedactBreakdown'].str.contains(f"Less Than or equal to {anonymizer.minimum_threshold} and not equal to zero")).all()


@pytest.mark.parametrize("parent_org, child_org, redact_column, redact_zero", [ ('ParentEntity', 'ChildEntity', None, True)])
def test_less_than_threshold_redact_zero(parent_org, child_org, redact_column, redact_zero):
    """
    Test that values less than threshold are properly redacted
    """
    anonymizer = DataAnonymizer(pd.read_csv('./data/TestingData.csv'), parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column, redact_zero = redact_zero, minimum_threshold=10)
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

@pytest.mark.parametrize("parent_org, child_org, redact_column", [('ParentEntity', 'ChildEntity', None)])
def test_apply_anonymization_return_dataframe_with_redact_column(parent_org, child_org, redact_column):
    """
    Test if 'Redact' column is added to the DataFrame.
    """
    anonymizer = DataAnonymizer(pd.read_csv('./data/TestingData.csv'), parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column)
    anonymizer.create_log()
    result_df = anonymizer.apply_anonymization()
    assert 'Redact' in result_df.columns

@pytest.mark.parametrize("parent_org, child_org, redact_column", [ ('ParentEntity', 'ChildEntity', None)])
def test_apply_anonymization_redacts_at_least_two_rows_per_sensitive_column(parent_org, child_org, redact_column):
    """
    Test if at least two rows per sensitive column are redacted.
    """
    anonymizer = DataAnonymizer(pd.read_csv('./data/TestingData.csv'), parent_organization= parent_org, child_organization= child_org, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='GraduationCount', redact_column=redact_column)
    result_df = anonymizer.apply_anonymization()
    redacted = result_df[result_df['Redact'] != 'Not Redacted']

    for column in anonymizer.sensitive_columns:
        assert (redacted.groupby(column)['Redact'].count()>=2).all()

def test_nebraska_sample_data_with_one_org_level():
    # Set seed for reproducibility
    np.random.seed(1234)

    # define data frame
    organization = 'org'
    sensitive = ['gr1', 'gr2']
    grps = [organization] + sensitive

    n = 500
    # n = 2500
    # n = 5000

    gr1 = np.random.choice(list('ABCD'), n, p=np.array([1, 2, 3, 4]) / 10)
    gr2 = np.random.choice([1, 2, 3, 4], n, p=[0.25, 0.25, 0.25, 0.25])
    org = np.random.choice(['Org1', 'Org2'], n, p=[0.3, 0.7])

    dat = pd.DataFrame({'org': org, 'gr1': gr1, 'gr2': gr2})

    df = (dat
        .astype(str)
        .groupby(grps, dropna=False)
        .size()
        .reset_index(name='frequency')
        .astype(str)
        .assign(frequency=lambda x: x['frequency'].astype(int))
        .assign(UserRedact=0))


    # Instantiate the anonymizer
    anonymizer = DataAnonymizer(df, child_organization=organization, sensitive_columns=sensitive, minimum_threshold=10, frequency='frequency')

    redacted = anonymizer.apply_anonymization()

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
       
        

