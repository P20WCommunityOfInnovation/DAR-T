import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from packages.suppression_check import DataAnonymizer

class TestDataAnonymizer(unittest.TestCase):
    
    def setUp(self):
        #Create sample DataFrame from example csv
        example_data = {
            'Subgroup1': ['STEM', 'STEM', 'STEM', 'STEM', 'STEM', 'Business', 'Business', 'Business', 'Business', 'Business', 'Education', 'Education', 'Education', 'Education', 'Education', 'Health', 'Health', 'Health', 'Health', 'Health', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral'],
            'Subgroup2': ['Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate'],
            'Counts': [10, 9, 20, 100, 40, 15, 40, 15, 90, 11, 50, 30, 12, 6, 44, 100, 20, 100, 30, 70, 25, 11, 60, 50, 10] 
            }   
        
        self.df = pd.DataFrame(example_data)
    
    def test_functions_return_dataframe_with_redact_column(self):
        anonymizer = DataAnonymizer(self.df, sensitive_columns=['Subgroup1', 'Subgroup2']) 
        result_df = anonymizer.less_than_threshold_not_zero('Counts', minimum_threshold=10)
        self.assertTrue('Redact' in result_df.columns)

        result_df = anonymizer.redact_threshold('Counts', minimum_threshold=10)
        self.assertTrue('Redact' in result_df.columns)

    def test_redact_threshold_redacts_at_least_two_rows_per_sensitive_column(self):
        sensitive_columns = ['Subgroup1', 'Subgroup2']
        anonymizer = DataAnonymizer(self.df, sensitive_columns=sensitive_columns)
        result_df = anonymizer.redact_threshold('Counts', minimum_threshold=10)
        redacted = result_df[result_df['Redact'].notnull()]
        
        for sensitive_column in sensitive_columns:
            self.assertTrue((redacted.groupby(sensitive_column)['Redact'].count() >= 2).all()) 

    def test_redact_threshold_matches_expected_output(self):
        expected_data = {
            'Subgroup1': ['STEM', 'STEM', 'STEM', 'STEM', 'STEM', 'Business', 'Business', 'Business', 'Business', 'Business', 'Education', 'Education', 'Education', 'Education', 'Education', 'Health', 'Health', 'Health', 'Health', 'Health', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral', 'Social and Behavioral'],
            'Subgroup2': ['Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate','Certificate', 'Associate', 'Bachelor', 'Masters', 'Doctorate'],
            'Counts': [10, 9, 20, 100, 40, 15, 40, 15, 90, 11, 50, 30, 12, 6, 44, 100, 20, 100, 30, 70, 25, 11, 60, 50, 10], 
            'Redact': ['NotRedacted', 'Less Than 10 and not zero', 'NotRedacted', 'Overlapping threshold secondary suppression', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'Overlapping threshold secondary suppression', 'NotRedacted', 'Less Than 10 and not zero', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted', 'NotRedacted'],
            }   

        expected_df = pd.DataFrame(expected_data)

        anonymizer = DataAnonymizer(self.df, sensitive_columns=['Subgroup1', 'Subgroup2'])
        result_df = anonymizer.redact_threshold('Counts', minimum_threshold=10)

        #Filling NaN values with 'Not Redacted' to avoid issues with NaN comparison
        result_df['Redact'] = result_df['Redact'].fillna('NotRedacted')

        self.assertIsNone(assert_frame_equal(result_df, expected_df)) #assert_frame_equal returns None when DataFrames are equal, so we check for None instead of True 

if __name__ == '__main__':
    unittest.main()
       
        

