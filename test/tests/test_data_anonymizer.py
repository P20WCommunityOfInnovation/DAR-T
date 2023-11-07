import unittest
import pandas as pd
from src.data_anonymizer import DataAnonymizer
import numpy as np

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
            'Redact': [np.nan, 'Less Than 10 and not zero', np.nan, 'Overlapping threshold secondary suppression', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 'Overlapping threshold secondary suppression', np.nan, 'Less Than 10 and not zero', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            }   

        expected_df = pd.DataFrame(expected_data)

        anonymizer = DataAnonymizer(self.df, sensitive_columns=['Subgroup1', 'Subgroup2'])
        result_df = anonymizer.redact_threshold('Counts', minimum_threshold=10)

        self.assertTrue(result_df.equals(expected_df))
       
        

        