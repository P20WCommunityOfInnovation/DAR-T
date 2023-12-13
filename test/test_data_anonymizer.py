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
        """
        Test if 'Redact' column is added to the DataFrame.
        """
        anonymizer = DataAnonymizer(self.df, sensitive_columns=['Subgroup1', 'Subgroup2'], frequency='Counts')
        anonymizer.create_log()
        for function in [anonymizer.less_than_threshold_not_zero, anonymizer.redact_threshold]:
            with self.subTest(function=function):
                result_df = function()
                self.assertIn('Redact', result_df.columns)

    def test_redact_threshold_redacts_at_least_two_rows_per_sensitive_column(self):
        """
        Test if at least two rows per sensitive column are redacted.
        """
        sensitive_columns = ['Subgroup1', 'Subgroup2']
        anonymizer = DataAnonymizer(self.df, sensitive_columns=sensitive_columns, frequency='Counts')
        anonymizer.create_log()
        result_df = anonymizer.redact_threshold()
        redacted = result_df[result_df['Redact'].notnull()]

        for column in sensitive_columns:
            with self.subTest(column=column):
                self.assertTrue((redacted.groupby(column)['Redact'].count() >= 2).all())

if __name__ == '__main__':
    unittest.main()
       
        

