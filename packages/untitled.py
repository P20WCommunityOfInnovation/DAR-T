import pandas as pd
# Define a class called DataAnonymizer
class DataAnonymizer:
    
    # Initialize the class with a dataframe (df) and optionally, a list of sensitive columns
    def __init__(self, df, organization_columns=None, sensitive_columns=None, frequency=None, minimum_threshold=10):
        # Create a copy of the input dataframe and store it as an instance variable
        self.df = df.copy()
        
        # Check the type of sensitive_columns and store it as an instance variable
        if sensitive_columns is None:
            print('Sensitive Column is empty')  # Print a message if sensitive_columns is None
            self.sensitive_columns = []  # Initialize an empty list
        elif isinstance(sensitive_columns, str):
            self.sensitive_columns = [sensitive_columns]  # Convert a single string to a list with one item
        else:
            self.sensitive_columns = sensitive_columns  # Store the provided list

        # Check the type of organization_columns and store it as an instance variable
        if organization_columns is None:
            print('Organization Column is empty')  # Print a message if organization_columns is None
            self.organization_columns = []  # Initialize an empty list
        elif isinstance(organization_columns, str):
            self.organization_columns = [organization_columns]  # Convert a single string to a list with one item
        else:
            self.organization_columns = organization_columns  # Store the provided list
            
        self.frequency = frequency

        self.minimum_threshold = minimum_threshold
        
        df_log = self.df[self.organization_columns + self.sensitive_columns + [self.frequency]]
        for organization_column in self.organization_columns:
            for sensitive_column in self.sensitive_columns:
                df_temp = self.df.groupby([organization_column] + [sensitive_column])[self.frequency].sum().reset_index()
                df_log = pd.concat([df_temp, df_log])

        df_log.drop_duplicates(inplace=True)
        
        self.df_log = df_log[self.organization_columns + self.sensitive_columns + [self.frequency]]
        
        self.df_log = self.df_log.reset_index(drop=True)
        
        self.df_log.loc[:, 'RedactBinary'] = 0

        self.df_log.loc[:, 'Redact'] = 'Not Redacted'
        display(self.df_log)

        if organization_columns is not None:
            # Filter rows where the value in the column specified by 'frequency' is greater than or equal to 'minimum_threshold'
            df_minimum_threshold = self.df_log[self.df_log[self.frequency] >= self.minimum_threshold]
            
            # Group the filtered dataframe by 'organization_columns' and get the minimum value in the 'frequency' column
            df_grouped_min = df_minimum_threshold.groupby(self.organization_columns)[self.frequency].min().reset_index()
            df_grouped_min.rename(columns = {self.frequency: "MinimumValue"}, inplace=True)
            
            # Merge the filtered and grouped dataframes based on 'organization_columns'
            self.df_log = self.df_log.merge(df_grouped_min, on=organization_columns, how='left')

            # drop NA rows based on merge
            self.df_log.dropna(axis= 0, how = "any", subset= self.organization_columns + self.sensitive_columns, inplace=True)
            self.df_log['MinimumValue'] = self.df_log['MinimumValue'].fillna(0)
        
    # Method to redact values in the dataframe that are less than a minimum threshold but not zero
    def less_than_threshold_not_zero(self):
        # Create a boolean mask that identifies rows where the column specified by 'frequency'
        # has values less than 'minimum_threshold' and not equal to zero
        mask = (self.df_log[self.frequency] < self.minimum_threshold) & (self.df_log[self.frequency] != 0)

        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update a new column named 'Redact' with a message for the rows that meet the condition specified by the mask
        self.df_log.loc[mask, 'Redact'] = f'Less Than {self.minimum_threshold} and not zero'
        
        # Return the updated dataframe
        return self.df_log