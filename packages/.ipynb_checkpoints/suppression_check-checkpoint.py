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

        if organization_columns is not None:
            # Filter rows where the value in the column specified by 'frequency' is greater than or equal to 'minimum_threshold'
            df_minimum_threshold = self.df_log[self.df_log[self.frequency] >= self.minimum_threshold]
            
            # Group the filtered dataframe by 'organization_columns' and get the minimum value in the 'frequency' column
            df_grouped_min = df_minimum_threshold.groupby(self.organization_columns)[self.frequency].min().reset_index()
            df_grouped_min.rename(columns = {self.frequency: "MinimumValue"}, inplace=True)
            
            # Merge the filtered and grouped dataframes based on 'organization_columns'
            self.df_log = self.df_log.merge(df_grouped_min, on=self.organization_columns, how='inner')
        
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

    # Method to redact values in the dataframe that are overlapping with other redacted values
    def redact_threshold(self):
        self.df_log.loc[:, 'Overlapping'] = 0
        
        # Loop through each sensitive column to check for overlapping sensitive information
        for sensitive_column in self.sensitive_columns:
            list_sensitive = self.df_log[self.df_log['RedactBinary'] == 1][sensitive_column].unique()
            print(list_sensitive)
            self.df_log.loc[self.df_log[sensitive_column].isin(list_sensitive), 'Overlapping'] += 1
        
        # Mark rows with maximum overlapping as 'Suppressed'
        mask = (self.df_log['Overlapping'] == self.df_log['Overlapping'].max()) & (self.df_log['RedactBinary'] == 0)

        self.df_log.loc[mask, 'Redact'] = 'Overlapping threshold secondary suppression'
        
        self.df_log.loc[mask, 'RedactBinary'] = 1
            
        # Return the modified dataframe
        return self.df_log

    # Method to redact values in the dataframe that are the sum of minimum threshold 
    def sum_redact(self):
        # Filter rows where the value in column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_less_than = self.df_log[(self.df_log[self.frequency] < self.minimum_threshold) & (self.df_log[self.frequency] != 0)]
        
        # Group the filtered dataframe by 'organization_columns' and sum the values in 'frequency'
        df_grouped_less_than = df_less_than.groupby(self.organization_columns)[self.frequency].sum().reset_index()
        
        df_grouped_less_than.rename(columns={self.frequency: "TotalSum"}, inplace=True)

        df_grouped_less_than = df_grouped_less_than[self.organization_columns + ['TotalSum']]
        
        # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
        self.df_log = self.df_log.merge(df_grouped_less_than, on=self.organization_columns, how='left')

        # Mark rows with maximum overlapping as 'Suppressed'
        mask = (self.df_log['TotalSum'] <= self.minimum_threshold) & (self.df_log['MinimumValue'] == self.df_log[self.frequency])
        
        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update the 'Redact' column with a specific message for rows where 'MinimumValue' is not null
        self.df_log.loc[mask, 'Redact'] = 'Sum of minimum threshold redact needed secondary suppression'
        
        # Return the updated dataframe
        return self.df_log

    # Method to redact values in the dataframe that are the only value in the group
    def one_count_redacted(self):
        # Filter rows where the value in the column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_redact_count = self.df_log[self.df_log['RedactBinary'] == 1]
        
        # Group the filtered dataframe by 'organization_columns' and count the size of each group
        df_grouped = df_redact_count.groupby(self.organization_columns).count().reset_index()
        
        df_grouped.rename(columns={self.frequency: "counts"}, inplace=True)

        df_grouped = df_grouped[self.organization_columns + ['counts']]
        
        # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
        self.df_log = self.df_log.merge(df_grouped, on=self.organization_columns, how='left')

        mask = (self.df_log['counts'] == 1) & (self.df_log['MinimumValue'] == self.df_log[self.frequency])
        
        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update the 'Redact' column with a specific message for rows where 'MinimumValue' is not null
        self.df_log.loc[mask, 'Redact'] = 'Count minimum threshold needed secondary suppression'
        
        # Return the updated dataframe
        return self.df_log
    
    def one_redact_zero (self):        
        # Filtering the DataFrame based on School Year and SuppressionID        
        df_filtered = self.df_log[self.df_log['RedactBinary'] == 1]    
        
        # Grouping by Organization and counting StudentCount, then filtering groups with a single record  
        df_grouped_count = df_filtered.groupby(self.organization_columns).count().reset_index()  
        
        df_grouped_count.rename(columns={self.frequency: "ZeroSuppressedCounts"}, inplace=True)

        df_filtered_grouped_count = df_grouped_count[df_grouped_count['ZeroSuppressedCounts'] == 1]

        df_filtered_grouped_count = df_filtered_grouped_count[self.organization_columns]
        
        df_filtered_grouped_count['Zero'] = 1    
        
        # Merge the original DataFrame with the filtered grouped DataFrame based on DimSeaID        
        self.df_log = self.df_log.merge(df_filtered_grouped_count, on=self.organization_columns, how='left') 

        self.df_log.loc[(self.df[self.frequency] == 0) & (self.df_log['Zero'] == 1), 'RedactBinary'] = 1
        
        self.df_log.loc[(self.df[self.frequency] == 0) & (self.df_log['Zero'] == 1), 'Redact'] = 'Redact zero needed for secondary suppression'
        
        return self.df_log
        
    def organization_group_redaction(self):
        return self.df
    # New method to call the specified functions
    def apply_anonymization(self):
        # Call less_than_threshold_not_zero
        self.less_than_threshold_not_zero()

        # Call one_count_redacted
        self.one_count_redacted()

        # Call sum_redact
        self.sum_redact()

        # Call one_redact_zero
        self.one_redact_zero()

        # Return the updated dataframe
        return self.df