
import pandas as pd

# Define a class called DataAnonymizer
class DataAnonymizer:
    
    # Initialize the class with a dataframe (df) and optionally, a list of sensitive columns
    def __init__(self, df, sensitive_columns=None):
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
        
        
    # Method to redact values in the dataframe that are less than a minimum threshold but not zero
    def less_than_threshold_not_zero(self, frequency, minimum_threshold=10):
        # Create a boolean mask that identifies rows where the column specified by 'frequency'
        # has values less than 'minimum_threshold' and not equal to zero
        mask = (self.df[frequency] < minimum_threshold) & (self.df[frequency] != 0)

        self.df['RedactBinary'] = 0

        self.df.loc[mask, 'RedactBinary'] = 1
        
        # Update a new column named 'Redact' with a message for the rows that meet the condition specified by the mask
        self.df.loc[mask, 'Redact'] = f'Less Than {minimum_threshold} and not zero'
        
        # Return the updated dataframe
        return self.df

    # Method to redact values in the dataframe that are overlapping with other redacted values
    def redact_threshold(self, frequency, organization_columns=None, minimum_threshold=10):
        # Call another method to handle rows where the frequency is less than 'minimum_threshold' but not zero
        self.less_than_threshold_not_zero(frequency, minimum_threshold)
        
        # Initialize 'Overlapping' column to zero
        self.df['Overlapping'] = 0
        
        # Loop through each sensitive column to check for overlapping sensitive information
        for sensitive_column in self.sensitive_columns:
            list_sensitive = self.df[~self.df['Redact'].isnull()][sensitive_column].unique()
            self.df.loc[self.df[sensitive_column].isin(list_sensitive), 'Overlapping'] += 1
        
        # Mark rows with maximum overlapping as 'Suppressed'
        self.df.loc[self.df['Overlapping'] == self.df['Overlapping'].max(), 'Suppressed'] = 1
        
        # Update 'Redact' column for rows where 'Overlapping' is maximum but 'Redact' is null
        mask = ((self.df['Overlapping'] == self.df['Overlapping'].max()) & (self.df['Redact'].isnull()))
        self.df.loc[mask, 'Redact'] = 'Overlapping threshold secondary suppression'
        
        # Finalize the columns to be retained in the dataframe
        if organization_columns is None:
            print('No organization columns')
            self.df = self.df[self.sensitive_columns + [frequency] + ['Redact']]
        elif isinstance(organization_columns, str):
            organization_columns = [organization_columns]
            self.df = self.df[organization_columns + self.sensitive_columns + [frequency] + ['Redact']]
        else:
            self.df = self.df[organization_columns + self.sensitive_columns + [frequency] + ['Redact']]
            
        # Return the modified dataframe
        return self.df

    # Method to redact values in the dataframe that are the sum of minimum threshold 
    def sum_redact(self, frequency, organization_columns=None, minimum_threshold=10):
        # Filter rows where the value in column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_less_than = self.df[(self.df[frequency] < minimum_threshold) & (self.df[frequency] != 0)]
        
        # Group the filtered dataframe by 'organization_columns' and sum the values in 'frequency'
        df_grouped_less_than = df_less_than.groupby(organization_columns)[frequency].sum().reset_index(name='TotalCount')
        
        # Further filter the grouped dataframe to retain only rows where 'TotalCount' is less or equal to 'minimum_threshold'
        df_filtered_group = df_grouped_less_than[df_grouped_less_than['TotalCount'] <= minimum_threshold]
        
        # Select only the 'organization_columns' from the filtered grouped dataframe
        df_filtered = df_filtered_group[organization_columns]
        
        # Filter rows where the value in the column specified by 'frequency' is greater than or equal to 'minimum_threshold'
        df_greater_than = self.df[self.df[frequency] >= minimum_threshold]
        
        # Group the filtered dataframe by 'organization_columns' and get the minimum value in the 'frequency' column
        df_grouped_greater_than = df_greater_than.groupby(organization_columns)[frequency].min().reset_index(name='MinimumValue')
        
        # Merge the filtered and grouped dataframes based on 'organization_columns'
        df_result = pd.merge(df_filtered, df_grouped_greater_than, on=organization_columns, how='inner')
        
        # List columns for the dataframe with values greater than or equal to 'minimum_threshold'
        greater_than_columns = organization_columns + ['MinimumValue']
        
        # List columns for merging the original dataframe with the result dataframe
        merged_columns = organization_columns + [frequency]
        
        # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
        self.df = pd.merge(self.df, df_result, left_on=merged_columns, right_on=greater_than_columns, how='left')
        
        self.df.loc[~self.df['MinimumValue'].isnull(), 'RedactBinary'] = 1
        
        # Update the 'Redact' column with a specific message for rows where 'MinimumValue' is not null
        self.df.loc[self.df['RedactBinary'] == 1, 'Redact'] = 'Sum of minimum threshold redact needed secondary suppression'
        
        # Retain only the necessary columns in the final dataframe
        self.df = self.df[organization_columns + self.sensitive_columns + [frequency] + ['Redact', 'RedactBinary']]
        
        # Return the updated dataframe
        return self.df

    # Method to redact values in the dataframe that are the only value in the group
    def one_count_redacted(self, frequency, organization_columns=None, minimum_threshold=10):
        # Filter rows where the value in the column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_less_than_eleven_count = self.df[(self.df[frequency] < minimum_threshold) & (self.df[frequency] != 0)]
        
        # Group the filtered dataframe by 'organization_columns' and count the size of each group
        df_grouped = df_less_than_eleven_count.groupby(organization_columns).size().reset_index(name='counts')
        
        # Further filter the grouped dataframe to retain only rows where 'counts' equals 1
        df_filtered_grouped = df_grouped[df_grouped['counts'] == 1]
        
        # Select only the 'organization_columns' from the filtered grouped dataframe
        df_filtered = df_filtered_grouped[organization_columns]
        
        # Filter rows where the value in the column specified by 'frequency' is greater than or equal to 'minimum_threshold'
        df_minimum_threshold = self.df[self.df[frequency] >= minimum_threshold]
        
        # Group the filtered dataframe by 'organization_columns' and get the minimum value in the 'frequency' column
        df_grouped_min = df_minimum_threshold.groupby(organization_columns)[frequency].min().reset_index(name='MinimumValue')
        
        # Merge the filtered and grouped dataframes based on 'organization_columns'
        df_result = pd.merge(df_filtered, df_grouped_min, on=organization_columns, how='inner')
        
        # List columns for the dataframe with values greater than or equal to 'minimum_threshold'
        greater_than_columns = organization_columns + ['MinimumValue']
        
        # List columns for merging the original dataframe with the result dataframe
        merged_columns = organization_columns + [frequency]
        
        # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
        self.df = pd.merge(self.df, df_result, left_on=merged_columns, right_on=greater_than_columns, how='left')

        self.df.loc[~self.df['MinimumValue'].isnull(), 'RedactBinary'] = 1
        
        # Update the 'Redact' column with a specific message for rows where 'MinimumValue' is not null
        self.df.loc[self.df['RedactBinary'] == 1, 'Redact'] = 'Count minimum threshold needed secondary suppression'
        
        # Retain only the necessary columns in the final dataframe
        self.df = self.df[organization_columns + self.sensitive_columns + [frequency] + ['Redact', 'RedactBinary']]
        
        # Return the updated dataframe
        return self.df
    def organization_group_redaction(self, frequency, organization_columns=None, minimum_threshold=10):
        return self.df


