import pandas as pd

class DataAnonymizer:
    # Initialize the class with a dataframe (df) and optionally, a list of sensitive columns
    def __init__(self, df, organization_columns=None, sensitive_columns=None, frequency=None, redact_column=None, minimum_threshold=10):
        df['Original'] = 1
        # Create a copy of the input dataframe and store it as an instance variable
        self.df = df.copy()
        self.organization_columns = list(organization_columns) if isinstance(organization_columns, (list, tuple)) else [organization_columns]
        self.sensitive_columns = list(sensitive_columns) if isinstance(sensitive_columns, (list, tuple)) else [sensitive_columns]

        # Rename the user supplied redact column to "UserRedact"
        self.df.rename({self.redact_column[0]: "UserRedact"}, axis = 1, inplace=True)
        self.redact_column = ['UserRedact']
        
        self.frequency = frequency
        self.minimum_threshold = minimum_threshold
        

    def create_log(self):
        df_dataframes = pd.DataFrame()
        grouping_value = 0
        for organization_column in self.organization_columns:
            for sensitive_column in self.sensitive_columns:
                df_grouped = self.df.groupby([organization_column, sensitive_column])[self.frequency].sum().reset_index()
                df_grouped['Grouping'] = grouping_value
                grouping_value += 1
                df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
                df_grouped_min = df_not_redacted.groupby([organization_column])[self.frequency].min().reset_index()
                df_grouped_min.rename(columns={self.frequency: "MinimumValue"}, inplace=True)
                df_grouped = df_grouped.merge(df_grouped_min, on=[organization_column], how='left')
                df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
        for organization_column in self.organization_columns:
            df_grouped = self.df.groupby([organization_column])[self.frequency].sum().reset_index()
            df_grouped['Grouping'] = grouping_value
            grouping_value += 1
            df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
            df_grouped_min = df_not_redacted.groupby('Grouping')[self.frequency].min().reset_index()
            df_grouped_min.rename(columns={self.frequency: "MinimumValue"}, inplace=True)
            df_grouped = df_grouped.merge(df_grouped_min, on=['Grouping'], how='left')
            df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
        for sensitive_column in self.sensitive_columns:
            df_grouped = self.df.groupby([sensitive_column])[self.frequency].sum().reset_index()
            df_grouped['Grouping'] = grouping_value
            grouping_value += 1
            df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
            df_grouped_min = df_not_redacted.groupby(['Grouping'])[self.frequency].min().reset_index()
            df_grouped_min.rename(columns={self.frequency: "MinimumValue"}, inplace=True)
            df_grouped = df_grouped.merge(df_grouped_min, on=['Grouping'], how='left')
            df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
        df_log_original =  pd.DataFrame(self.df[self.organization_columns + self.sensitive_columns + [self.frequency]])
        df_log_original['Grouping'] = grouping_value
        df_log = pd.concat([df_dataframes, df_log_original], ignore_index=True)
        df_log = df_log.drop_duplicates().reset_index(drop=True)
        df_log['RedactBinary'] = 0
        df_log['Redact'] = 'Not Redacted'
        
        if self.organization_columns:
            df_not_redacted = df_log[(df_log[self.frequency] > self.minimum_threshold)]
            df_grouped_min = df_not_redacted.groupby(self.organization_columns, dropna=False)[self.frequency].min().reset_index()
            df_grouped_min.rename(columns={self.frequency: "MinimumValueTotal"}, inplace=True)
            df_log = df_log.merge(df_grouped_min, on=self.organization_columns, how='left')
            df_log.loc[df_log['MinimumValue'].isnull(), 'MinimumValue'] = df_log['MinimumValueTotal']
            df_log.drop('MinimumValueTotal', axis=1, inplace=True)
        self.df_log = df_log.copy()
        
        df_log = self.df[self.organization_columns + self.sensitive_columns + [self.frequency]]

        if redact_column is None:
            df_log[self.redact_column[0]] = 0
        else:
            df_log[self.redact_column] = self.df[self.redact_column]

        print(df_log.head())
        
        for organization_column in self.organization_columns:
            for sensitive_column in self.sensitive_columns:
                df_temp = self.df.groupby([organization_column] + [sensitive_column])[self.frequency].sum().reset_index()
                df_log = pd.concat([df_temp, df_log])

        df_log.drop_duplicates(inplace=True)
        
        self.df_log = df_log[self.organization_columns + self.sensitive_columns + [self.frequency] + self.redact_column]
        
        self.df_log = self.df_log.reset_index(drop=True)
        
        self.df_log.loc[:, 'RedactBinary'] = 0

        self.df_log.loc[:, 'Redact'] = 'Not Redacted'

        if len(self.organization_columns) != 0:
            # Filter rows where the value in the column specified by 'frequency' is greater than or equal to 'minimum_threshold'
            df_minimum_threshold = self.df_log[self.df_log[self.frequency] >= self.minimum_threshold]
            display(df_minimum_threshold)
            # Group the filtered dataframe by 'organization_columns' and get the minimum value in the 'frequency' column
            df_grouped_min = df_minimum_threshold.groupby(self.organization_columns)[self.frequency].min().reset_index()
            df_grouped_min.rename(columns = {self.frequency: "MinimumValue"}, inplace=True)
            
            # Merge the filtered and grouped dataframes based on 'organization_columns'
            self.df_log = self.df_log.merge(df_grouped_min, on=organization_columns, how='left')

            # drop NA rows based on merge
            self.df_log.dropna(axis= 0, how = "any", subset= self.organization_columns + self.sensitive_columns, inplace=True)
            self.df_log['MinimumValue'] = self.df_log['MinimumValue'].fillna(0)

        return self.df_log
        
    # Method to redact values in the dataframe that are less than a minimum threshold but not zero
    def less_than_threshold_not_zero(self):
        # Create a boolean mask that identifies rows where the column specified by 'frequency'
        # has values less than 'minimum_threshold' and not equal to zero
        mask = (self.df_log[self.frequency] < self.minimum_threshold) & (self.df_log[self.frequency] != 0)

        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update a new column named 'Redact' with a message for the rows that meet the condition specified by the mask
        self.df_log.loc[mask, 'Redact'] = 'Primary Suppression'
        #self.df_log.loc[mask, 'Redact'] = f'Less Than {self.minimum_threshold} and not zero'
        
        # Return the updated dataframe
        return self.df_log


    # Method to redact values in the dataframe that are overlapping with other redacted values
    def redact_threshold(self):
        self.df_log.loc[:, 'Overlapping'] = 0
        
        # Loop through each sensitive column to check for overlapping sensitive information
        for sensitive_column in self.sensitive_columns:
            list_sensitive = self.df_log[self.df_log['RedactBinary'] == 1][sensitive_column].unique()

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
        df_redact_count = self.df_log[self.df_log['RedactBinary'] == 1]
        
        # Group the filtered dataframe by 'organization_columns' and sum the values in 'frequency'
        df_grouped_less_than = df_redact_count.groupby(['Grouping'] + self.organization_columns, dropna=False)[self.frequency].sum().reset_index()
        
        df_grouped_less_than.rename(columns={self.frequency: "TotalSum"}, inplace=True)

        df_grouped_less_than = df_grouped_less_than[['Grouping'] + self.organization_columns + ['TotalSum']]
        
        # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
        self.df_log = self.df_log.merge(df_grouped_less_than, on=['Grouping'] + self.organization_columns, how='left')
        
        # Mark rows with maximum overlapping as 'Suppressed'
        mask = (self.df_log['TotalSum'] <= self.minimum_threshold) & (self.df_log['MinimumValue'] == self.df_log[self.frequency])
        
        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update the 'Redact' column with a specific message for rows where 'MinimumValue' is not null
        self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
        #self.df_log.loc[mask, 'Redact'] = 'Sum of minimum threshold redact needed secondary suppression'
        
        # Return the updated dataframe
        return self.df_log

    # Method to redact values in the dataframe that are the only value in the group
    def one_count_redacted(self):
        # Filter rows where the value in the column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_redact_count = self.df_log[self.df_log['RedactBinary'] == 1]
        
        # Group the filtered dataframe by 'organization_columns' and count the size of each group
        df_grouped = df_redact_count.groupby(['Grouping'] + self.organization_columns, dropna=False).count().reset_index()
        
        df_grouped.rename(columns={self.frequency: "counts"}, inplace=True)

        df_grouped = df_grouped[['Grouping'] + self.organization_columns + ['counts']]
        
        # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
        self.df_log = self.df_log.merge(df_grouped, on=['Grouping'] + self.organization_columns, how='left')

        mask = (self.df_log['counts'] == 1) & (self.df_log['MinimumValue'] == self.df_log[self.frequency])
        
        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update the 'Redact' column with a specific message for rows where 'MinimumValue' is not null

        self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
        #self.df_log.loc[mask, 'Redact'] = 'Count minimum threshold needed secondary suppression'
        
        # Return the updated dataframe
        return self.df_log
    
    def one_redact_zero (self):        
        # Filtering the DataFrame based on School Year and SuppressionID        
        df_filtered = self.df_log[self.df_log['RedactBinary'] == 1]    
        
        # Grouping by Organization and counting StudentCount, then filtering groups with a single record  
        df_grouped_count = df_filtered.groupby(['Grouping'] + self.organization_columns, dropna=False).count().reset_index()  
        
        df_grouped_count.rename(columns={self.frequency: "ZeroSuppressedCounts"}, inplace=True)
        
        df_filtered_grouped_count = df_grouped_count[df_grouped_count['ZeroSuppressedCounts'] == 1]

        df_filtered_grouped_count = df_filtered_grouped_count[['Grouping'] + self.organization_columns]
        
        df_filtered_grouped_count['Zero'] = 1    

        # Merge the original DataFrame with the filtered grouped DataFrame based on DimSeaID        
        self.df_log = self.df_log.merge(df_filtered_grouped_count, on=['Grouping'] + self.organization_columns, how='left') 

        self.df_log.loc[(self.df[self.frequency] == 0) & (self.df_log['Zero'] == 1), 'RedactBinary'] = 1
        
        self.df_log.loc[(self.df[self.frequency] == 0) & (self.df_log['Zero'] == 1), 'Redact'] = 'Redact zero needed for secondary suppression'
        
        return self.df_log

    def apply_log(self):
        original_columns = list(self.df_log)
        df_original =  self.df_log.merge(self.df, on = self.organization_columns + self.sensitive_columns +  [self.frequency], how='inner')
        df_all =  self.df_log.merge(self.df, on = self.organization_columns + self.sensitive_columns +  [self.frequency], how='left')
        df_not_original = df_all[df_all['Original'].isnull()]
        df_redacted = df_not_original[df_not_original['RedactBinary'] == 1]
        df_totals_redacted = df_redacted[original_columns]
        df_totals_redacted['RedactTotals'] = 1
        for organization in self.organization_columns:
            df_organization = df_totals_redacted[~df_totals_redacted[organization].isnull()]
            if not df_organization.empty:
                for sensitive in self.sensitive_columns:
                    df_sensitive = df_totals_redacted[~df_totals_redacted[sensitive].isnull()]
                    if not df_sensitive.empty:
                        df_temp = df_totals_redacted[[organization] + [sensitive] + ['RedactTotals']]
                        df_original = df_original.merge(df_temp, on = [organization] + [sensitive], how='left')
                        df_original.loc[df_original['RedactTotals'] == 1, 'Redact'] = 'Secondary Suppression'
                        df_original.loc[df_original['RedactTotals'] == 1, 'RedactBinary'] = 1
        return df_original
    # New method to call the specified functions

    def redact_user_requested_records(self):
        self.df_log.loc[(self.df_log["UserRedact"] == 1), 'RedactBinary'] = 1

        self.df_log.loc[(self.df_log["UserRedact"] == 1), 'Redact'] = 'User-requested redaction'
        return self.df_log
    def apply_anonymization(self):
        # Call less_than_threshold_not_zero
        self.less_than_threshold_not_zero()

        # Call redact_user_requested_records
        self.redact_user_requested_records()

        # Call one_count_redacted
        self.one_count_redacted()

        # Call sum_redact
        self.sum_redact()

        # Call one_redact_zero
        self.one_redact_zero()

        # Return the updated dataframe
        return self.df_log