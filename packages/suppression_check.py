import pandas as pd
from itertools import combinations

class DataAnonymizer:
    # Initialize the class with a dataframe (df) and optionally, a list of sensitive columns
    def __init__(self, df, parent_organization=None, child_organization=None, sensitive_columns=None, frequency=None, redact_column=None, minimum_threshold=10):
        if (child_organization is None) & (parent_organization is None):
            organization_columns = None
        elif (parent_organization is not None) & (child_organization is None):
            organization_columns = [parent_organization]
        elif (parent_organization is None) & (child_organization is not None):
            organization_columns = [child_organization]
        else:
            organization_columns = [parent_organization, child_organization]
        print(organization_columns)
        
        df['Original'] = 1
        # Create a copy of the input dataframe and store it as an instance variable
        self.df = df.copy()
        self.organization_columns = list(organization_columns) if isinstance(organization_columns, (list, tuple)) else [organization_columns]
        self.sensitive_columns = list(sensitive_columns) if isinstance(sensitive_columns, (list, tuple)) else [sensitive_columns]
        self.sensitive_combinations = sorted([combo for i in range(1, len(self.sensitive_columns) + 1) for combo in combinations(self.sensitive_columns, i)], key=len, reverse=True)
        
        # Rename the user supplied redact column to "UserRedact"
        if redact_column is not None:
            self.df.rename({redact_column: "UserRedact"}, axis = 1, inplace=True)
            redact_column = 'UserRedact'
        else:
            self.df['UserRedact'] = 0
        
        # Check the type of organization_columns and store it as an instance variable
        if redact_column is None:
            print('Redact is empty or not included')  # Print a message if organization_columns is None
            self.redact_column = ['UserRedact']  # Initialize an empty list
        elif isinstance(redact_column, str):
            self.redact_column = [redact_column]  # Convert a single string to a list with one item
        else:
            self.redact_column = redact_column  # Store the provided list
        
        self.frequency = frequency
        self.minimum_threshold = minimum_threshold
        self.parent_organization = parent_organization
        self.child_organization = child_organization
        

    def create_log(self):
        print('Creating log!')
        df_dataframes = pd.DataFrame()
        grouping_value = 0
        if self.parent_organization is not None:
            for sensitive_combination in self.sensitive_combinations:
                df_grouped = self.df.groupby([self.parent_organization] + list(sensitive_combination)  + ['UserRedact'])[self.frequency].sum().reset_index()
                df_grouped['Grouping'] = grouping_value
                grouping_value += 1
                df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
                df_grouped_min = df_not_redacted.groupby([self.parent_organization]  + ['UserRedact'])[self.frequency].min().reset_index()
                df_grouped_min.rename(columns={self.frequency: "MinimumValue"}, inplace=True)
                df_grouped = df_grouped.merge(df_grouped_min, on=[self.parent_organization] + ['UserRedact'], how='left')
                df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
        if self.parent_organization is not None:
            df_grouped = self.df.groupby([self.parent_organization]  + ['UserRedact'])[self.frequency].sum().reset_index()
            df_grouped['Grouping'] = grouping_value
            grouping_value += 1
            df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
            df_grouped_min = df_not_redacted.groupby(['Grouping']  + ['UserRedact'])[self.frequency].min().reset_index()
            df_grouped_min.rename(columns={self.frequency: "MinimumValue"}, inplace=True)
            df_grouped = df_grouped.merge(df_grouped_min, on=['Grouping']  + ['UserRedact'], how='left')
            df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
        for sensitive_combination in self.sensitive_combinations:
            list_combination = list(sensitive_combination)
            df_grouped = self.df.groupby(list_combination + ['UserRedact'])[self.frequency].sum().reset_index()
            df_grouped['Grouping'] = grouping_value
            grouping_value += 1
            df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
            df_not_redacted = df_dataframes[df_dataframes[self.frequency] > self.minimum_threshold]
            if list_combination != self.sensitive_columns:
                df_grouped_min = df_not_redacted.groupby(['Grouping'] + list_combination + ['UserRedact'])[self.frequency].min().reset_index()
                string_combination = ''.join(list_combination)
                df_grouped_min.rename(columns={self.frequency: "MinimumValue" + string_combination}, inplace=True)
                df_dataframes = df_dataframes.merge(df_grouped_min, on= ['Grouping'] + list_combination + ['UserRedact'], how='left')
        if self.organization_columns[0] is not None:
            df_log_original =  pd.DataFrame(self.df[self.organization_columns + self.sensitive_columns +[self.frequency] + self.redact_column])
        else:
            df_log_original =  pd.DataFrame(self.df[self.sensitive_columns +[self.frequency] + self.redact_column])
        df_log_original['Grouping'] = grouping_value
        df_log = df_dataframes
        df_log = df_log.drop_duplicates().reset_index(drop=True)
        df_log['RedactBinary'] = 0
        df_log['Redact'] = 'Not Redacted'
        
        if self.organization_columns[0] is not None:
            df_not_redacted = df_log[(df_log[self.frequency] > self.minimum_threshold)]
            df_grouped_min = df_not_redacted.groupby(self.organization_columns, dropna=False)[self.frequency].min().reset_index()
            df_grouped_min.rename(columns={self.frequency: "MinimumValueTotal"}, inplace=True)
            df_log = df_log.merge(df_grouped_min, on=self.organization_columns, how='left')
            df_log.loc[df_log['MinimumValue'].isnull(), 'MinimumValue'] = df_log['MinimumValueTotal']
            df_log.drop('MinimumValueTotal', axis=1, inplace=True)
        self.df_log = df_log.copy()
    
        
        if self.organization_columns[0] is not None:
            df_log =  pd.DataFrame(self.df_log[self.organization_columns + self.sensitive_columns +[self.frequency] + self.redact_column])
        else:
            df_log =  pd.DataFrame(self.df_log[self.sensitive_columns +[self.frequency] + self.redact_column])

        df_log.drop_duplicates(inplace=True)
        
        self.df_log = self.df_log.reset_index(drop=True)
        
        self.df_log.loc[:, 'RedactBinary'] = 0

        self.df_log.loc[:, 'Redact'] = 'Not Redacted'

        print('Log created!')
        return self.df_log

    def redact_user_requested_records(self):
        print('Seeing if user redact column exist.')
        self.df_log.loc[(self.df_log["UserRedact"] == 1), 'RedactBinary'] = 1

        self.df_log.loc[(self.df_log["UserRedact"] == 1), 'Redact'] = 'User-requested redaction'
        self.df_log = self.df_log.drop('UserRedact', axis=1)
        print('Completed review if user redact column exists.')
        return self.df_log
    # Method to redact values in the dataframe that are less than a minimum threshold but not zero
    def less_than_threshold_not_zero(self):
        # Create a boolean mask that identifies rows where the column specified by 'frequency'
        # has values less than 'minimum_threshold' and not equal to zero
        print('Redacting values are less than threshold and not zero.')
        mask = (self.df_log[self.frequency] <= self.minimum_threshold) & (self.df_log[self.frequency] != 0)

        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update a new column named 'Redact' with a message for the rows that meet the condition specified by the mask
        self.df_log.loc[mask, 'Redact'] = 'Primary Suppression'
        #self.df_log.loc[mask, 'Redact'] = f'Less Than {self.minimum_threshold} and not zero'

        print('Completed redacting values less than threshold and not zero.')
        # Return the updated dataframe
        return self.df_log

    # Method to redact values in the dataframe that are overlapping with other redacted values
    def redact_threshold(self):
        print('Begin Redacting based on the overlapping relationships between organizations and subgroups.')
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
        print('Completed redaction based on the overlapping relationships between organizations and subgroups.')
        return self.df_log

    # # Method to redact values in the dataframe that are the sum of minimum threshold 
    # def sum_redact(self):
    #     # Filter rows where the value in column specified by 'frequency' is less than 'minimum_threshold' but not zero
    #     df_redact_less = self.df_log[self.df_log['RedactBinary'] == 1]
        
    #     # Group the filtered dataframe by 'organization_columns' and sum the values in 'frequency'
    #     if self.organization_columns[0] is not None:
    #         df_grouped_less_than = df_redact_count.groupby(['Grouping'] + self.organization_columns, dropna=False)[self.frequency].sum().reset_index()
            
    #         df_grouped_less_than.rename(columns={self.frequency: "TotalSum"}, inplace=True)

    #         df_grouped_less_than = df_grouped_less_than[['Grouping'] + self.organization_columns + ['TotalSum']]
        
    #     # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
    #         self.df_log = self.df_log.merge(df_grouped_less_than, on=['Grouping'] + self.organization_columns, how='left')
        
    #     else:
    #         df_grouped_less_than = df_redact_count.groupby(['Grouping'], dropna=False)[self.frequency].sum().reset_index()
            
    #         df_grouped_less_than.rename(columns={self.frequency: "TotalSum"}, inplace=True)

    #         df_grouped_less_than = df_grouped_less_than[['Grouping'] + ['TotalSum']]
        
    #     # Merge the original dataframe with the result dataframe based on 'merged_columns' and 'greater_than_columns'
    #         self.df_log = self.df_log.merge(df_grouped_less_than, on=['Grouping'], how='left')
        
    #     # Mark rows with maximum overlapping as 'Suppressed'
    #     mask = (self.df_log['TotalSum'] <= self.minimum_threshold) & (self.df_log['MinimumValue'] == self.df_log[self.frequency])
        
    #     self.df_log.loc[mask, 'RedactBinary'] = 1
        
    #     # Update the 'Redact' column with a specific message for rows where 'MinimumValue' is not null
    #     self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
    #     #self.df_log.loc[mask, 'Redact'] = 'Sum of minimum threshold redact needed secondary suppression'
        
    #     # Return the updated dataframe
    #     return self.df_log

    # Method to redact values in the dataframe that are the only value in the group
    def one_count_redacted(self):
        print('Start review is secondary disclosure avoidance is needed and begin application.') 
        # Filter rows where the value in the column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_redact_count = self.df_log[self.df_log['RedactBinary'] == 1]
        df_redact_count['Redacted'] = 1
        if self.organization_columns[0] is not None:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_merge = df_redact_count[['Grouping'] + self.organization_columns + list_combination + ['Redacted']]
                    df_redact_merge.drop_duplicates(inplace=True)
                    df_primary = self.df_log.merge(df_redact_merge, on=['Grouping'] + self.organization_columns + list_combination, how='left')
                    mask = (df_primary['Redacted'] == 1) & (df_primary["MinimumValue" + string_combination] == df_primary[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'

        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_merge = df_redact_count[['Grouping'] + list_combination + ['Redacted']]
                    df_redact_merge.drop_duplicates(inplace=True)
                    df_primary = self.df_log.merge(df_redact_merge, on=['Grouping'] + list_combination, how='left')
                    mask = (df_primary['Redacted'] == 1) & (df_primary["MinimumValue" + string_combination] == df_primary[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'

        print('Completion of initial step with secondary disclosure avoidance!')
        # Return the updated dataframe
        return self.df_log
    
    def one_redact_zero(self):    
        print('Start review of next step of secondary disclosure avoidance where review of one count of redacted category in a group.')
        df_log_na = self.df_log.copy()

        temp_value = 'NaFill'

        for sensitive_column in self.sensitive_columns:
            df_log_na[sensitive_column].fillna(temp_value, inplace=True)  
        
        # Grouping by Organization and counting StudentCount, then filtering groups with a single record 
        if self.organization_columns[0] is not None: 
            df_grouped_count = df_filtered.groupby(['Grouping'] + self.organization_columns + self.sensitive_columns, dropna=False).count().reset_index()  
            
            df_grouped_count.rename(columns={self.frequency: "ZeroSuppressedCounts"}, inplace=True)
            
            df_filtered_grouped_count = df_grouped_count[df_grouped_count['ZeroSuppressedCounts'] == 1]

            df_filtered_grouped_count = df_filtered_grouped_count[['Grouping'] + self.organization_columns]
            
            df_filtered_grouped_count['Zero'] = 1    

            # Merge the original DataFrame with the filtered grouped DataFrame based on DimSeaID        
            self.df_log = self.df_log.merge(df_filtered_grouped_count, on=['Grouping'] + self.organization_columns, how='left') 
        
        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_less = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_redact_less['Redacted'] = 1
                    df_count = df_redact_less.groupby(['Grouping'] + list_combination)['Redacted'].count().reset_index()
                    df_one_redacted = df_count[df_count['Redacted'] == 1]
                    if not df_one_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum = df_not_redacted.groupby(['Grouping'] + list_combination, dropna=False)['Counts'].min().reset_index()
                        df_minimum.rename(columns={'Counts':'LastMiniumValue'}, inplace=True)
                        df_minimum_redacted = df_one_redacted.merge(df_minimum, on = ['Grouping'] + list_combination)
                        df_minimum_one = df_log_na.merge(df_minimum_redacted, on = ['Grouping'] + list_combination, how='left')
                        mask = (df_minimum_one['Counts'] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[mask, 'RedactBinary'] = 1
                        df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'

        self.df_log.loc[df_log_na['RedactBinary'] == 1, 'RedactBinary'] = 1
        self.df_log.loc[df_log_na['Redact'] == 'Secondary Suppression', 'Redact'] = 'Secondary Suppression'

        print('Complete review of secondary disclosure avoidance where review of one count of redacted category in a group.')
        
        return self.df_log
        
    def cross_suppression(self):
        print('Begin analysis if secondary redaction on aggregate levels need to be applied to original dataframe.')
        if (self.child_organization is not None) & (self.parent_organization is not None):
            df_parent_redact = self.df_log[self.df_log[self.child_organization].isnull()]
            redact_parent_name = 'RedactParentBinary'
            df_parent_redact.rename(columns = {'RedactBinary':redact_parent_name}, inplace=True)
            df_parent_redact = df_parent_redact[[self.parent_organization] + self.sensitive_columns + [redact_parent_name]]
            df_parent_redact.drop_duplicates(inplace=True)
            self.df_log = self.df_log.merge(df_parent_redact, on = [self.parent_organization] + self.sensitive_columns, how='left')
            self.df_log.loc[(self.df_log[redact_parent_name] == 1), 'RedactBinary'] = 1

        if (self.child_organization is not None) & (self.parent_organization is not None):
            df_sensitive = self.df_log[self.df_log[self.child_organization].isnull() & self.df_log[self.parent_organization].isnull()]
        elif (self.child_organization is None) & (self.parent_organization is not None):
            df_sensitive = self.df_log[self.df_log[self.parent_organization].isnull()]
        elif (self.child_organization is not None) & (self.parent_organization is None):
            df_sensitive = self.df_log[self.df_log[self.child_organization].isnull()]
        else:
            df_sensitive = self.df_log

        redact_sensitive_name = 'RedactSensitiveBinary'
        df_sensitive.rename(columns = {'RedactBinary':redact_sensitive_name}, inplace=True)
        df_sensitive = df_sensitive[self.sensitive_columns + [redact_sensitive_name]]
        df_sensitive.drop_duplicates(inplace=True)
        if (self.child_organization is not None) & (self.parent_organization is not None):
            self.df_log = self.df_log.merge(df_sensitive, on =self.sensitive_columns, how='left', suffixes = ('', '_y'))
        else:
            self.df_log = self.df_log.merge(df_sensitive, on =self.sensitive_columns + [redact_sensitive_name], how='left', suffixes = ('', '_y'))
        self.df_log.drop(self.df_log.filter(regex='_y$').columns, axis = 1, inplace = True)
        self.df_log.loc[(self.df_log[redact_sensitive_name] == 1), 'RedactBinary'] = 1
        
        self.df_log.loc[(self.df_log['RedactBinary'] != 1), 'RedactBinary'] = 0
        print('Completion of analysis if secondary redaction on aggregate levels need to be applied to original dataframe.')
        return self.df_log
    
    def apply_log(self):
        print('Start applying log to given dataframe.')
        if self.organization_columns[0] is not None:
            df_redacted =  self.df.merge(self.df_log, on = self.organization_columns + self.sensitive_columns +  [self.frequency], how='inner')
            columns = self.organization_columns + self.sensitive_columns +  [self.frequency] + ['RedactBinary', 'Redact']
        else:
            df_redacted =  self.df.merge(self.df_log, on = self.sensitive_columns +  [self.frequency], how='inner')
            columns = self.sensitive_columns +  [self.frequency] + ['RedactBinary', 'Redact']
        df_redacted = df_redacted[columns]
        # df_redacted = df_redacted.drop_duplicates().reset_index(drop=True) # this helps remove the duplicate issue, but the duplicates should not be there
        self.df_redacted = df_redacted

        print('Finished applying log to given dataframe!')
        
        return self.df_redacted
    # New method to call the specified functions
    def get_log(self):
        print('Pulling log from class.')
        print('Log returned from class!')
        return self.df_log

    def apply_anonymization(self):
        
        self.create_log()

        # Call redact_user_requested_records
        self.redact_user_requested_records()
        
        # Call less_than_threshold_not_zero
        self.less_than_threshold_not_zero()
        
        # Call one_count_redacted
        self.one_count_redacted()

        # # Call sum_redact
        # self.sum_redact()

        # Call one_redact_zero
        self.one_redact_zero()

        # Call cross_suppression
        self.cross_suppression()

        #Call apply_log
        self.apply_log()
        
        # Return the updated dataframe
        return self.df_redacted
        