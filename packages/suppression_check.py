import pandas as pd
import logging
from itertools import combinations

# Configure logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) #Sets lowest level logger will handle. Debug level messages will be ignored with this setting. 
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s') 
handler = logging.StreamHandler() #Sends logs to console by default
handler.setFormatter(formatter) 
logger.addHandler(handler)

class DataAnonymizer:
    # Initialize the class with a dataframe (df) and optionally, a list of sensitive columns, organization columns, and user specified redaction column. 
    def __init__(self, df, parent_organization=None, child_organization=None, sensitive_columns=None, frequency=None, redact_column=None, minimum_threshold=10, redact_zero = False):
        
        self.validate_inputs(df, parent_organization, child_organization, sensitive_columns, frequency, redact_column, minimum_threshold, redact_zero) #Validating user inputs

        if (child_organization is None) & (parent_organization is None):
            organization_columns = None
        elif (parent_organization is not None) & (child_organization is None):
            organization_columns = [parent_organization]
        elif (parent_organization is None) & (child_organization is not None):
            organization_columns = [child_organization]
        else:
            organization_columns = [parent_organization, child_organization]
        logger.info(organization_columns)
        
        df.loc[:, 'Original'] = 1
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
            self.df.loc[:, 'UserRedact'] = 0
        
        # Check the type of organization_columns and store it as an instance variable
        if redact_column is None:
            logger.info('Redact is empty or not included')  # Display a message if organization_columns is None
            self.redact_column = ['UserRedact']  # Initialize an empty list
        elif isinstance(redact_column, str):
            self.redact_column = [redact_column]  # Convert a single string to a list with one item
        else:
            self.redact_column = redact_column  # Store the provided list
        
        self.frequency = frequency
        self.minimum_threshold = minimum_threshold
        self.parent_organization = parent_organization
        self.child_organization = child_organization
        self.redact_zero = redact_zero
        

    def validate_inputs(self, df, parent_organization, child_organization, sensitive_columns, frequency, redact_column, minimum_threshold, redact_zero):
        #The class currently supports only dataframes as an input. If this changes to support .csvs or other formats this check can be expanded. 
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Data object must be a DataFrame.")

        ##Validate that specified columns exist in the dataframe and that all mandatory inputs are included. Mostly checking for spelling errors from end user. 
        if parent_organization and parent_organization not in df.columns:
            raise KeyError(f"Parent organization column '{parent_organization}' does not exist in the DataFrame. Verify you spelled the column name correctly.")
        if child_organization and child_organization not in df.columns:
            raise KeyError(f"Child organization column '{child_organization}' does not exist in the DataFrame. Verify you spelled the column name correctly.")


        #Validating presenence of all senstive columns 
        if isinstance(sensitive_columns, str):
            sensitive_columns_list = [sensitive_columns]
        else:
            sensitive_columns_list = sensitive_columns

        missing_sensitive_columns = []
        for col in sensitive_columns_list:
            if col not in df.columns:
                missing_sensitive_columns.append(col)
        
        if missing_sensitive_columns:
                raise KeyError(f"Sensitive columns '{missing_sensitive_columns}' do not exist in the DataFrame. Verify you spelled the column names correctly.")

        if not frequency:
            raise KeyError("You must specify a frquency column containing counts.")
        
        if frequency not in df.columns:
            raise KeyError(f"Frequency column '{frequency}' does not exist in the DataFrame. Verify you spelled the column name correctly.")
     
        #Validating frequency column values 
        try:
            pd.to_numeric(df[frequency])
        except ValueError:
            raise ValueError(f"All values in the frequency column '{frequency}' must be numeric or null.")
        
        #Validating redact column - includes check for invalid values in the column. 
        if redact_column and redact_column not in df.columns:
            raise KeyError(f"User specified redaction column '{redact_column}' does not exist in the DataFrame. Verify you spelled the column name correctly.")
        if redact_column:
            try:
                numeric_values = pd.to_numeric(df[redact_column])
            except ValueError:
                raise ValueError(f"The user specified redact_column '{redact_column}' contains non-numeric and non-null values. Valid values are 0 and 1.")
            
            numeric_values = numeric_values[~numeric_values.isna()]

            invalid_values = numeric_values[~numeric_values.isin([0,1])]

            if not invalid_values.empty:
                raise ValueError(f"The user specified redact_column '{redact_column}' contains the following invalid numeric values: {invalid_values.unique()}. Please only include values of 0 and 1.")

        #Validate minimum threshold input
        if minimum_threshold < 0:
            raise ValueError("Minimum threshold for redaction must be a positive number.")

        #Validate redact_zero input
        if redact_zero not in [True, False]:
            raise ValueError("Value for redact_zero should be True or False, not {}. Please only use True or False without quotation marks.".format(redact_zero))
            

    def create_log(self):
        logger.info('Creating log!')
        df_dataframes = pd.DataFrame()
        grouping_value = 0
        if self.organization_columns[0] is not None:
            for organization_column in self.organization_columns:
                for sensitive_combination in self.sensitive_combinations:
                    df_grouped = self.df.groupby([organization_column] + list(sensitive_combination)  + ['UserRedact'])[self.frequency].sum().reset_index()
                    df_grouped.loc[:, 'Grouping'] = grouping_value
                    grouping_value += 1
                    df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
                    df_grouped_min = df_not_redacted.groupby([organization_column]  + ['UserRedact'])[self.frequency].min().reset_index()
                    df_grouped_min.rename(columns={self.frequency: "MinimumValue"}, inplace=True)
                    df_grouped = df_grouped.merge(df_grouped_min, on=[organization_column] + ['UserRedact'], how='left')
                    df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
        if self.parent_organization is not None:
            df_grouped = self.df.groupby([self.parent_organization]  + ['UserRedact'])[self.frequency].sum().reset_index()
            df_grouped.loc[:, 'Grouping'] = grouping_value
            grouping_value += 1
            df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
            df_grouped_min = df_not_redacted.groupby(['Grouping']  + ['UserRedact'])[self.frequency].min().reset_index()
            df_grouped_min.rename(columns={self.frequency: "MinimumValue"}, inplace=True)
            df_grouped = df_grouped.merge(df_grouped_min, on=['Grouping']  + ['UserRedact'], how='left')
            df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
            
        for sensitive_combination in self.sensitive_combinations:
            list_combination = list(sensitive_combination)
            df_grouped = self.df.groupby(list_combination + ['UserRedact'])[self.frequency].sum().reset_index()
            df_grouped.loc[:, 'Grouping'] = grouping_value
            grouping_value += 1
            df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
            df_not_redacted = df_dataframes[df_dataframes[self.frequency] > self.minimum_threshold]
            if (list_combination != self.sensitive_columns) | (len(self.sensitive_columns) == 1) :
                df_grouped_min = df_not_redacted.groupby(['Grouping'] + list_combination + ['UserRedact'])[self.frequency].min().reset_index()
                string_combination = ''.join(list_combination)
                df_grouped_min.rename(columns={self.frequency: "MinimumValue" + string_combination}, inplace=True)
                df_dataframes = df_dataframes.merge(df_grouped_min, on= ['Grouping'] + list_combination + ['UserRedact'], how='left')
        if self.organization_columns[0] is not None:
            df_log_original =  pd.DataFrame(self.df[self.organization_columns + self.sensitive_columns +[self.frequency] + self.redact_column])
            df_log_original.loc[:, 'Grouping'] = grouping_value
            df_log = pd.concat([df_dataframes, df_log_original])
            df_log.drop_duplicates(self.organization_columns + self.sensitive_columns +[self.frequency] + self.redact_column, inplace=True)
        else:
            df_log_original =  pd.DataFrame(self.df[self.sensitive_columns +[self.frequency] + self.redact_column])
            df_log_original.loc[:, 'Grouping'] = grouping_value
            df_log = pd.concat([df_dataframes, df_log_original])
            df_log.drop_duplicates(self.sensitive_columns +[self.frequency] + self.redact_column, inplace=True)
        df_log.reset_index(drop=True, inplace=True)
        df_log.loc[:, 'RedactBinary'] = 0
        df_log.loc[:, 'Redact'] = 'Not Redacted'
        
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

        self.df_log.loc[:, 'RedactBreakdown'] = 'Not Redacted'

        logger.info('Log created!')
        return self.df_log

    def redact_user_requested_records(self):
        logger.info('Seeing if user redact column exists.')
        self.df_log.loc[(self.df_log["UserRedact"] == 1), 'RedactBinary'] = 1

        self.df_log.loc[(self.df_log["UserRedact"] == 1), 'Redact'] = 'User-requested redaction'
        self.df_log.loc[(self.df_log["UserRedact"] == 1), 'RedactBreakdown'] += ', User-requested redaction'
        self.df_log = self.df_log.drop('UserRedact', axis=1)
        logger.info('Completed review if user redact column exists.')
        return self.df_log
    # Method to redact values in the dataframe that are less than a minimum threshold (possibly including 0)
    def less_than_threshold(self):
        # Create a boolean mask that identifies rows where the column specified by 'frequency'
        # has values less than 'minimum_threshold'
        # and also identify rows equal to 0 if correct parameter was passed in
        if self.redact_zero == False:
            logger.info('Redacting values that are less than the threshold and not zero.')
            mask = (self.df_log[self.frequency] <= self.minimum_threshold) & (self.df_log[self.frequency] != 0)
        else:
            logger.info('Redacting values that are less than the threshold or equal to zero.')
            mask = (self.df_log[self.frequency] <= self.minimum_threshold) | (self.df_log[self.frequency] == 0)

        self.df_log.loc[mask, 'RedactBinary'] = 1
        
        # Update a new column named 'Redact' with a message for the rows that meet the condition specified by the mask
        self.df_log.loc[mask, 'Redact'] = 'Primary Suppression'
        if self.redact_zero == False:
            self.df_log.loc[mask, 'RedactBreakdown'] += f', Less Than or equal to {self.minimum_threshold} and not equal to zero'
            logger.info('Completed redacting values less than or equal to the threshold and not zero.')
        else:
            self.df_log.loc[mask, 'RedactBreakdown'] += f', Less Than or equal to {self.minimum_threshold} or zero'
            logger.info('Completed redacting values less than or equal to the threshold or equal to zero.')
        # Return the updated dataframe
        return self.df_log

    # Method to redact values in the dataframe that are overlapping with other redacted values
    def redact_threshold(self):
        logger.info('Begin Redacting based on the overlapping relationships between organizations and subgroups.')
        self.df_log.loc[:, 'Overlapping'] = 0
        
        # Loop through each sensitive column to check for overlapping sensitive information
        for sensitive_column in self.sensitive_columns:
            list_sensitive = self.df_log[self.df_log['RedactBinary'] == 1][sensitive_column].unique()

            self.df_log.loc[self.df_log[sensitive_column].isin(list_sensitive), 'Overlapping'] += 1
        
        # Mark rows with maximum overlapping as 'Suppressed'
        mask = (self.df_log['Overlapping'] == self.df_log['Overlapping'].max()) & (self.df_log['RedactBinary'] == 0)

        self.df_log.loc[mask, 'Redact'] = 'Overlapping threshold secondary suppression'
        
        self.df_log.loc[mask, 'RedactBinary'] = 1

        self.df_log.loc[mask, 'RedactBreakdown'] += ', Overlapping threshold secondary suppression'
            
        # Return the modified dataframe
        logger.info('Completed redaction based on the overlapping relationships between organizations and subgroups.')
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
        logger.info('Start review of if secondary disclosure avoidance is needed and begin application.') 
        # Filter rows where the value in the column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_redact_count = self.df_log[self.df_log['RedactBinary'] == 1]
        df_redact_count.loc[:, 'Redacted'] = 1
        if self.organization_columns[0] is not None:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_merge = df_redact_count[['Grouping'] + self.organization_columns + list_combination + ['Redacted']]
                    df_redact_merge = df_redact_merge.copy()
                    df_redact_merge.drop_duplicates(inplace=True)
                    df_primary = self.df_log.merge(df_redact_merge, on=['Grouping'] + self.organization_columns + list_combination, how='left')
                    mask = (df_primary['Redacted'] == 1) & (df_primary["MinimumValue" + string_combination] == df_primary[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
                    self.df_log.loc[mask, 'RedactBreakdown'] += ', One count redacted leading to secondary suppression'

        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if (list_combination != self.sensitive_columns) | (len(self.sensitive_columns) == 1) :
                    string_combination = ''.join(list_combination)
                    df_redact_merge = df_redact_count[['Grouping'] + list_combination + ['Redacted']]
                    df_redact_merge = df_redact_merge.copy()
                    df_redact_merge.drop_duplicates(inplace=True)
                    df_primary = self.df_log.merge(df_redact_merge, on=['Grouping'] + list_combination, how='left')
                    mask = (df_primary['Redacted'] == 1) & (df_primary["MinimumValue" + string_combination] == df_primary[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
                    self.df_log.loc[mask, 'RedactBreakdown'] += ', One count redacted leading to secondary suppression'

        logger.info('Completion of initial step with secondary disclosure avoidance!')
        # Return the updated dataframe
        return self.df_log
    
    def one_redact_zero(self):    
        logger.info('Start review of next step of secondary disclosure avoidance where review of one count of redacted category in a group.')
        df_log_na = self.df_log.copy()

        temp_value = 'NaFill'

        for sensitive_column in self.sensitive_columns:
            df_log_na[sensitive_column].fillna(temp_value, inplace=True)  
        
        # Grouping by Organization and counting StudentCount, then filtering groups with a single record 
        if self.organization_columns[0] is not None: 
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_less = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_redact_less.loc[:, 'Redacted'] = 1
                    df_count = df_redact_less.groupby(['Grouping'] + self.organization_columns + list_combination)['Redacted'].count().reset_index()
                    df_one_redacted = df_count[df_count['Redacted'] == 1]
                    if not df_one_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum = df_not_redacted.groupby(['Grouping'] + self.organization_columns + list_combination, dropna=False)[self.frequency].min().reset_index()
                        df_minimum.rename(columns={self.frequency:'LastMiniumValue'}, inplace=True)
                        df_minimum_redacted = df_one_redacted.merge(df_minimum, on = ['Grouping'] + self.organization_columns + list_combination)
                        df_minimum_one = df_log_na.merge(df_minimum_redacted, on = ['Grouping'] + self.organization_columns + list_combination, how='left')
                        mask = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[mask, 'RedactBinary'] = 1
                        df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[mask, 'RedactBreakdown'] += ', Redacting zeroes or other remaining values missed in one count function'
        
        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_less = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_redact_less.loc[:, 'Redacted'] = 1
                    df_count = df_redact_less.groupby(['Grouping'] + list_combination)['Redacted'].count().reset_index()
                    df_one_redacted = df_count[df_count['Redacted'] == 1]
                    if not df_one_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum = df_not_redacted.groupby(['Grouping'] + list_combination, dropna=False)[self.frequency].min().reset_index()
                        df_minimum.rename(columns={self.frequency:'LastMiniumValue'}, inplace=True)
                        df_minimum_redacted = df_one_redacted.merge(df_minimum, on = ['Grouping'] + list_combination)
                        df_minimum_one = df_log_na.merge(df_minimum_redacted, on = ['Grouping'] + list_combination, how='left')
                        mask = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[mask, 'RedactBinary'] = 1
                        df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[mask, 'RedactBreakdown'] += ', Redacting zeroes or other remaining values missed in one count function'

        self.df_log.loc[df_log_na['RedactBinary'] == 1, 'RedactBinary'] = 1
        self.df_log.loc[df_log_na['Redact'] == 'Secondary Suppression', 'Redact'] = 'Secondary Suppression'
        self.df_log.loc[:, 'RedactBreakdown'] = df_log_na['RedactBreakdown']

        logger.info('Complete review of secondary disclosure avoidance where review of one count of redacted category in a group.')
        
        return self.df_log
        
    def cross_suppression(self):
        logger.info('Begin analysis if secondary redaction on aggregate levels needs to be applied to original dataframe.')
        df_parent_redact = self.df_log[(self.df_log['Grouping'] > 0) & (self.df_log['RedactBinary'] == 1)]
        redact_parent_name = 'RedactParentBinary'
        df_parent_redact.rename(columns = {'RedactBinary':redact_parent_name}, inplace=True)
        if self.organization_columns[0] is not None: 
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                df_test = df_parent_redact[df_parent_redact[self.organization_columns + list(sensitive_combination)].notna().all(axis=1)]
                if (list_combination != self.sensitive_columns) & (not df_test.empty):
                    string_combination = ''.join(list_combination)
                    df_parent_list = df_parent_redact[self.organization_columns + list(sensitive_combination) + [redact_parent_name]]
                    df_primary = self.df_log.merge(df_parent_list, on = self.organization_columns +  list_combination, how='left')
                    mask = (df_primary['RedactParentBinary'] == 1) & (df_primary["MinimumValue" + string_combination] == df_primary[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
                    self.df_log.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if (list_combination != self.sensitive_columns):  
                    string_combination = ''.join(list_combination)
                    df_redacted = self.df_log[(self.df_log['RedactBinary'] == 1) & (self.df_log['Grouping'] == 0)]
                    df_count = df_redacted.groupby(['Grouping'] + self.organization_columns +  list(sensitive_combination))['RedactBinary'].count().reset_index()
                    df_one_count = df_count[df_count['RedactBinary'] == 1]
                    df_one_count = df_one_count[['Grouping'] + self.organization_columns + list(sensitive_combination)]
                    df_one_count = self.df_log.merge(df_one_count, on =['Grouping'] + self.organization_columns + list(sensitive_combination))
                    df_one_count = df_one_count[(df_one_count['RedactBinary'] == 0)]
                    df_minimum = df_one_count.groupby(['Grouping'] + self.organization_columns + list(sensitive_combination))[self.frequency].min().reset_index()
                    df_minimum.rename(columns = {self.frequency:'CrossMinimum' + string_combination}, inplace=True)
                    df_minimum_value = self.df_log.merge(df_minimum, on =['Grouping'] + self.organization_columns + list(sensitive_combination), how='left')
                    mask = (df_minimum_value["CrossMinimum" + string_combination] == df_minimum_value[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
                    self.df_log.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'
        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                df_test = df_parent_redact[df_parent_redact[list(sensitive_combination)].notna().all(axis=1)]
                if (list_combination != self.sensitive_columns) & (not df_test.empty):
                    string_combination = ''.join(list_combination)
                    df_parent_list = df_parent_redact[list(sensitive_combination) + [redact_parent_name]]
                    df_primary = self.df_log.merge(df_parent_list, on = list_combination, how='left')
                    mask = (df_primary['RedactParentBinary'] == 1) & (df_primary["MinimumValue" + string_combination] == df_primary[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
                    self.df_log.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if (list_combination != self.sensitive_columns):  
                    string_combination = ''.join(list_combination)
                    df_redacted = self.df_log[(self.df_log['RedactBinary'] == 1) & (self.df_log['Grouping'] == 0)]
                    df_count = df_redacted.groupby(['Grouping'] + list(sensitive_combination))['RedactBinary'].count().reset_index()
                    df_one_count = df_count[df_count['RedactBinary'] == 1]
                    df_one_count = df_one_count[['Grouping'] + list(sensitive_combination)]
                    df_one_count = self.df_log.merge(df_one_count, on =['Grouping'] + list(sensitive_combination))
                    df_one_count = df_one_count[(df_one_count['RedactBinary'] == 0)]
                    df_minimum = df_one_count.groupby(['Grouping'] + list(sensitive_combination))[self.frequency].min().reset_index()
                    df_minimum.rename(columns = {self.frequency:'CrossMinimum' + string_combination}, inplace=True)
                    df_minimum_value = self.df_log.merge(df_minimum, on =['Grouping'] + list(sensitive_combination), how='left')
                    mask = (df_minimum_value["CrossMinimum" + string_combination] == df_minimum_value[self.frequency])
                    self.df_log.loc[mask, 'RedactBinary'] = 1
                    self.df_log.loc[mask, 'Redact'] = 'Secondary Suppression'
                    self.df_log.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'

        logger.info('Completion of analysis if secondary redaction on aggregate levels needs to be applied to original dataframe.')
        return self.df_log
    
    def apply_log(self):
        logger.info('Start applying log to given dataframe.')
        self.df_log.loc[:, 'RedactBreakdown'] = self.df_log['RedactBreakdown'].str.replace('Not Redacted, ', '')
        if self.organization_columns[0] is not None:
            df_redacted =  self.df.merge(self.df_log, on = self.organization_columns + self.sensitive_columns +  [self.frequency], how='inner')
            columns = self.organization_columns + self.sensitive_columns +  [self.frequency] + ['RedactBinary', 'Redact', 'RedactBreakdown']
        else:
            df_redacted =  self.df.merge(self.df_log, on = self.sensitive_columns +  [self.frequency], how='inner')
            columns = self.sensitive_columns +  [self.frequency] + ['RedactBinary', 'Redact', 'RedactBreakdown']

        if self.redact_column is not None:
            columns = columns + [self.redact_column]
        
        df_redacted = df_redacted[columns]
        # df_redacted = df_redacted.drop_duplicates().reset_index(drop=True) # this helps remove the duplicate issue, but the duplicates should not be there
        self.df_redacted = df_redacted

        logger.info('Finished applying log to given dataframe!')
        
        return self.df_redacted
    # New method to call the specified functions
    def get_log(self):
        logger.info('Pulling log from class.')
        logger.info('Log returned from class!')
        return self.df_log

    def apply_anonymization(self):
        
        self.create_log()

        # Call redact_user_requested_records
        self.redact_user_requested_records()
        
        # Call less_than_threshold
        self.less_than_threshold()
        
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
        