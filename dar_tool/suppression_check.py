
import pandas as pd
#import logging
from itertools import combinations

from pandas import DataFrame

from util import LogUtil

# Configure logging
# logger = logging.getLogger(__name__)
# logger.setLevel(
#     logging.INFO)  # Sets lowest level logger will handle. Debug level messages will be ignored with this setting.
#
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# handler = logging.StreamHandler()  # Sends logs to console by default
# handler.setFormatter(formatter)
# logger.addHandler(handler)


logger = LogUtil.create_logger(__name__)
class DataAnonymizer:


    # Initialize the class with a dataframe (df) and optionally, a list of sensitive columns, organization columns, and user specified redaction column.
    def __init__(self, df: DataFrame, parent_organization:str = None, child_organization:str=None, sensitive_columns=None,
                 frequency: str = None, redact_column:str=None, minimum_threshold:int=10, redact_zero:bool
                 =False, redact_value:str=None):

        self.original_columns = df.columns.tolist()
        logger.info('original_columns that came>>%s', self.original_columns)

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
        self.df: DataFrame = df.copy()

        self.organization_columns = list(organization_columns) if isinstance(organization_columns, (list, tuple)) else [
            organization_columns]
        """organization_columns is a combination of parent_organization and child_organization if both are present, if one of them is null
           use the not null column as organization column
        """

        self.sensitive_columns = list(sensitive_columns) if isinstance(sensitive_columns, (list, tuple)) else [
            sensitive_columns]
        self.sensitive_combinations = sorted([combo for i in range(1, len(self.sensitive_columns) + 1) for combo in
                                              combinations(self.sensitive_columns, i)], key=len, reverse=True)
        self.redact_column = redact_column

        # Rename the user supplied redact column to "UserRedact"
        if self.redact_column is None:
            logger.info('Redact is empty or not included')  # Display a message if redact_column is None

        self.frequency = frequency
        self.minimum_threshold = int(minimum_threshold)
        self.parent_organization = parent_organization
        self.child_organization = child_organization
        self.redact_zero = redact_zero
        self.redact_value = redact_value


    def validate_inputs(self, df, parent_organization, child_organization, sensitive_columns, frequency, redact_column,
                        minimum_threshold, redact_zero):
        # The class currently supports only dataframes as an input. If this changes to support .csvs or other formats this check can be expanded.
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Data object must be a DataFrame.")

        ##Validate that specified columns exist in the dataframe and that all mandatory inputs are included. Mostly checking for spelling errors from end user.
        if parent_organization and parent_organization not in df.columns:
            raise KeyError(
                f"Parent organization column '{parent_organization}' does not exist in the DataFrame. Verify you spelled the column name correctly.")
        if child_organization and child_organization not in df.columns:
            raise KeyError(
                f"Child organization column '{child_organization}' does not exist in the DataFrame. Verify you spelled the column name correctly.")

        # Add validation that at least one sensitive column exists
        if sensitive_columns is None:
            raise KeyError("You must specify at least one sensitive column.")

        # Validating presenence of all senstive columns
        if isinstance(sensitive_columns, str):
            sensitive_columns_list = [sensitive_columns]
        else:
            sensitive_columns_list = sensitive_columns

        missing_sensitive_columns = []
        for col in sensitive_columns_list:
            if col not in df.columns:
                missing_sensitive_columns.append(col)

        if missing_sensitive_columns:
            raise KeyError(
                f"Sensitive columns '{missing_sensitive_columns}' do not exist in the DataFrame. Verify you spelled the column names correctly.")

        if not frequency:
            raise KeyError("You must specify a frquency column containing counts.")

        if frequency not in df.columns:
            raise KeyError(
                f"Frequency column '{frequency}' does not exist in the DataFrame. Verify you spelled the column name correctly.")

        # Validating frequency column values
        try:
            pd.to_numeric(df[frequency])
        except ValueError:
            raise ValueError(f"All values in the frequency column '{frequency}' must be numeric or null.")

        # Validating redact column - includes check for invalid values in the column.
        if redact_column and redact_column not in df.columns:
            raise KeyError(
                f"User specified redaction column '{redact_column}' does not exist in the DataFrame. Verify you spelled the column name correctly.")
        if redact_column:
            try:
                numeric_values = pd.to_numeric(df[redact_column])
            except ValueError:
                raise ValueError(
                    f"The user specified redact_column '{redact_column}' contains non-numeric and non-null values. Valid values are 0 and 1.")

            numeric_values = numeric_values[~numeric_values.isna()]

            invalid_values = numeric_values[~numeric_values.isin([0, 1])]

            if not invalid_values.empty:
                raise ValueError(
                    f"The user specified redact_column '{redact_column}' contains the following invalid numeric values: {invalid_values.unique()}. Please only include values of 0 and 1.")

        # Validate minimum threshold input
        if minimum_threshold < 0:
            raise ValueError("Minimum threshold for redaction must be a positive number.")

        # Validate redact_zero input
        if redact_zero not in [True, False]:
            raise ValueError(
                "Value for redact_zero should be True or False, not {}. Please only use True or False without quotation marks.".format(
                    redact_zero))

        # Check if duplicates are present in the groping columns. All rows should represent a unique group.

        subset_cols = []

        # Append must be used instead of extend since parent_org and child_org should always be strings. If you extend a string it unpacks it and adds every character to the list.
        if parent_organization is not None:
            subset_cols.append(parent_organization)
        if child_organization is not None:
            subset_cols.append(child_organization)

        if isinstance(sensitive_columns, str):
            sensitive_columns = [sensitive_columns]
        subset_cols.extend(sensitive_columns)

        if df.duplicated(subset=subset_cols).any():
            raise ValueError(f"""
                             Each combination of values in the {subset_cols} columns should be unique, but there are instances where the same combination appears more than once. 
                             Check your input dataframe and use more subgroups or organizations as needed to make sure each row identifies a unique group. 
                             The following grouping column values are duplicated: \n {df[subset_cols][df.duplicated(subset=subset_cols)]}
                             """)
        if sensitive_columns is not None:
            for column in sensitive_columns:
                df[column] = df[column].astype(str)

        # Ensure that the frequency column contains only integer values
        try:
            df[frequency] = df[frequency].astype(int)
        except ValueError:
            raise ValueError(f"All values in the frequency column '{frequency}' must be integers.")

    def create_log(self):
        logger.info('Creating log!')
        df_dataframes: DataFrame = pd.DataFrame()
        grouping_value = 0
        if self.organization_columns[0] is not None:
            for organization_column in self.organization_columns:
                for sensitive_combination in self.sensitive_combinations:
                    group_by_col: list = [organization_column] + list(sensitive_combination)
                    logger.info('group_by_col>>%s,frequency_col>>%s', group_by_col, self.frequency)

                    """
                    sum of GraduationCount for group of ParentEntity1, SubGroup1 SubGroup2
                    group_by_col is an array of columns
                    For eg
                    
                    select ParentEntity1, SubGroup1 SubGroup2, sum(GraduationCount) from record_table
                    group by ParentEntity1, SubGroup1 SubGroup2
                    
                    and assigning result  to a new table called df_grouped
                    """
                    df_grouped: DataFrame = self.df.groupby(group_by_col)[self.frequency].sum().reset_index()
                    logger.info('df_grouped>>>')
                    logger.info("****")
                    logger.info('%s', df_grouped)
                    if not df_grouped.empty:
                        # assigning a new column Grouping and give current grouping_value
                        df_grouped.loc[:, 'Grouping'] = grouping_value
                        logger.info('after adding grouping')
                        
                        grouping_value += 1
                        """
                        records where GraduationCount column value > threshold
                        select * from df_grouped where GraduationCount > 10
                        assign result to new record assigning to   df_not_redacted
                        from df_grouped 
                        """
                        df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]

                        """
                        min of frequency column and group by organization_column
                        here organization_column = ParentEntity
                        frequency = GraduationCount
                        This is 
                        select ParentEntity,min(GraduationCount) from df_not_redacted
                        """
                        df_grouped_min = df_not_redacted.groupby([organization_column])[
                            self.frequency].min().reset_index()
                        ## rename GraduationCount to MinimumValue
                        df_grouped_min = df_grouped_min.rename(columns={self.frequency: "MinimumValue"})

                        # Ensure that the parent and child columns are strings
                        df_grouped_min[organization_column] = df_grouped_min[organization_column].astype(str)
                        """Join df_grouped and df_grouped_min on organization_column 
                        select * from df_grouped df
                        join df_grouped_min dfn left join df.organization_column = dfn
                        and assign the result back to df_group
                        """
                        df_grouped = df_grouped.merge(df_grouped_min, on=[organization_column], how='left')
                        """This is a union
                        select * from df_dataframes
                        union
                        select * from df_grouped
                        
                        assign the result to df_dataframes
                        """
                        df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
                        logger.info('For Group>>%s,values\n>>%s',df_dataframes.columns,df_dataframes)
        if self.parent_organization is not None:
            df_grouped: DataFrame = self.df.groupby([self.parent_organization])[self.frequency].sum().reset_index()

            if not df_grouped.empty:
                df_grouped.loc[:, 'Grouping'] = grouping_value
                grouping_value += 1
                df_not_redacted = df_grouped[df_grouped[self.frequency] > self.minimum_threshold]
                df_grouped_min = df_not_redacted.groupby(['Grouping'])[self.frequency].min().reset_index()
                df_grouped_min = df_grouped_min.rename(columns={self.frequency: "MinimumValue"})
                df_grouped = df_grouped.merge(df_grouped_min, on=['Grouping'], how='left')
                df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)

        for sensitive_combination in self.sensitive_combinations:
            list_combination = list(sensitive_combination)
            df_grouped = self.df.groupby(list_combination)[self.frequency].sum().reset_index()
            if not df_grouped.empty:
                df_grouped.loc[:, 'Grouping'] = grouping_value
                grouping_value += 1
                df_dataframes = pd.concat([df_dataframes, df_grouped], ignore_index=True)
                df_not_redacted = df_dataframes[df_dataframes[self.frequency] > self.minimum_threshold]
                if (list_combination != self.sensitive_columns) | (len(self.sensitive_columns) == 1):
                    df_grouped_min = df_not_redacted.groupby(['Grouping'] + list_combination)[
                        self.frequency].min().reset_index()
                    string_combination = ''.join(list_combination)
                    df_grouped_min = df_grouped_min.rename(
                        columns={self.frequency: "MinimumValue" + string_combination})
                    df_dataframes = df_dataframes.merge(df_grouped_min, on=['Grouping'] + list_combination, how='left')

        self.df.loc[:, 'Grouping'] = grouping_value
        df_log:DataFrame = pd.concat([df_dataframes, self.df])
        duplicate_columns:list[str | list[str]] = []
        if self.organization_columns[0] is not None and self.redact_column is not None:
            duplicate_columns = self.organization_columns + self.sensitive_columns + [self.frequency] + [
                self.redact_column]
        elif self.organization_columns[0] is not None and self.redact_column is None:
            duplicate_columns = self.organization_columns + self.sensitive_columns + [self.frequency]
        elif self.organization_columns[0] is None and self.redact_column is not None:
            duplicate_columns = self.sensitive_columns + [self.frequency] + [self.redact_column]
        elif self.organization_columns[0] is None and self.redact_column is None:
            duplicate_columns = self.sensitive_columns + [self.frequency]
        else:
            print(self.redact_column)
            print(self.organization_columns)

        df_log = df_log.drop_duplicates(duplicate_columns)
        df_log = df_log.reset_index(drop=True)
        df_log.loc[:, 'RedactBinary'] = 0
        df_log.loc[:, 'Redact'] = 'Not Redacted'

        if self.organization_columns[0] is not None:
            df_not_redacted = df_log[(df_log[self.frequency] > self.minimum_threshold)]
            df_grouped_min = df_not_redacted.groupby(self.organization_columns, dropna=False)[
                self.frequency].min().reset_index()
            df_grouped_min = df_grouped_min.rename(columns={self.frequency: "MinimumValueTotal"})

            # Ensure that the parent and child columns are strings
            df_grouped_min[self.organization_columns[0]] = df_grouped_min[self.organization_columns[0]].astype(str)
            if len(self.organization_columns) > 1 and self.organization_columns[1] is not None:
                df_grouped_min[self.organization_columns[1]] = df_grouped_min[self.organization_columns[1]].astype(str)

            df_log = df_log.merge(df_grouped_min, on=self.organization_columns, how='left')
            df_log.loc[df_log['MinimumValue'].isnull(), 'MinimumValue'] = df_log['MinimumValueTotal']
            df_log = df_log.drop('MinimumValueTotal', axis=1)
        self.df_log: DataFrame = df_log.copy()

        df_log = df_log.drop_duplicates()

        self.df_log = self.df_log.reset_index(drop=True)

        self.df_log.loc[:, 'RedactBinary'] = 0

        self.df_log.loc[:, 'Redact'] = 'Not Redacted'

        self.df_log.loc[:, 'RedactBreakdown'] = 'Not Redacted'

        logger.info('Log created!')
        return self.df_log

    # Develop script to autopopulate log for each function

    def data_logger(self, filter_value, redact_name, redact_breakdown_name):
        """Updates
            column RedactBreakdown,
            column Redact and
            column RedactBinary to 1
            based on certain where conditions
        Args:
            filter_value: This is actually where condition provided  by the caller
            redact_name: This value is used to update column Redact
            redact_breakdown_name: This is used to update column RedactBreakdown
        Returns:
                None
        """
        # Update RedactBreakdown column append comma if filter condition matches
        self.df_log.loc[filter_value & (self.df_log['Redact'] != 'User-requested redaction'), 'RedactBreakdown'] += ', '
        # Update column RedactBreakdown by replacing  'Not Redacted' to ', '
        self.df_log.loc[:, 'RedactBreakdown'] = self.df_log['RedactBreakdown'].str.replace('Not Redacted, ', '')

        # Update column RedactBreakdown with redact_breakdown_name field value
        # if filter_value condition matches and Redact column != 'User-requested redaction'
        self.df_log.loc[filter_value & (
                    self.df_log['Redact'] != 'User-requested redaction'), 'RedactBreakdown'] += redact_breakdown_name
        # Update column Redact with redact_name field value
        self.df_log.loc[filter_value & (self.df_log['RedactBinary'] != 1), 'Redact'] = redact_name

        # Update column RedactBinary with 1 if some condition match
        self.df_log.loc[filter_value & (self.df_log['RedactBinary'] != 1), 'RedactBinary'] = 1

        # Take value given by user and apply to log
    def redact_user_requested_records(self):
        logger.info('Seeing if user redact column exists.')
        if self.redact_column is not None:
            self.data_logger((self.df_log[self.redact_column] == 1), 'User-requested redaction',
                             'User-requested redaction')

            self.df_log = self.df_log.drop(self.redact_column, axis=1)

        logger.info('Completed review if user redact column exists.')
        return self.df_log

    # Method to redact values in the dataframe that are less than a minimum threshold (possibly including 0)
    def less_than_threshold(self):
        # Create a boolean mask that identifies rows where the column specified by 'frequency'
        # has values less than 'minimum_threshold'
        # and also identify rows equal to 0 if correct parameter was passed in
        if self.redact_zero == False:
            logger.info('Redacting values that are less than the threshold and not zero.')
            condition = (self.df_log[self.frequency] <= self.minimum_threshold) & (self.df_log[self.frequency] != 0)
            redact_breakdown_name = f'Less Than or equal to {self.minimum_threshold} and not equal to zero'
            logger_value = 'Completed redacting values less than or equal to the threshold and not zero.'
        else:
            logger.info('Redacting values that are less than the threshold or equal to zero.')
            condition = (self.df_log[self.frequency] <= self.minimum_threshold) | (self.df_log[self.frequency] == 0)
            redact_breakdown_name = f'Less Than or equal to {self.minimum_threshold} or zero'
            logger_value = 'Completed redacting values less than or equal to the threshold or equal to zero.'

        self.data_logger(condition, 'Primary Suppression', redact_breakdown_name)

        logger.info(logger_value)

        # Return the updated dataframe
        return self.df_log

    # Method to redact values in the dataframe that are the sum of minimum threshold
    def sum_redact(self):
        # Filter rows where the value in column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_log_na:DataFrame = self.df_log.copy()

        temp_value = 'NaFill'

        for sensitive_column in self.sensitive_columns:
            df_log_na[sensitive_column] = df_log_na[sensitive_column].fillna(temp_value)
            # Grouping by Organization and counting StudentCount, then filtering groups with a single record
        if self.organization_columns[0] is not None:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    sum_redact_group_col = ['Grouping'] + self.organization_columns + list_combination
                    logger.info('sum_redact_group_col>>%s',sum_redact_group_col)
                    string_combination = ''.join(list_combination)
                    df_redacted = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_sum_of_frequency_group_redacted = df_redacted.groupby(sum_redact_group_col)[self.frequency].sum().reset_index()
                    df_less_than_threshold_from_redacted = df_sum_of_frequency_group_redacted[df_sum_of_frequency_group_redacted[self.frequency] <= self.minimum_threshold]
                    if not df_less_than_threshold_from_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum_from_not_redacted = \
                        df_not_redacted.groupby(sum_redact_group_col,dropna=False)[self.frequency].min().reset_index()
                        df_minimum_from_not_redacted = df_minimum_from_not_redacted.rename(columns={self.frequency: 'LastMiniumValue'})

                        # Ensure that the parent and child columns are strings
                        df_minimum_from_not_redacted[self.organization_columns[0]] = df_minimum_from_not_redacted[self.organization_columns[0]].astype(str)
                        if len(self.organization_columns) > 1 and self.organization_columns[1] is not None:
                            df_minimum_from_not_redacted[self.organization_columns[1]] = df_minimum_from_not_redacted[self.organization_columns[1]].astype(
                                str)

                        df_minimum_redacted = df_less_than_threshold_from_redacted.merge(df_minimum_from_not_redacted, on=sum_redact_group_col)
                        df_minimum_redacted = df_minimum_redacted[
                            ['Grouping'] + self.organization_columns + list_combination + ['LastMiniumValue']]
                        df_minimum_one = df_log_na.merge(df_minimum_redacted,
                                                         on=['Grouping'] + self.organization_columns + list_combination,
                                                         how='left')
                        condition = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[condition, 'RedactBinary'] = 1
                        df_log_na.loc[condition, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[condition, 'RedactBreakdown'] += ', Sum of values less than threshold'
        elif len(self.sensitive_combinations) == 1:
            df_redacted = df_log_na[df_log_na['RedactBinary'] == 1]
            df_sum_of_frequency_group_redacted = df_redacted.groupby(['Grouping'], dropna=False)[self.frequency].sum().reset_index()
            df_less_than_threshold_from_redacted = df_sum_of_frequency_group_redacted[df_sum_of_frequency_group_redacted[self.frequency] <= self.minimum_threshold]
            df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
            df_minimum_from_not_redacted = df_not_redacted.groupby(['Grouping'], dropna=False)[self.frequency].min().reset_index()
            df_minimum_from_not_redacted = df_minimum_from_not_redacted.rename(columns={self.frequency: 'LastMiniumValue'})
            df_minimum_redacted = df_less_than_threshold_from_redacted.merge(df_minimum_from_not_redacted, on=['Grouping'])
            df_minimum_redacted = df_minimum_redacted[['Grouping'] + ['LastMiniumValue']]
            df_minimum_one = df_log_na.merge(df_minimum_redacted, on=['Grouping'], how='left')
            condition = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
            df_log_na.loc[condition, 'RedactBinary'] = 1
            df_log_na.loc[condition, 'Redact'] = 'Secondary Suppression'
            df_log_na.loc[condition, 'RedactBreakdown'] += ', Sum of values less than threshold'
        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redacted = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_sum_of_frequency_group_redacted = df_redacted.groupby(['Grouping'] + list_combination, dropna=False)[
                        self.frequency].sum().reset_index()
                    df_less_than_threshold_from_redacted = df_sum_of_frequency_group_redacted[df_sum_of_frequency_group_redacted[self.frequency] <= self.minimum_threshold]
                    if not df_less_than_threshold_from_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum_from_not_redacted = df_not_redacted.groupby(['Grouping'] + list_combination, dropna=False)[
                            self.frequency].min().reset_index()
                        df_minimum_from_not_redacted = df_minimum_from_not_redacted.rename(columns={self.frequency: 'LastMiniumValue'})
                        df_minimum_redacted = df_less_than_threshold_from_redacted.merge(df_minimum_from_not_redacted, on=['Grouping'] + list_combination)
                        df_minimum_redacted = df_minimum_redacted[['Grouping'] + list_combination + ['LastMiniumValue']]
                        df_minimum_one = df_log_na.merge(df_minimum_redacted, on=['Grouping'] + list_combination,
                                                         how='left')
                        condition = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[condition, 'RedactBinary'] = 1
                        df_log_na.loc[condition, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[condition, 'RedactBreakdown'] += ', Sum of values less than threshold'

        self.df_log.loc[df_log_na['RedactBinary'] == 1, 'RedactBinary'] = 1
        self.df_log.loc[df_log_na['Redact'] == 'Secondary Suppression', 'Redact'] = 'Secondary Suppression'
        self.df_log.loc[:, 'RedactBreakdown'] = df_log_na['RedactBreakdown']
        self.df_log.loc[:, 'RedactBreakdown'] = self.df_log['RedactBreakdown'].str.replace('Not Redacted, ', '')

        # Return the updated dataframe
        return self.df_log

    # Method to redact values in the dataframe that are the only value in the group
    def one_count_redacted(self):
        logger.info('Start review of if secondary disclosure avoidance is needed and begin application.')
        # Filter rows where the value in the column specified by 'frequency' is less than 'minimum_threshold' but not zero
        # Filter rows where the value in column specified by 'frequency' is less than 'minimum_threshold' but not zero
        df_log_na = self.df_log.copy()

        temp_value = 'NaFill'

        for sensitive_column in self.sensitive_columns:
            df_log_na[sensitive_column] = df_log_na[sensitive_column].fillna(temp_value)
            # Grouping by Organization and counting StudentCount, then filtering groups with a single record
        if self.organization_columns[0] is not None:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_less = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_redact_less.loc[:, 'Redacted'] = 1
                    df_count_value = \
                    df_redact_less.groupby(['Grouping'] + self.organization_columns + list_combination)[
                        self.frequency].count().reset_index()
                    df_one_redacted = df_count_value[df_count_value[self.frequency] == 1]
                    if not df_one_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum = \
                        df_not_redacted.groupby(['Grouping'] + self.organization_columns + list_combination,
                                                dropna=False)[self.frequency].min().reset_index()
                        df_minimum = df_minimum.rename(columns={self.frequency: 'LastMiniumValue'})
                        df_minimum_redacted = df_one_redacted.merge(df_minimum, on=[
                                                                                       'Grouping'] + self.organization_columns + list_combination)
                        df_minimum_redacted = df_minimum_redacted[
                            ['Grouping'] + self.organization_columns + list_combination + ['LastMiniumValue']]
                        df_minimum_one = df_log_na.merge(df_minimum_redacted,
                                                         on=['Grouping'] + self.organization_columns + list_combination,
                                                         how='left')
                        condition = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[condition, 'RedactBinary'] = 1
                        df_log_na.loc[condition, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[condition, 'RedactBreakdown'] += ', Sum of values less than threshold'
        elif len(self.sensitive_combinations) == 1:
            df_redact_less = df_log_na[df_log_na['RedactBinary'] == 1]
            df_count_value = df_redact_less.groupby(['Grouping'], dropna=False)[self.frequency].count().reset_index()
            df_one_redacted = df_count_value[df_count_value[self.frequency] == 1]
            df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
            df_minimum = df_not_redacted.groupby(['Grouping'], dropna=False)[self.frequency].min().reset_index()
            df_minimum = df_minimum.rename(columns={self.frequency: 'LastMiniumValue'})
            df_minimum_redacted = df_one_redacted.merge(df_minimum, on=['Grouping'])
            df_minimum_redacted = df_minimum_redacted[['Grouping'] + ['LastMiniumValue']]
            df_minimum_one = df_log_na.merge(df_minimum_redacted, on=['Grouping'], how='left')
            condition = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
            df_log_na.loc[condition, 'RedactBinary'] = 1
            df_log_na.loc[condition, 'Redact'] = 'Secondary Suppression'
            df_log_na.loc[condition, 'RedactBreakdown'] += ', One count redacted leading to secondary suppression'
        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_less = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_count_value = df_redact_less.groupby(['Grouping'] + list_combination, dropna=False)[
                        self.frequency].count().reset_index()
                    df_one_redacted = df_count_value[df_count_value[self.frequency] == 1]
                    if not df_one_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum = df_not_redacted.groupby(['Grouping'] + list_combination, dropna=False)[
                            self.frequency].min().reset_index()
                        df_minimum = df_minimum.rename(columns={self.frequency: 'LastMiniumValue'})
                        df_minimum_redacted = df_one_redacted.merge(df_minimum, on=['Grouping'] + list_combination)
                        df_minimum_redacted = df_minimum_redacted[['Grouping'] + list_combination + ['LastMiniumValue']]
                        df_minimum_one = df_log_na.merge(df_minimum_redacted, on=['Grouping'] + list_combination,
                                                         how='left')
                        condition = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[condition, 'RedactBinary'] = 1
                        df_log_na.loc[condition, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[
                            condition, 'RedactBreakdown'] += ', One count redacted leading to secondary suppression'

        self.df_log.loc[df_log_na['RedactBinary'] == 1, 'RedactBinary'] = 1
        self.df_log.loc[df_log_na['Redact'] == 'Secondary Suppression', 'Redact'] = 'Secondary Suppression'
        self.df_log.loc[:, 'RedactBreakdown'] = df_log_na['RedactBreakdown']
        self.df_log.loc[:, 'RedactBreakdown'] = self.df_log['RedactBreakdown'].str.replace('Not Redacted, ', '')

        logger.info('Completion of initial step with secondary disclosure avoidance!')
        # Return the updated dataframe
        return self.df_log

    def one_redact_zero(self):
        logger.info(
            'Start review of next step of secondary disclosure avoidance where review of one count of redacted category in a group.')
        df_log_na = self.df_log.copy()

        temp_value = 'NaFill'

        for sensitive_column in self.sensitive_columns:
            df_log_na[sensitive_column] = df_log_na[sensitive_column].fillna(temp_value)

            # Grouping by Organization and counting StudentCount, then filtering groups with a single record
        if self.organization_columns[0] is not None:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if list_combination != self.sensitive_columns:
                    string_combination = ''.join(list_combination)
                    df_redact_less = df_log_na[df_log_na['RedactBinary'] == 1]
                    df_redact_less.loc[:, 'Redacted'] = 1
                    df_count = df_redact_less.groupby(['Grouping'] + self.organization_columns + list_combination)[
                        'Redacted'].count().reset_index()
                    df_one_redacted = df_count[df_count['Redacted'] == 1]
                    if not df_one_redacted.empty:
                        df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                        df_minimum = \
                        df_not_redacted.groupby(['Grouping'] + self.organization_columns + list_combination,
                                                dropna=False)[self.frequency].min().reset_index()
                        df_minimum = df_minimum.rename(columns={self.frequency: 'LastMiniumValue'})
                        df_minimum_redacted = df_one_redacted.merge(df_minimum, on=[
                                                                                       'Grouping'] + self.organization_columns + list_combination)
                        df_minimum_one = df_log_na.merge(df_minimum_redacted,
                                                         on=['Grouping'] + self.organization_columns + list_combination,
                                                         how='left')
                        mask = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[mask, 'RedactBinary'] = 1
                        df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[
                            mask, 'RedactBreakdown'] += ', Redacting zeroes or other remaining values missed in one count function'

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
                        df_minimum = df_not_redacted.groupby(['Grouping'] + list_combination, dropna=False)[
                            self.frequency].min().reset_index()
                        df_minimum = df_minimum.rename(columns={self.frequency: 'LastMiniumValue'})
                        df_minimum_redacted = df_one_redacted.merge(df_minimum, on=['Grouping'] + list_combination)
                        df_minimum_one = df_log_na.merge(df_minimum_redacted, on=['Grouping'] + list_combination,
                                                         how='left')
                        mask = (df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                        df_log_na.loc[mask, 'RedactBinary'] = 1
                        df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                        df_log_na.loc[
                            mask, 'RedactBreakdown'] += ', Redacting zeroes or other remaining values missed in one count function'

        self.df_log.loc[df_log_na['RedactBinary'] == 1, 'RedactBinary'] = 1
        self.df_log.loc[df_log_na['Redact'] == 'Secondary Suppression', 'Redact'] = 'Secondary Suppression'
        self.df_log.loc[:, 'RedactBreakdown'] = df_log_na['RedactBreakdown']
        self.df_log.loc[:, 'RedactBreakdown'] = self.df_log['RedactBreakdown'].str.replace('Not Redacted, ', '')

        logger.info(
            'Complete review of secondary disclosure avoidance where review of one count of redacted category in a group.')

        return self.df_log

    # Apply cross suppression for aggreagte values that need to be redacted
    def cross_suppression(self):
        logger.info(
            'Begin analysis if secondary redaction on aggregate levels needs to be applied to original dataframe.')

        df_log_na = self.df_log.copy()

        temp_value = 'NaFill'

        for sensitive_column in self.sensitive_columns:
            df_log_na[sensitive_column] = df_log_na[sensitive_column].fillna(temp_value)

        df_parent_redact = self.df_log[(self.df_log['Grouping'] > 0) & (self.df_log['RedactBinary'] == 1)]
        redact_parent_name = 'RedactParentBinary'
        df_parent_redact = df_parent_redact.rename(columns={'RedactBinary': redact_parent_name})
        if self.organization_columns[0] is not None:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                df_test = df_parent_redact[
                    df_parent_redact[self.organization_columns + list(sensitive_combination)].notna().all(axis=1)]
                if (list_combination != self.sensitive_columns) & (not df_test.empty):
                    string_combination = ''.join(list_combination)
                    df_parent_list = df_parent_redact[
                        self.organization_columns + list(sensitive_combination) + [redact_parent_name]]
                    df_parent_list = df_parent_list.drop_duplicates()
                    df_primary = df_log_na.merge(df_parent_list, on=self.organization_columns + list_combination,
                                                 how='left')
                    df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                    df_minimum = df_not_redacted.groupby(self.organization_columns + list_combination, dropna=False)[
                        self.frequency].min().reset_index()
                    df_minimum = df_minimum.rename(columns={self.frequency: 'LastMiniumValue'})

                    for organization_column in self.organization_columns:
                        df_primary[organization_column] = df_primary[organization_column].astype(str)
                        df_minimum[organization_column] = df_minimum[organization_column].astype(str)

                    df_minimum_redacted = df_primary.merge(df_minimum, on=self.organization_columns + list_combination)
                    df_minimum_redacted = df_minimum_redacted[
                        ['Grouping'] + self.organization_columns + list_combination + ['LastMiniumValue'] + [
                            redact_parent_name]]
                    df_minimum = df_minimum_redacted.drop_duplicates()
                    df_minimum_one = df_log_na.merge(df_minimum_redacted,
                                                     on=['Grouping'] + self.organization_columns + list_combination,
                                                     how='left')

                    mask = (df_minimum_one['RedactParentBinary'] == 1) & (df_minimum_one['RedactBinary'] != 1) & (
                                df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                    df_log_na.loc[mask, 'RedactBinary'] = 1
                    df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                    df_log_na.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'

            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if (list_combination != self.sensitive_columns):
                    string_combination = ''.join(list_combination)
                    df_redacted = self.df_log[(df_log_na['RedactBinary'] == 1) & (df_log_na['Grouping'] == 0)]

                    # One count of redacted value is present
                    df_count = \
                    df_redacted.groupby(['Grouping'] + self.organization_columns + list(sensitive_combination))[
                        'RedactBinary'].count().reset_index()
                    df_one_count = df_count[df_count['RedactBinary'] == 1]
                    df_one_count = df_one_count[['Grouping'] + self.organization_columns + list(sensitive_combination)]
                    df_one_count = df_one_count.drop_duplicates()
                    df_one_count = df_log_na.merge(df_one_count, on=['Grouping'] + self.organization_columns + list(
                        sensitive_combination))
                    df_one_count = df_one_count[(df_one_count['RedactBinary'] == 0)]
                    df_minimum = \
                    df_one_count.groupby(['Grouping'] + self.organization_columns + list(sensitive_combination))[
                        self.frequency].min().reset_index()
                    df_minimum = df_minimum.rename(columns={self.frequency: 'CrossMinimum' + string_combination})
                    df_minimum_value = df_log_na.merge(df_minimum, on=['Grouping'] + self.organization_columns + list(
                        sensitive_combination), how='left')
                    mask = (df_minimum_value['RedactBinary'] != 1) & (
                                df_minimum_value["CrossMinimum" + string_combination] == df_minimum_value[
                            self.frequency])

                    df_log_na.loc[mask, 'RedactBinary'] = 1
                    df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                    df_log_na.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'

        else:
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                df_test = df_parent_redact[df_parent_redact[list(sensitive_combination)].notna().all(axis=1)]
                if (list_combination != self.sensitive_columns) & (not df_test.empty):
                    string_combination = ''.join(list_combination)
                    df_parent_list = df_parent_redact[list(sensitive_combination) + [redact_parent_name]]
                    df_parent_list = df_parent_list.drop_duplicates()
                    df_primary = self.df_log.merge(df_parent_list, on=list_combination, how='left')
                    df_not_redacted = df_log_na[df_log_na['RedactBinary'] != 1]
                    df_minimum = df_not_redacted.groupby(list_combination, dropna=False)[
                        self.frequency].min().reset_index()
                    df_minimum = df_minimum.rename(columns={self.frequency: 'LastMiniumValue'})
                    df_minimum_redacted = df_primary.merge(df_minimum, on=list_combination)
                    df_minimum_redacted = df_minimum_redacted[
                        ['Grouping'] + list_combination + ['LastMiniumValue'] + [redact_parent_name]]
                    df_minimum_redacted = df_minimum_redacted.drop_duplicates()
                    df_minimum_one = df_log_na.merge(df_minimum_redacted, on=['Grouping'] + list_combination,
                                                     how='left')

                    mask = (df_minimum_one['RedactParentBinary'] == 1) & (df_minimum_one['RedactBinary'] != 1) & (
                                df_minimum_one[self.frequency] == df_minimum_one['LastMiniumValue'])
                    df_log_na.loc[mask, 'RedactBinary'] = 1
                    df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                    df_log_na.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'
            for sensitive_combination in self.sensitive_combinations:
                list_combination = list(sensitive_combination)
                if (list_combination != self.sensitive_columns):
                    string_combination = ''.join(list_combination)
                    df_redacted = df_log_na[(df_log_na['RedactBinary'] == 1) & (df_log_na['Grouping'] == 0)]
                    df_count = df_redacted.groupby(['Grouping'] + list(sensitive_combination))[
                        'RedactBinary'].count().reset_index()
                    df_one_count = df_count[df_count['RedactBinary'] == 1]
                    df_one_count = df_one_count[['Grouping'] + list(sensitive_combination)]
                    df_one_count = df_one_count.drop_duplicates()
                    df_one_count = df_log_na.merge(df_one_count, on=['Grouping'] + list(sensitive_combination))
                    df_one_count = df_one_count[(df_one_count['RedactBinary'] == 0)]
                    df_minimum = df_one_count.groupby(['Grouping'] + list(sensitive_combination))[
                        self.frequency].min().reset_index()
                    df_minimum = df_minimum.rename(columns={self.frequency: 'CrossMinimum' + string_combination})
                    df_minimum_value = df_log_na.merge(df_minimum, on=['Grouping'] + list(sensitive_combination),
                                                       how='left')
                    mask = (df_minimum_value['RedactBinary'] != 1) & (
                                df_minimum_value["CrossMinimum" + string_combination] == df_minimum_value[
                            self.frequency])
                    df_log_na.loc[mask, 'RedactBinary'] = 1
                    df_log_na.loc[mask, 'Redact'] = 'Secondary Suppression'
                    df_log_na.loc[mask, 'RedactBreakdown'] += ', Redacting based on aggregate level redaction'

        self.df_log.loc[df_log_na['RedactBinary'] == 1, 'RedactBinary'] = 1
        self.df_log.loc[df_log_na['Redact'] == 'Secondary Suppression', 'Redact'] = 'Secondary Suppression'
        self.df_log.loc[:, 'RedactBreakdown'] = df_log_na['RedactBreakdown']
        self.df_log.loc[:, 'RedactBreakdown'] = self.df_log['RedactBreakdown'].str.replace('Not Redacted, ', '')

        logger.info(
            'Completion of analysis if secondary redaction on aggregate levels needs to be applied to original dataframe.')
        return self.df_log


    """
    Will anonymize multiple frequency columns from the dataframe
    for each frequency column call DataAnonymizer
    and merge all of the redacted data set together into one
    """
    def process_multiple_frequency_col(self, frequency_columns):

        logger.info('Inside process_multiple_frequency_col')

        if frequency_columns is None:
            raise Exception("frequency_columns is missing")

        frequency_columns = frequency_columns if isinstance(frequency_columns,list)  else [frequency_columns]
        frequency_column_len = len(frequency_columns)
        initial_columns = self.df.columns.tolist()
        cols_without_freq = [col for col in initial_columns if col not in frequency_columns]
        df_copy_without_freq = self.df.loc[:, cols_without_freq]
        
        composite_key: list[str] = []
        if self.parent_organization is not None:
            composite_key.append(self.parent_organization)
        if self.child_organization is not None:
            composite_key.append(self.child_organization)
        if self.sensitive_columns is not None:
            composite_key.extend(self.sensitive_columns)
            
        #if only one frequency column just anonymize and return
        if frequency_column_len == 1:
            self.frequency = frequency_columns[0]
            df_merged: DataFrame = self.apply_anonymization()
            #df_merged = pd.merge(df_merged,df_copy_without_freq,on=composite_key,how="outer")
            return df_merged


        logger.info("found composite_key>>"+str(composite_key))

        df_copy_arr = []

        
        for idx in range(frequency_column_len):
            agg_colum = frequency_columns[idx]

            # # get other freq cols than we are looping
            other_freq_cols = [col for col in frequency_columns if col != agg_colum]

            #from initial_columns get a column copy of current looping agg_colum or  that is not in other_freq_cols
            select_cols = [col for col in initial_columns if col not in other_freq_cols]
            logger.info('selecting cols for copy>>%s',select_cols,exc_info=1)

            # will use this select_cols to select a new data frame from incoming datafram and add it to df_copy_arr
            df_copy_arr.append( self.df.loc[:,select_cols] )

        # first data frame , we will use to merge other frequency column redaction scenario
        df_first:DataFrame = DataFrame()
        for idx in range(frequency_column_len):

            agg_colum = frequency_columns[idx]
            self.frequency = agg_colum
            logger.info("processing frequency column>>%s",str(agg_colum))
            df_copy = df_copy_arr[idx]

            df_merged: DataFrame = self.apply_anonymization()
            #For each Redact** columns will append the _ and agg_colum to identify it
            redact_key_pair_columns = {
                'RedactBinary': 'RedactBinary_' + agg_colum,
                'Redact': 'Redact_' + agg_colum,
                'RedactBreakdown': 'RedactBreakdown_' + agg_colum
            }

            df_merged = df_merged.rename(columns=redact_key_pair_columns)
            logger.info("df_merged cols>>%s",str(df_merged.columns.tolist()))
            #First in the loop is the main dataframe will use to merge other anonymized datafram
            if idx == 0:
                df_first = df_merged
            else:

                other_freq_cols = composite_key + [agg_colum]+ list(redact_key_pair_columns.values())
                df_merged = df_merged[other_freq_cols]
                df_first = pd.merge(df_first,df_merged,on=composite_key,how="left")


        #df_first = pd.merge(df_first,df_copy_without_freq,on=composite_key,how="left")
        logger.info("done processing multiple frequency col")
        return df_first


    # Integrate log into main dataframe
    def apply_log(self):
        logger.info('Start applying log to given dataframe.')
        logger.info(f'original columns>>{self.df.columns}')
        if self.organization_columns[0] is not None:
            df_redacted = self.df.merge(self.df_log,
                                        on=self.organization_columns + self.sensitive_columns + [self.frequency],
                                        how='inner')
            columns = self.organization_columns + self.sensitive_columns + [self.frequency] + ['RedactBinary', 'Redact',
                                                                                               'RedactBreakdown']
            # columns = list(set(columns) | set(self.df.columns))
            logger.info("organization_columns not null columns>>%s", str(columns))
        else:
            df_redacted = self.df.merge(self.df_log, on=self.sensitive_columns + [self.frequency], how='inner')
            columns = self.sensitive_columns + [self.frequency] + ['RedactBinary', 'Redact', 'RedactBreakdown']
            logger.info("organization_columns is null columns>>{columns}", )
            # columns = list(set(columns) | set(self.df.columns))
        if self.redact_column is not None:
            columns = columns + [self.redact_column]

        logger.info(f'df_redacted columns before selection>>+{df_redacted.columns}')
        logger.info(f"In Apply Log columns to use>>{str(columns)}")
        absent_cols = list(set(self.original_columns) - set(columns))
        rename_hash = {}
        for index, item in enumerate(absent_cols):
            absent_cols[index] = item + '_x'
            rename_hash[absent_cols[index]] = item

        logger.info(f'absent_cols>>,rename_hash >>{str(absent_cols)+":"+str(rename_hash)}')
        columns =  columns + absent_cols
        logger.info(f'columns after adding absent_cols >>{str(absent_cols)}')
        msg = f'columns after adding absent_cols >>{absent_cols}'
        print(msg)
        df_redacted = df_redacted[columns]
        df_redacted = df_redacted.rename(columns=rename_hash)

        if self.redact_value is not None:
            datatype_of_variable = type(self.redact_value)
            df_redacted[self.frequency] = df_redacted[self.frequency].astype(datatype_of_variable)
            df_redacted.loc[df_redacted['RedactBinary'] == 1, self.frequency] = self.redact_value

        self.df_redacted = df_redacted

        logger.info('Finished applying log to given dataframe!')
        # print(self.df_redacted.to_string())
        return self.df_redacted

    # New method to call the specified functions
    def get_log(self):
        logger.info('Pulling log from class.')
        logger.info('Log returned from class!')
        return self.df_log

    def apply_anonymization(self):

        self.validate_inputs(self.df, self.parent_organization, self.child_organization, self.sensitive_columns, self.frequency, self.redact_column,
                             self.minimum_threshold, self.redact_zero)  # Validating user inputs

        self.create_log()

        # Call redact_user_requested_records
        self.redact_user_requested_records()

        # Call less_than_threshold
        # Do Primary Suppression
        self.less_than_threshold()

        # Call sum_redact
        self.sum_redact()

        # Call one_count_redacted
        self.one_count_redacted()

        # Call one_redact_zero
        self.one_redact_zero()

        # Call cross_suppression
        self.cross_suppression()

        # Call apply_log
        self.apply_log()

        # Return the updated dataframe

        return self.df_redacted

