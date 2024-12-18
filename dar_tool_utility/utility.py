
import pandas as pd
from pandas import DataFrame

from dar_tool.suppression_check import DataAnonymizer
from util.LogUtil import create_logger
"""
Will anonymize multiple frequency columns from the dataframe
for each frequency column call DataAnonymizer
and merge all of the redacted data set together into one
"""

logger = create_logger(__name__)
def process_multiple_frequency_col(df: DataFrame, parent_organization:str = None, child_organization:str=None, sensitive_columns=None,
                                   frequency_columns=None, redact_column:str=None, minimum_threshold:int=10, redact_zero:bool
                 =False, redact_value:str=None):

    logger.info('Inside process_multiple_frequency_col')

    if frequency_columns is None:
        raise Exception("frequency_columns is missing")

    frequency_columns = frequency_columns if isinstance(frequency_columns,list)  else [frequency_columns]
    frequency_column_len = len(frequency_columns)
    initial_columns = df.columns.tolist()
    cols_without_freq = [col for col in initial_columns if col not in frequency_columns]
    df_copy_without_freq = df.loc[:, cols_without_freq]
    
    composite_key: list[str] = []
    if parent_organization is not None:
        composite_key.append(parent_organization)
    if child_organization is not None:
        composite_key.append(child_organization)
    if sensitive_columns is not None:
        composite_key.extend(sensitive_columns)
        
    #if only one frequency column just anonymize and return
    if frequency_column_len == 1:
        anonymizer = DataAnonymizer(df, parent_organization, child_organization,
                                    sensitive_columns, frequency=frequency_columns[0],
                                    minimum_threshold=minimum_threshold, redact_column=redact_column,
                                    redact_zero=redact_zero, redact_value=redact_value)
        df_merged: DataFrame = anonymizer.apply_anonymization()
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
        df_copy_arr.append( df.loc[:,select_cols] )

    # first data frame , we will use to merge other frequency column redaction scenario
    df_first:DataFrame = DataFrame()
    for idx in range(frequency_column_len):

        agg_colum = frequency_columns[idx]
        logger.info("processing frequency column>>%s",str(agg_colum))
        df_copy = df_copy_arr[idx]

        anonymizer = DataAnonymizer( df_copy, parent_organization, child_organization,
                                    sensitive_columns, frequency=agg_colum,
                                    minimum_threshold=minimum_threshold, redact_column=redact_column,
                                    redact_zero=redact_zero, redact_value=redact_value)
        df_merged: DataFrame = anonymizer.apply_anonymization()
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
