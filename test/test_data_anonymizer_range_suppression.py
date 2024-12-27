from decimal import ROUND_HALF_UP, Decimal, getcontext
import decimal
import hashlib
import random
import uuid
import numpy as np
import pytest
import pandas as pd
import logging

from dar_tool.suppression_check import DataAnonymizer, RangeSuppressionModel
from util import LogUtil
logger:logging = LogUtil.create_logger(__name__)


# """
# in  var args
# every other element starting from 0 index is condition
# ever other element starting from 1 index is the value for that condition
# so say there are 8 length tuple 
# ( 
# ('one' == some_value), 1,
# ('two' == some_value), 2,
# ('three' == some_value),3,
# ('four' == some_value),4
# )
# for index 0 condition truthy value 1 is returned
# for index 1 condition truthy value 2 is returned
# for index 2 condition truthy value 3 is returned
# for index 3 condition truthy value 4 is returned

# so forth 
# """
# def case_when(*args):
#     return np.select(
#         condlist = args[::2], #every other element starting from 0 index is condition
#         choicelist = args[1::2] #every other element starting from 1 index is value or choice for that condition
#     )

# def precision(row:pd.DataFrame):
#     total_fuzzy = row['TotalStudents_fuzzy']
#     logger.info("Inside precision fuzzy: %s", total_fuzzy)
#     precision = 2
#     if total_fuzzy >= 101 and total_fuzzy <= 1000:
#         precision = 3
#     elif total_fuzzy >= 1001 and total_fuzzy <= 10000:
#         precision = 4
#     elif total_fuzzy >= 10001 and total_fuzzy <= 100000:
#         precision = 5
    
#     logger.info("Inside precision precision value: %s", precision)
#     retval = None
#     calc = 0
#     if row['NumberGraduated_Unprotected'] < 3:
#         calc = int(3/total_fuzzy)*100
#         retval = f"<{calc:.{precision}f}%"
#         logger.info("NumberGraduated_Unprotected less 3: %s", retval)

#     elif (row['TotalStudents_Unprotected'] - row['NumberGraduated_Unprotected']) < 3:
#         calc = (1-int(3/total_fuzzy))*100
#         retval = f">{calc:.{precision}f}%"
#         logger.info("TotalStudents_Unprotected - NumberGraduated_Unprotected < 3: %s", retval)    
#     else:
#         retval = f"{row['GraduationRate_Unprotected']:.{4}f}%"
    
#     return retval

     

# def test_top_bottom_one():
#     df:pd.DataFrame = pd.read_csv('./data/dart.csv')

#     totalRow = len(df)
#     #df['new_id'] = [random.uniform(0, 1) for _ in range(totalRow)]
#     # 
#     df['TotalStudents_Unprotected'] = df['TotalStudents']#.copy(deep=True)
#     df['NumberGraduated_Unprotected'] = df['NumberGraduated']#.copy(deep=True)
#     df['GraduationRate_Unprotected'] = df['GraduationRate']#.copy(deep=True)

#     df['TotalStudents_fuzzy'] = case_when(
#        # (df['new_id'] < 0.5) , (df['TotalStudents_Unprotected'] + 1) ,
#         (random.uniform(0, 1) < 0.5) , (df['TotalStudents_Unprotected'] + 1) ,
#         True, df['TotalStudents_Unprotected'] 
#     )
        
#     df['DAT_Reason'] = ''
#     df.loc[(df['NumberGraduated'] < 3) | (df['TotalStudents'] - df['NumberGraduated'] < 3), ['TotalStudents','NumberGraduated','DAT_Reason']] = [np.nan,np.nan,'Top/Bottom']

#     df["GraduationRate"] = df.apply(precision, axis=1)

#     print("")
#     print(df)
    
# def test_suppression():
def test_tail_suppression_based_on_range():
    range_supp = RangeSuppressionModel('TotalStudents','NumberGraduated','GraduationRate',3)
    anonymizer = DataAnonymizer(pd.read_csv('./data/dart.csv'),range_suppression_model=range_supp)
    result_df = anonymizer.apply_anonymization()    

