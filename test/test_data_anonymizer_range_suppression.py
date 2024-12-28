import pandas as pd
import logging

from dar_tool.suppression_check import DataAnonymizer, RangeSuppressionModel
from util import LogUtil
logger:logging = LogUtil.create_logger(__name__)

# def test_suppression():
def test_tail_suppression_based_on_range():
    range_supp = RangeSuppressionModel('TotalStudents','NumberGraduated','GraduationRate',3)
    anonymizer = DataAnonymizer(pd.read_csv('./data/dart.csv'),range_suppression_model=range_supp)
    result_df = anonymizer.apply_anonymization()    

