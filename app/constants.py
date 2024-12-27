NONE_RANGE_COL_ONE_TEXT =   """
            #### Grouping Columns

            **Parent Organization**: The top level grouping for your aggregates. This may be a category like "School District". This is optional. 

            **Child Organization**: A subgroup for your aggregates. This may be a category like "School". This is optional. 

            **Sensitive Columns**: Columns that group individual records into an aggregate. This would be a category like "Gender" or "IEP Status". At least one column must be specified. 

            *While parent organization and child organization are optional, you may not have duplicate values across your grouping columns. E.g., You cannot have more than one record for "English Language Learner - Male".*  
              
            *If you do have duplicates, you should specify a child organization or parent organization column to disaggregate these values. E.g., School 1 - English Language Learner - Male, School 2 - English Language Learner - Male*
            """
            
NONE_RANGE_COL_TWO_TEXT = """
            #### Count Column

            **Aggregate Count Colum**: The column that contains your aggregate counts to be suppressed. 
            """


NONE_RANGE_COL_THREE_TEXT = """
            #### Additional User Inputs

            **User Specified Redaction Column**: An optional column where the user may pre-specify which columns they want redaction applied to. The user specififed redaction column should only contain 1s and 0s, where 1 is a row that should have redaction applied. 

            **Minimum Threshold**: The minimum threshold for redaction. This is 10 by default and does not include zero. All values 10 and below, except zero, will be redacted. 

            **Should zeroes be redacted?**: If checked, zeroes will be redacted in addition to values at or below the minimum threshold. 

            **W"""

RANGE_COL_ONE_TEXT = """
### Example columns based on above picture for the range selection are as follows:

TotalCount = TotalStudents

Sub total count = NumberGraduated 

Taget Rate Column = GraduationRate

Threshold percentage = 3


"""           
RANGE_COL_TWO_TEXT = """"""     
RANGE_COL_THREE_TEXT = """"""      