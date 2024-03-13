#### How to run DART from your R file ####
# Python must be installed locally to utilize this tool

# import reticulate package, which enables you to access and run Python
library(reticulate)

# load in the DataAnonymizer class with source_python
source_python("..\\dar_tool\\suppression_check.py")

# read in your data you would like to redact
df <- read.csv("..\\data\\TwoSensitive.csv")

# initialize parameters to be used in DataAnonymizer call
sensitive_list <- list('Subgroup1', 'Subgroup2')
frequency_value <- 'Counts'

# initialize the anonymizer
anonymizer <- DataAnonymizer(df, sensitive_columns = sensitive_list, 
                            frequency = frequency_value, redact_column = 'UserRedact')

# call the function apply_anonymization and save to a new dataframe
df_redacted <- anonymizer$apply_anonymization()

# view redacted dataframe
View(df_redacted)
