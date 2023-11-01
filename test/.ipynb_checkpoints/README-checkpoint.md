# Data Anonymizer
## Overview
The DataAnonymizer class is a Python class designed to anonymize sensitive data in a Pandas DataFrame. The class provides various methods to identify and mark rows that need to be redacted or suppressed based on different rules and thresholds.

## Installation
No additional installation required. Make sure you have Pandas installed in your Python environment:

```bash
pip install pandas
```
## Usage
Import the class and initialize it with a Pandas DataFrame and optionally a list of sensitive columns.

```python
Copy code
from DataAnonymizer import DataAnonymizer

# Initialize with DataFrame and sensitive columns
anonymizer = DataAnonymizer(df, sensitive_columns=['Name', 'Email'])
```
## Methods
### __init__(self, df, sensitive_columns=None)
Initializes the DataAnonymizer object.

* df: The Pandas DataFrame to be anonymized.
* sensitive_columns: Optional list of columns considered sensitive.
### less_than_threshold_not_zero(self, frequency, minimum_threshold=10)
Marks rows where the value in the specified frequency column is less than minimum_threshold but not zero.

* frequency: The column name to check frequency against.
* minimum_threshold: The minimum threshold below which data should be flagged for redaction.
### sum_redact(self, frequency, organization_columns=None, minimum_threshold=10)
Groups the DataFrame by organization_columns and marks rows for secondary suppression based on their summed frequency.

* frequency: The column name to check frequency against.
* organization_columns: Optional columns to group by.
* minimum_threshold: The minimum threshold for the summed frequency.
### one_count_redacted(self, frequency, organization_columns=None, minimum_threshold=10)
Similar to sum_redact, but marks rows for secondary suppression based on a single occurrence in the grouping.

### redact_threshold(self, frequency, organization_columns=None, minimum_threshold=10)
Runs less_than_threshold_not_zero and then marks rows for secondary suppression based on the number of overlapping sensitive columns.


```python
# Initialize the anonymizer
anonymizer = DataAnonymizer(df, sensitive_columns=['Name', 'Email'])

# Run redaction methods
anonymizer.less_than_threshold_not_zero('Salary', 50000)
anonymizer.sum_redact('Salary', organization_columns=['Department'])
anonymizer.one_count_redacted('Salary')
anonymizer.redact_threshold('Salary')

# Get the anonymized DataFrame
df_anonymized = anonymizer.df
```

## Known Issues
The code snippet contains some syntactical errors and unfinished logic for L-Diversity which might lead to unexpected results.

## Future Enhancements
1. Improved error handling.
2. Addition of more anonymization techniques like T-Closeness.
## Author
For questions or further improvements, feel free to reach out.