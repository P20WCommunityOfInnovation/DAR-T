<p align="center">
  <img src="assets/images/DAR-T_main_text.png" alt="DART Logo" width = "30%">
</p>

## Overview
The Disclosure Avoidance Redaction Tool (DAR-T) is designed to ensure the anonymization of sensitive information within a DataFrame. Tailored for environments where data privacy and confidentiality are of utmost importance, encompasses a variety of methods to redact sensitive data. These methods are meticulously designed to cater to specific criteria, including frequency of data, predefined minimum thresholds, and records earmarked for redaction as per user requests. 

## Requirements
Python Version:
* Python 3.10

Packages: 
* Pandas
* itertools
* logging

# Where to get it
The source code is currently hosted on GitHub at: https://github.com/P20WCommunityOfInnovation/DAR-T

Binary installers for the latest released version are available at the Python Package Index (PyPI).

```bash
#PyPI
pip install dar-tool
```

# Launching the Dart User Interface Streamlit App 
DAR-T also provides a Streamlit app for users which prefer interacting with a user interface. 

## Launching Via Docker
Docker provides a pre-packaged container of all requirements and dependencies, so users do not need to install Python or packages. 

As a prerequisite you must have [Docker installed](https://docs.docker.com/engine/install/) on your machine. 

To run the Docker image, you must first pull it from Docker Hub using the following command:

```bash
docker pull p20wcommunityofinnovation/dar-t:latest
```
If desired, you may pull a specific release of DAR-T by using the following command:

```bash
docker pull p20wcommunityofinnovation/dar-t:<release-tag>
```
Then run:

```bash
docker run -p 8501:8501 dart_ui
```
You may also run the Docker image via the Docker Desktop UI. Ensure that port 8501 is specified as the mapped port on your host machine in the optional run settings. 
## Launching Locally Via Command Line

To launch the Streamlit application using your locally installed Python version, navigate to the directory (app directory) containing app.py and run the following commands:

```bash
pip install -r requirements.txt
streamlit run dart_app.py
```

## Modules and Functions
### Import Statements
The script uses:

Pandas for data manipulation (pd)

### DataAnonymizer Class
#### __init__(df, parent_organization=None, child_organization=None, sensitive_columns=None, frequency=None, redact_column=None, minimum_threshold=10, redact_zero=False, redact_value=None)
`df`: Initializes the DataAnonymizer object with a data frame df.

`parent_organization`: Parent organization column name.

`child_organization`: Child organization column name.

`sensitive_columns`: Columns that contain sensitive data.

`frequency`: Column indicating frequency/count of data.

`redact_column`: Column to apply redaction.

`minimum_threshold`: The minimum threshold for redaction.

`redact_zero`: User can choose to redact zero or not.

`redact_value`: User can select a replacement for redacted values in the frequency column. 

#### create_log()

Generates a log of data groups based on sensitive columns and thresholds. The create log method aggregates data, applies minimum threshold checks, and prepares a detailed log for further redaction steps.

##### Grouping
Puts each aggregate level in a numeric value so that the rest of the dataset can be analyzed without overlapping the value sets in other columns.

#### MinimumValue + `Sensitive Column Name`
Find the lowest value of that sensitive column that is not redacted and generates a number to be used going forward.

#### RedactBinary
Binary Value representing whether the data is or is not redacted

#### Redact
Signals whether the dataset is not redacted, has primary suppression, or secondary suppression is applied.

`Primary Suppression` - Occurs when the value is less than the given threshold and is not zero

`Secondary Suppression` - Occurs when the value is suppressed in primary suppression and needs this value to be covered so the other is not represented.

#### Redact Breakdown
Represents the steps taken to properly redact the information within the data set. 

`Not Redacted` - Value was not redacted.

`Less Than 10 and not zero` - The value was less than the threshold, and the value is not zero.

`Sum of values less than threshold` - The sum of values in the set was less than the redacted threshold, so this value needed to be redacted.

`One count redacted leading to secondary suppression` - Only one value in the set was redacted, so this value needed to be redacted.

`Redacting zeroes or other remaining values missed in one count function` - If the redaction in the `One count redacted leading to secondary suppression` leads to values still being represented through counter mathematics.

`Redacting based on aggregate level redaction` - Using the aggregates of the sensitive columns or the organization columns leads to the suppression being applied on the original counts. 

#### data_logger(filter_value, redact_name, redact_breakdown_name)

Designed to update a logging DataFrame (df_log) within a class based on specific conditions. This method is particularly useful for managing and documenting changes in data access or visibility controls, especially in scenarios where data redaction is necessary for privacy or security reasons. By systematically updating the DataFrame, the data_logger method aids in maintaining a clear audit trail of redactions and their justifications within the dataset.

#### redact_user_requested_records()

Specifically redacts records as per user requests. If a record is marked for redaction in the 'UserRedact' column with a `1`, the redact user requested records method will update its status to reflect user-requested redaction.

`filter_value`: This parameter is used to filter or select rows in the DataFrame where updates will be applied.
`redact_name`: A string value that will be assigned to the 'Redact' column of the DataFrame for rows matching the filter_value.
`redact_breakdown_name`: A string that will be appended to the 'RedactBreakdown' column for rows matching the filter_value.

#### less_than_threshold()

Applies primary suppression to records where the frequency is less than or equal to the specified minimum threshold and also zero depending on the user input. The less than threshold updates these records to indicate they have been redacted due to failing to meet the minimum threshold criteria.

#### sum_redact()

Redacts records based on the sum of their frequencies. If the total sum of a group of records is less than the minimum threshold, those records are redacted for additional privacy protection.

#### one_count_redacted()

Targets and redacts individual records in a group if they are the only ones left after other redactions. The one count redacted method is a form of secondary suppression to prevent singling out individual data points.
#### one_redact_zero()

Specifically focuses on if one single data point is redacted then redact the zero. The one redact zero method ensures that these unique cases are handled appropriately to maintain data anonymity.

#### cross_suppression()

Implements a more complex form of redaction where records are suppressed based on cross-referencing between parent and child organizations. The cross suppression method is designed to handle cases where sensitive data might be indirectly exposed through relationships between different data groups.
#### apply_log()

Apply log method finalizes the redaction process by merging the redaction log with the original data. The apply log method ensures that all redaction rules are applied consistently across the dataset.
#### apply_anonymization()

The central method that orchestrates the entire data anonymization process. Apply anonymization sequentially calls other methods in the class to apply a comprehensive anonymization strategy, resulting in a fully redacted and anonymized dataset.

#### get_log()

Outputs log in a dataframe for the user to access at any point. The main point is to be able to retrieve the log if an error occurs while running another method.

### Example Usage
Here is a quick example:

```python
import pandas as pd
from dar_tool import DataAnonymizer

# Sample DataFrame
data = {
    'Parent_Organization': ['Parent1', 'Parent1', 'Parent2', 'Parent2', 'Parent2', 'Parent3'],
    'Organization': ['Org1', 'Org2', 'Org3', 'Org4', 'Org5', 'Org6'],
    'Gender': ['F', 'M', 'F', 'M', 'F', 'M'],
    'Count': [20, 0, 13, 5, 25, 9]
}

df = pd.DataFrame(data)

# Instantiate the anonymizer
anonymizer = DataAnonymizer(df, parent_organization='Parent_Organization', child_organization='Organization', sensitive_columns='Gender', frequency='Count')

# Apply anonymization
df_anonymized = anonymizer.apply_anonymization()

# Output to CSV
df_anonymized.to_csv('AnonymizedData.csv', index=False)
```

## Known Issues
### Performance with Large Datasets
The script may experience performance degradation when processing extremely large datasets, leading to longer execution times. We plan to utilize Spark in the future to better handle large datasets. 

### Handling of Null Values
Current implementation might not optimally handle null or missing values in the dataset, which could affect the accuracy of the anonymization process.

### Limited Support for Non-Numeric Data
The script may have limited functionality when dealing with non-numeric or categorical data, potentially leading to less effective anonymization for such data types.

### Inconsistent Redaction Across Similar Values
There might be instances where similar values are not consistently redacted, leading to potential risks of data re-identification.

### Complex Nested Data Structure
The current version may struggle with complex nested data structures, like JSON objects or XML trees, potentially leading to incomplete anonymization.

### Overlapping Data Sensitivity
In cases where data sensitivity overlaps between parent and child organizations, the script might not optimally redact the overlapping sensitive data.

### Balance Between Anonymization and Data Utility
Striking the right balance between data anonymization and retaining the utility of the data for analysis purposes can be challenging.

### Unicode and Special Characters Handling
The script might face issues in correctly processing text fields containing Unicode or special characters.

### Limited Customization in Redaction Methods
As of now, the customization options for different redaction methods are limited, which may not suit all types of datasets.

### Dependency on External Libraries
The script's reliance on external libraries like Pandas may pose compatibility issues with different versions or in environments where these libraries are not available.

## Future Enhancements
### Dynamic Threshold Adjustment
Implement an algorithm that dynamically adjusts the minimum threshold based on the dataset's size and diversity. Dynamic Threshold Adjustment would ensure more efficient and context-sensitive anonymization.

### Enhanced Pattern Recognition
Develop functionality to identify and redact indirect identifiers or patterns that could lead to data re-identification, such as unique combinations of non-sensitive attributes.

### User Interface for Parameter Configuration
Create a graphical user interface (GUI) that allows users to easily configure anonymization parameters without editing the code directly.

### Automated Data Quality Checks
Integrate automated checks to ensure data quality and consistency post-anonymization, helping to maintain the integrity and usability of the data.

### Support for More Data Types
Extend the class to handle a wider range of data types, such as geospatial data, timestamps, and complex data structures, for more comprehensive anonymization.

### Parallel Processing for Large Datasets
Implement parallel processing capabilities to handle large datasets more efficiently, reducing the time required for anonymization.

### Customizable Redaction Techniques
Allow users to define custom redaction techniques, such as hashing or encryption, for specific columns or data types.

### Machine Learning Integration
Leverage machine learning algorithms to identify and protect potential indirect identifiers, enhancing the effectiveness of the anonymization process.

### API and Integration Support
Develop an API for the DataAnonymizer class, allowing it to be easily integrated with other software applications or data processing pipelines.

### Continuous Learning and Improvement
Implement a feedback mechanism where the tool learns from its anonymization decisions and improves over time, especially in handling complex or ambiguous cases.

### Enhanced Security Features
Add features to ensure the security of the data during the anonymization process, such as secure memory handling and protection against data leaks.


## Contributors
Drew Bennett-Stein

Nathan Clinton

Garrett Amstutz

Seth Taylor

Didi Miles

Stephanie Straus

Devin Kammerdeiner
