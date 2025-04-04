#without this first two lines not finding my module
import sys
sys.path.append('..')

import streamlit as st

import pandas as pd

from dar_tool.suppression_check import DataAnonymizer

st.set_page_config(
    layout="wide",
    page_title="DART User Interface",
    page_icon="images/favicon.ico",
    menu_items={
        "Report a bug": "https://github.com/P20WCommunityOfInnovation/DAR-T/issues",
        "About": "This tool is developed by the P20W+ Community of Innovation to support users with applying redaction to aggregate data in order to avoid disclosure of sensitive information. More information can be found here: https://github.com/P20WCommunityOfInnovation/DAR-T"
    }
)

# Add columns for centering image
left, middle, right = st.columns(3)

with middle:
    st.image(image="images/DAR-T_main_text.png", width = 300)

st.header("This tool is designed to support users with redacting sensitive records in aggregate files. By default this tool will redact records where the count is 10 or less and all additional records needed for complimentary suppression.")

st.subheader("Upload your .csv or .xlsx file with aggregates to get started:")

#Creating columns so file upload widget does not span entire page. 
filecol,unusedcol = st.columns([.3,.7])
with filecol:
    uploadedFile = st.file_uploader("Upload file", type=['csv','xlsx'],accept_multiple_files=False,key="fileUploader")

if uploadedFile:
    if uploadedFile.name.endswith('.csv'):
        df = pd.read_csv(uploadedFile)

    elif uploadedFile.name.endswith('.xlsx'):
        df = pd.read_excel(uploadedFile)
    else:
        raise Exception("Your uploaded file must be a .csv or .xlsx")
    
    
    #Create sidebar for user to specify inupts to redaction function
    with st.sidebar:
        st.sidebar.title("Inputs for Redaction")
        st.header("Select your function inputs")
        parent_org = st.selectbox("Parent Organization", options= [None] + list(df.columns))

        child_org = st.selectbox("Child Organization", options= [None] + list(df.columns))

        sensitive_columns = st.multiselect("Sensitive Columns", options= df.columns)

        st.caption("At least one sensitive column must be specified.")

        frequency_columns = st.multiselect("Aggregate Count Column", options=df.columns)

        redact_column = st.selectbox("User Specified Redaction Column", options= [None] + list(df.columns))

        minimum_threshold = st.number_input('Specify the minimum threshold for suppression', value= 10, min_value= 0)

        redact_zero = st.checkbox('Should zeroes be redacted?')

        overwrite = st.checkbox("Do you want to overwrite redacted values with a specific string? E.g., 'Suppressed'")
        if overwrite:
            redact_value = st.text_input("What string should replace redacted values?")
            st.caption("If you do not specify a value, columns will be marked for redaction but original counts will still be shown.")
            st.caption("Leaving the box checked but no value specified will overwrite values with a blank.")

 
    #Creating layout for 
    st.header("Un-redacted file")
    st.subheader("Verify accuracy of the file before continuing.")

    st.write(df)

    st.subheader("Once you have verified your input file, select the appropriate columns in the file as inputs to the function in the sidebar. An explanation of each option is included below.")

    #Create columns for organizing explainer text
    col1, col2, col3 = st.columns([1.5,1,1.5])

    with col1:
        st.markdown(
            """
            #### Grouping Columns

            **Parent Organization**: The top level grouping for your aggregates. This may be a category like "School District". This is optional. 

            **Child Organization**: A subgroup for your aggregates. This may be a category like "School". This is optional. 

            **Sensitive Columns**: Columns that group individual records into an aggregate. This would be a category like "Gender" or "IEP Status". At least one column must be specified. 

            *While parent organization and child organization are optional, you may not have duplicate values across your grouping columns. E.g., You cannot have more than one record for "English Language Learner - Male".*  
              
            *If you do have duplicates, you should specify a child organization or parent organization column to disaggregate these values. E.g., School 1 - English Language Learner - Male, School 2 - English Language Learner - Male*
            """)
    with col2:
        st.markdown(
            """
            #### Count Column

            **Aggregate Count Colum**: The column that contains your aggregate counts to be suppressed. 
            """)
    with col3:
        st.markdown(
            """
            #### Additional User Inputs

            **User Specified Redaction Column**: An optional column where the user may pre-specify which columns they want redaction applied to. The user specififed redaction column should only contain 1s and 0s, where 1 is a row that should have redaction applied. 

            **Minimum Threshold**: The minimum threshold for redaction. This is 10 by default and does not include zero. All values 10 and below, except zero, will be redacted. 

            **Should zeroes be redacted?**: If checked, zeroes will be redacted in addition to values at or below the minimum threshold. 

            **What string should replace redacted values?**: An optional string value you can specify what value will overwrite redacted counts. 
            """
        )

    st.subheader("Click 'Redact my dataset' in the sidebar when you are ready to redact your file.")

    #Format user provided values as needed
    if parent_org == 'None':
        parent_org = None

    if child_org == 'None':
        child_org= None

    if redact_column == 'None':
        redact_column = None

    try:
        redact_value = redact_value #Just assigning this to itself so it doesn't print to the streamlit app. 
    except NameError:
        redact_value = None

#Add button to apply redaction
    
    if st.sidebar.button("Redact my dataset"):

        anonymizer = DataAnonymizer(df, parent_organization=parent_org, child_organization=child_org,
                                    sensitive_columns=sensitive_columns, 
                                    minimum_threshold=minimum_threshold, redact_column=redact_column,
                                    redact_zero=redact_zero, redact_value=redact_value)
        df_merged = anonymizer.process_multiple_frequency_col(frequency_columns)

        st.header("Redacted File")
        st.subheader("The file can be downloaded via the download icon in the top right of the table.")
        st.write(df_merged)

       



    
    


    