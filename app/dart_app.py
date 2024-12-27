#without this first two lines not finding my module
import os
import sys
import streamlit as st
from app.constants import NONE_RANGE_COL_ONE_TEXT, NONE_RANGE_COL_TWO_TEXT, NONE_RANGE_COL_THREE_TEXT, RANGE_COL_ONE_TEXT, RANGE_COL_TWO_TEXT, RANGE_COL_THREE_TEXT



import pandas as pd

from dar_tool_utility.utility import  process_multiple_frequency_col
from dar_tool.suppression_check import DataAnonymizer,RangeSuppressionModel

st.set_page_config(
    layout="wide",
    page_title="DART User Interface",
    page_icon="images/favicon.ico",
    menu_items={
        "Report a bug": "https://github.com/P20WCommunityOfInnovation/DAR-T/issues",
        "About": "This tool is developed by the P20W+ Community of Innovation to support users with applying redaction to aggregate data in order to avoid disclosure of sensitive information. More information can be found here: https://github.com/P20WCommunityOfInnovation/DAR-T"
    }
)


DF_MERGED_SESSION_KEY = 'df_merged'
ACTIVE_RANGE_SESSION_KEY = "active_range_select"
SELECT_RANGE = "Range Selection"
SELECT_NON_RANGE = "None Range Selection"
# Add columns for centering image
left, middle, right = st.columns(3)

with middle:
    st.image(image="images/DAR-T_main_text.png", width = 1000)

st.header("This tool is designed to support users with redacting sensitive records in aggregate files. By default this tool will redact records where the count is 10 or less and all additional records needed for complimentary suppression.")

st.subheader("Upload your .csv or .xlsx file with aggregates to get started:")

#Creating columns so file upload widget does not span entire page. 
filecol,unusedcol = st.columns([.3,.7])

if DF_MERGED_SESSION_KEY not in st.session_state:
    st.session_state[DF_MERGED_SESSION_KEY] = None
    
with filecol:
    uploadedFile = st.file_uploader("Upload file", type=['csv','xlsx'],accept_multiple_files=False,key="fileUploader")

if uploadedFile is None:
    st.session_state[DF_MERGED_SESSION_KEY] = None
    
if uploadedFile:
    if uploadedFile.name.endswith('.csv'):
        df = pd.read_csv(uploadedFile)

    elif uploadedFile.name.endswith('.xlsx'):
        df = pd.read_excel(uploadedFile)
    else:
        raise Exception("Your uploaded file must be a .csv or .xlsx")
    
    st.session_state[DF_MERGED_SESSION_KEY] = None
    #Create sidebar for user to specify inupts to redaction function
    with st.sidebar:
        st.sidebar.title("Inputs for Redaction")
        #st.header("Select your function inputs")
        tabs = ["Range Selection","None Range Selection"]
        active_tab = st.radio("Select your function inputs",tabs, key=ACTIVE_RANGE_SESSION_KEY)
        # Initialize session state for active tab
        if ACTIVE_RANGE_SESSION_KEY not in st.session_state:
            st.session_state[ACTIVE_RANGE_SESSION_KEY] = SELECT_RANGE
                
        if st.session_state[ACTIVE_RANGE_SESSION_KEY] == SELECT_RANGE:

            total_col = st.selectbox("Total count", options= [None] + list(df.columns))
            sub_col = st.selectbox("Sub total count", options= [None] + list(df.columns))
            rate_col = st.selectbox("Target Rate Column", options= [None] + list(df.columns))
            threshold_val = st.number_input('Threshold percentage', value= 3, min_value= 1)
            range_select_btn = st.button("Redact my dataset",key="range_selection")
            if range_select_btn:
                st.session_state[DF_MERGED_SESSION_KEY] = None
                range_supp = RangeSuppressionModel(total_col,sub_col,rate_col,threshold_val)
                anonymizer = DataAnonymizer(df,range_suppression_model=range_supp)
                st.session_state[DF_MERGED_SESSION_KEY] = anonymizer.apply_anonymization()
       
        if st.session_state[ACTIVE_RANGE_SESSION_KEY] == SELECT_NON_RANGE:

            parent_org = st.selectbox("Parent Organization", options= [None] + list(df.columns))
            parent_org = None if parent_org == 'None' else parent_org
            
            child_org = st.selectbox("Child Organization", options= [None] + list(df.columns))
            child_org = None if child_org == 'None' else child_org
            
            sensitive_columns = st.multiselect("Sensitive Columns", options= df.columns)
            st.caption("At least one sensitive column must be specified.")
            
            frequency_columns = st.multiselect("Aggregate Count Column", options=df.columns)
            
            redact_column = st.selectbox("User Specified Redaction Column", options= [None] + list(df.columns))
            redact_column = None if redact_column == 'None' else redact_column
            
            minimum_threshold = st.number_input('Specify the minimum threshold for supression', value= 10, min_value= 0)
            redact_zero = st.checkbox('Should zeroes be redacted?')

            overwrite = st.checkbox("Do you want to overwrite redacted values with a specific string? E.g., 'Suppressed'")
            if overwrite:
                redact_value = st.text_input("What string should replace redacted values?")
                st.caption("If you do not specify a value, columns will be marked for redaction but original counts will still be shown.")
                st.caption("Leaving the box checked but no value specified will overwrite values with a blank.")            

            try:
                redact_value = redact_value #Just assigning this to itself so it doesn't print to the streamlit app. 
            except NameError:
                redact_value = None

            if st.button("Redact my dataset",key="not_range_selection"):

                st.session_state[DF_MERGED_SESSION_KEY] = None
                df_merged = process_multiple_frequency_col(df, parent_organization=parent_org, child_organization=child_org,
                                            sensitive_columns=sensitive_columns, frequency_columns=frequency_columns,
                                            minimum_threshold=minimum_threshold, redact_column=redact_column,
                                            redact_zero=redact_zero, redact_value=redact_value)
                st.session_state[DF_MERGED_SESSION_KEY] = df_merged

 
    #Creating layout for 
    st.header("Un-redacted file")
    st.subheader("Verify accuracy of the file before continuing.")

    st.write(df)

    st.subheader("Once you have verified your input file, select the appropriate columns in the file as inputs to the function in the sidebar. An explanation of each option is included below.")

    #Create columns for organizing explainer text
    col1, col2, col3= st.columns([1.5,1,1.5])

    col_value = []
    if st.session_state[ACTIVE_RANGE_SESSION_KEY] == SELECT_RANGE:
        col_value = [RANGE_COL_ONE_TEXT,RANGE_COL_TWO_TEXT,RANGE_COL_THREE_TEXT]  
    if st.session_state[ACTIVE_RANGE_SESSION_KEY] == SELECT_NON_RANGE:
        col_value = [NONE_RANGE_COL_ONE_TEXT,SELECT_RANGE,NONE_RANGE_COL_THREE_TEXT]
         
    
    with col1:
        if st.session_state[ACTIVE_RANGE_SESSION_KEY] == SELECT_RANGE:
            st.image("images/DAR-T-Range-example.png", caption="Example columns for the range selection are as follows:",width=600)
        st.markdown(col_value[0])
    with col2:
        st.markdown(col_value[2])
    with col3:
        st.markdown(col_value[2])

    st.subheader("Click 'Redact my dataset' in the sidebar when you are ready to redact your file.")
    
    if st.session_state[DF_MERGED_SESSION_KEY] is not None:
        st.header("Redacted File")
        st.subheader("The file can be downloaded via the download icon in the top right of the table.")
        st.write(st.session_state[DF_MERGED_SESSION_KEY])

       



    
    


    