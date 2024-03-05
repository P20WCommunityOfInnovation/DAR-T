import streamlit as st
from dar_tool import DataAnonymizer
import pandas as pd

st.set_page_config(
    layout="wide",
    page_title="DART User Interface",
    page_icon="images/favicon.ico",
    menu_items={
        "Report a bug": "https://github.com/P20WCommunityOfInnovation/DAR-T/issues",
        "About": "This tool is developed by the P20W+ Community of Innovation to support users with applying redaction to aggregate data in order to avoid disclosure of sensitive information. More information can be found here: https://github.com/P20WCommunityOfInnovation/DAR-T"
    }
)

# Add columsn for center
st.image(image="images/DAR-T_main_text.png", width=500)

st.header("This tool is designed to support users with redacting sensitive records in aggregate files. By default this tool will redact records where the count is 10 or less and all additional records needed for complimentary suppression.")

st.subheader("Upload your .csv or .xlsx file with aggregates to get started:")
uploadedFile = st.file_uploader("Upload file", type=['csv','xlsx'],accept_multiple_files=False,key="fileUploader")

if uploadedFile:
    if uploadedFile.name.endswith('.csv'):
        df = pd.read_csv(uploadedFile)

    elif uploadedFile.name.endswith('.xlsx'):
        df = pd.read_excel(uploadedFile)
    else:
        raise Exception("Your uploaded file must be a .csv or .xlsx")
    
    st.header("Un-redacted file")
    st.subheader("Verify accuracy of the file before continuing.")
    st.write(df)

    
    with st.sidebar:
        st.sidebar.title("Inputs for Redaction")
        st.header("Select your function inputs")
        parent_org = st.selectbox("Parent Organization", options= [None] + list(df.columns))

        child_org = st.selectbox("Child Organization", options= [None] + list(df.columns))

        sensitive_columns = st.multiselect("Sensitive Columns", options= df.columns)

        st.caption("At least one sensitive column must be specified.")

        frequency = st.selectbox("Aggregate Count Column", options=df.columns)

        redact_column = st.selectbox("User Specified Redaction Column", options= [None] + list(df.columns))

        minimum_threshold = st.number_input('Specify the minimum threshold for supression', value= 10, min_value= 0)

        redact_zero = st.checkbox('Should zeroes be redacted?')

        overwrite = st.checkbox("Do you want to overwrite redacted values with a specific string? E.g., 'Suppressed'")
        if overwrite:
            redact_value = st.text_input("What string should replace redacted values?")
            st.caption("If you do not specify a value, columns will be marked for redaction but original counts will still be shown.")
            st.caption("Leaving the box checked but no value specified will overwrite values with a blank.")

#Format user provided values as needed
    if parent_org == 'None':
        parent_org = None

    if child_org == 'None':
        child_org= None

    if redact_column == 'None':
        redact_column = None

    try:
        redact_value = redact_value #Just assignign this to itself so it doesn't print to the streamlit app. 
    except NameError:
        redact_value = None

#Add button to apply redaction
    
    if st.button("Redact my dataset"):
        anonymizer = DataAnonymizer(df, parent_organization=parent_org, child_organization=child_org,sensitive_columns=sensitive_columns, frequency= frequency, redact_column=redact_column,redact_zero=redact_zero,redact_value= redact_value)
        df_redacted = anonymizer.apply_anonymization()


        st.header("Redacted File")
        st.subheader("The file can be downloaded via the download icon in the top right of the table.")
        st.write(df_redacted)


    
    


    