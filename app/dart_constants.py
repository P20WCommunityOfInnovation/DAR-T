import os
from dataclasses import dataclass


def safe_parse_int(val:str, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


@dataclass(frozen=True)
class AppDefaults:

    def __new__(cls, *args, **kwargs):
        raise TypeError(f"{cls.__name__} may not be instantiated.")

    """
    Class to hold default values for the app environment.
    """
    # Default values for the app environment
    PAGE_TITLE: str = os.getenv('PAGE_TITLE', "DART User Interface")
    HEADER_IMAGE:str = os.getenv('HEADER_IMAGE', 'images/DAR-T_main_text.png')

    HEADER_TEXT: str = os.getenv('HEADER_TEXT',"This tool is designed to support users with redacting sensitive records in aggregate files. By default this tool will redact records where the count is 10 or less and all additional records needed for complimentary suppression")

    MIN_SUPPRESSION_THRESHOLD: int = safe_parse_int(os.getenv('MIN_SUPPRESSION_THRESHOLD'), 10)
    DISABLE_STREAMLIT_HAMBURGER:bool = os.getenv('DISABLE_STREAMLIT_HAMBURGER', 'false').lower() in ('true', '1', 't')
    CUSTOM_TOP_NAVIGATION_HTML: str = os.getenv('CUSTOM_TOP_NAVIGATION_HTML', None)


def print_defaults():
    """
    Print the default values for the app environment.
    """
    print("Default values for the app environment:")
    print(f"PAGE_TITLE: {AppDefaults.PAGE_TITLE}")
    print(f"HEADER_IMAGE: {AppDefaults.HEADER_IMAGE}")
    print(f"HEADER_TEXT: {AppDefaults.HEADER_TEXT}")
    print(f"MIN_SUPPRESSION_THRESHOLD: {AppDefaults.MIN_SUPPRESSION_THRESHOLD}")
    print(f"DISABLE_STREAMLIT_HAMBURGER: {AppDefaults.DISABLE_STREAMLIT_HAMBURGER}")
    print(f"CUSTOM_HEADER_HTML: {AppDefaults.CUSTOM_TOP_NAVIGATION_HTML}")

