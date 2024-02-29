from setuptools import setup, find_packages

setup(
    name='data_anonymizer',
    version='0.1.0',
    author='DAFne',
    author_email='drew.bennett@vermont.gpv',
    description='A Python package for anonymizing sensitive data in pandas DataFrames.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown', 
    url='https://github.com/P20WCommunityOfInnovation/DisclosureAvoidance', 
    packages=find_packages(), 
    install_requires=[
        'pandas>=1.0.0'
    ],
    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        'Intended Audience :: State and Federal United States Education Agencies ',
        
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: OSI Approved',

        'Programming Language :: Python :: 3'
    ],
    python_requires='>=3.11',
    keywords='data anonymization, privacy, pandas', 
)