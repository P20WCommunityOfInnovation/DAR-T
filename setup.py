from setuptools import setup, find_packages

setup(
    name='dar-tool',
    version='1.0.5',
    author='P20W+ Community of Innovation',
    author_email='info@communityofinnovation.org',
    description='A Python package for anonymizing sensitive data in pandas DataFrames.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown', 
    url='https://github.com/P20WCommunityOfInnovation/DisclosureAvoidance', 
    packages=find_packages(), 
    install_requires=[
        'pandas>=1.0.0'
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Information Technology',
        
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 3'
    ],
    python_requires='>=3.10',
    keywords='data anonymization, privacy, pandas', 
)