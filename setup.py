from setuptools import setup, find_packages

setup(
    name='data_anonymizer',  # Replace with your chosen package name
    version='0.1.0',  # Start with a small version number and increment it with each release
    author='DAFne',  # Replace with your name or organization name
    author_email='drew.bennett@vermont.gpv',  # Replace with your email
    description='A Python package for anonymizing sensitive data in pandas DataFrames.',
    long_description=open('README.md').read(),  # Ensure you have a README.md file in your project
    long_description_content_type='text/markdown',  # This is important for PyPI to render Markdown
    url='https://github.com/P20WCommunityOfInnovation/DisclosureAvoidance',  # Replace with the URL to your repository
    packages=find_packages(),  # Automatically find and include all packages
    install_requires=[
        'pandas>=1.0.0',
        'logging>=1.0.0',
        'itertools>=1.0.0'
    ],
    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: State and Federal United Government ',
        'Topic :: Software Development :: Libraries :: Python Modules',

        # Pick your license as you wish
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3, or both.
        'Programming Language :: Python :: 3'
    ],
    python_requires='>=3.9',  # Specify the minimum Python version required
    keywords='data anonymization, privacy, pandas',  # Add keywords related to your package
    # You can specify package data and scripts here, and more. Refer to the setuptools documentation for details.
)