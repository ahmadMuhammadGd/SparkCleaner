from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

VERSION = '0.0.1' 
DESCRIPTION = 'spark dataframe cleaner'
LONG_DESCRIPTION = readme

setup(
        name="SparkCleaner", 
        version=VERSION,
        author="Ahmad Muhammad",
        author_email="ahmadmuhammadgd@gmail.com",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[],
        keywords=['python', 'spark', 'data cleaning'],
        classifiers= [
            "Development Status :: 1 - Planning",
            "Intended Audience :: Information Technology",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
            "Operating System :: POSIX :: Linux"
        ]
)