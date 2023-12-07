import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mysql_helpers",
    version="0.0.1.0",
    author="Nono London",
    author_email="",
    description="MySQL Connection Helper in Sync mode",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nono-london/mysql_helpers",
    packages=["mysql_helpers"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["mysql-connector-python", "python-dotenv", "pandas"],
    tests_require=["pytest"],
    python_requires='>=3.9',
)
