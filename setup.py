from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="mysql_helpers",
    version="0.0.1.8",
    author="Nono London",
    author_email="",
    description="MySQL Connection Helper in Sync mode",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nono-london/mysql_helpers",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["mysql-connector-python", "python-dotenv", "pandas"],
    tests_require=["pytest", "pytest-asyncio"],
    python_requires=">=3.9",
)
