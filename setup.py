"""Setup configuration for Petrinex Python API"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="petrinex",
    version="1.1.1",
    author="Guanjie Shen",
    description="Load Alberta Petrinex data (Volumetrics, NGL) into Spark/pandas DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/guanjieshen/petrinex-python-api",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
        ],
    },
    keywords="petrinex alberta oil gas volumetrics ngl energy data spark databricks unity-catalog",
    project_urls={
        "Bug Reports": "https://github.com/guanjieshen/petrinex-python-api/issues",
        "Source": "https://github.com/guanjieshen/petrinex-python-api",
        "Documentation": "https://github.com/guanjieshen/petrinex-python-api#readme",
    },
)
