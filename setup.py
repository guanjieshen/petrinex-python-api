"""Setup script for petrinex package."""

from setuptools import setup, find_packages

setup(
    name="petrinex",
    version="0.3.0",
    packages=find_packages(),
    python_requires=">=3.7",
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
        ],
    },
)

