from setuptools import find_packages, setup

setup(
    name="nypd_analysis",
    packages=find_packages(exclude=["nypd_analysis_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "matplotlib",
        "seaborn",
        "pymongo",
        "psycopg2",
        "SQLAlchemy",
        "geopandas",
        "mapclassify"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
