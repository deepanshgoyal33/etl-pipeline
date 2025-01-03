from setuptools import find_packages, setup

setup(
    name="etl_pipeline",
    packages=find_packages(exclude=["etl_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "faker",
        "kafka-python",
        "pandas",
        "pytz",
        "sqlalchemy",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "pandas"]},
)
