from setuptools import setup, find_packages

setup(
	author="Matt Triano",
	description="A package of tools for working with my personal data warehouse.",
	name="pgutils",
	version="0.1.0",
	packages=find_packages(include=["pgutils", "pgutils.*"]),
)