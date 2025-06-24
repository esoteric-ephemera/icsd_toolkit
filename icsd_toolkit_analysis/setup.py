import os

from setuptools import find_namespace_packages, setup

SETUP_PTH = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(SETUP_PTH, "README.md")) as f:
    desc = f.read()


setup(
    name="kapstick",
    packages=find_namespace_packages(include=["src.kapstick.*"]),
    version="0.0.1",
    install_requires=["pymongo", "numpy"],
    extras_require={
        "dev": [
            "pytest==7.1.2",
            "pytest-cov==3.0.0",
            "coverage==6.2",
            "mypy==0.950",
            "ruff",
        ]
    },
    package_data={},
    python_requires=">=3.9",
    author="Aaron Kaplan",
    author_email="aaron.kaplan.physics [@] gmail.com",
    license="BSD",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Development Status :: 4 - Beta",
    ],
)
