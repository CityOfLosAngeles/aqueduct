from setuptools import find_packages, setup

setup(
    name="civis-aqueduct-utils",
    version="0.1.0",
    packages=find_packages(),
    package_dir={"civis-aqueduct-utils": "civis-aqueduct-utils"},
    include_package_data=True,
    install_requires=["civis"],
    data_files=[
        (
            "share/jupyter/nbextensions/civis-aqueduct-utils",
            ["civis_aqueduct_utils/static/extension.js"],
        ),
    ],
    entry_points={
        "console_scripts": ["civis-service = civis_aqueduct_utils.share:main"]
    },
)
