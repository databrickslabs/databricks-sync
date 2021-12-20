from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setup(
    name="databricks-sync",
    version="1.0.0",
    author="Itai Weiss",
    author_email="itai@databricks.com",
    description="Databricks Sync CLI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/databricks-sync",
    license="https://github.com/databrickslabs/databricks-sync/blob/master/LICENSE",
    packages=find_packages(exclude=['tests', 'tests.*', ]),
    install_requires=[
        'PyYAML==5.4',
        'requests>=2.17.3',
        'click>=6.7',
        'click-log==0.3.2',
        'databricks-cli==0.11.0',
        'gitpython==3.1.11',
        'streamz==0.5.5',
        'tenacity==6.2.0',
        'dotty_dict==1.3.0',
        'pygrok==1.0.0',
        'pandas==1.1.1',
        'SQLAlchemy==1.3.22',
        'styleframe==3.0.6',
        'dask==2021.10.0',
        'distributed==2.23.0',
        'setuptools==45'
    ],
    package_data={'': ['export.yaml']},
    entry_points='''
        [console_scripts]
        databricks-sync=databricks_sync.cmds.cli:cli
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
