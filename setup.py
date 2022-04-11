from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

import sys

if sys.version_info < (3, 6):
    sys.exit('Sorry, Python < 3.6 is not supported')

python_versioned_libraries = ['pandas==1.4.1', 'styleframe==4.0.0']

if (3, 6) <= sys.version_info < (3, 8):
    python_versioned_libraries = ['pandas==1.1.1', 'styleframe==3.0.6']

setup(
    name="databricks-sync",
    author="Itai Weiss",
    author_email="itai@databricks.com",
    description="Databricks Sync CLI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/databricks-sync",
    license="https://github.com/databrickslabs/databricks-sync/blob/master/LICENSE",
    packages=find_packages(exclude=['tests', 'tests.*', ]),
    use_scm_version={
        "local_scheme": "dirty-tag"
    },
    setup_requires=['setuptools_scm'],
    install_requires=[
        'PyYAML==5.4',
        'requests>=2.17.3',
        'click>=6.7',
        'click-log==0.3.2',
        'databricks-cli==0.11.0',
        'gitpython==3.1.11',
        'streamz==0.6.3',
        'tenacity==6.2.0',
        'dotty_dict==1.3.0',
        'pygrok==1.0.0',
        'SQLAlchemy==1.3.22',
        'dask==2021.10.0',
        'distributed==2021.10.0',
        'setuptools==45'
    ] + python_versioned_libraries,
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
