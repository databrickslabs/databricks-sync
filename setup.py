import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="databricks-terraformer",
    version="1.0.0",
    author="Itai Weiss",
    author_email="itai@databricks.com",
    description="Databricks Terraformer CLI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/databricks-terraformer",
    license="https://github.com/databrickslabs/databricks-terraformer/blob/master/LICENSE",
    packages=setuptools.find_packages(exclude=['tests', 'tests.*',]),
    install_requires=[
          'requests>=2.17.3',
          'click>=6.7',
          'click-log==0.3.2',
          'databricks-cli==0.11.0',
          'gitpython',
          'streamz==0.5.5',
          'tenacity'
      ],
    package_data={
        'databricks_terraformer': ['sdk/hcl/json2hcl.so', 'utils/changelog.md.j2', 'sdk/hcl/hcl.tf.j2'],
    },
    entry_points='''
        [console_scripts]
        databricks-terraformer=databricks_terraformer.cmds.cli:cli
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
