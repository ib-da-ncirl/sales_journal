import os
import setuptools

_here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

version = {}
with open(os.path.join(_here, 'sales_journal', 'version.py')) as f:
    exec(f.read(), version)

setuptools.setup(
    name="dap_project",
    version=version['__version__'],
    author="Ian Buttimer & Philippe Tap",
    author_email="author@example.com",
    description="DAP project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ib-da-ncirl/sales_journal",
    license='MIT',
    packages=setuptools.find_packages(),
    install_requires=[
      'psycopg2>=2.8.4',
      'pymongo>=3.9.0',
      'dagster>=0.6.4',
      'dagit>=0.6.4',
      'dagster_pandas>=0.6.4',
      'pandas>=0.25.3'
    ],
    dependency_links=[
        'git+https://github.com/ib-da-ncirl/db_toolkit.git#egg=db_toolkit',
        'git+https://github.com/ib-da-ncirl/dagster_toolkit.git#egg=dagster_toolkit',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
