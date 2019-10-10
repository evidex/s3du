from setuptools import setup, find_packages

LONGDESC = """Reads the list of files from Amazon S3 and sends it to the ncdu
utility for interactive browsing.  Makes it easy to quickly find out what takes
most storage.
"""

setup(
    name='s3du',
    version='0.2',
    description='Inspect Amazon S3 buckets interactively with ncdu',
    author='Justin Forest',
    author_email='hex@umonkey.net',
    url='http://github.com/umonkey/s3du',
    packages=find_packages(),
    install_requires=['boto3'],
    long_description=LONGDESC,
    license='GNU GPL',

    entry_points={
        'console_scripts': [
            's3du = s3du.main:main'
        ]
    },

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Systems Administration',
        'Topic :: Utilities'
    ]
)