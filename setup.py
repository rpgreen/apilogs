import sys

from setuptools import setup, find_packages

install_requires = [
    'boto3>=1.2.1',
    'termcolor>=1.1.0',
    'python-dateutil>=2.4.0'
]

# as of Python >= 2.7 argparse module is maintained within Python.
if sys.version_info < (2, 7):
    install_requires.append('argparse>=1.1.0')

setup(
    name='apilogs',
    version='1.1',
    url='http://github.com/rpgreen/apilogs',
    license='BSD',
    author='Ryan Green',
    author_email='ryang@ryang.ca',
    description='Easy logging and debugging for Amazon API Gateway and AWS Lambda Serverless APIs',
    long_description='Easy logging and debugging for Amazon API Gateway and AWS Lambda Serverless APIs',
    keywords="aws logs cloudwatch api gateway lambda serverless logging",
    packages=find_packages(),
    platforms='any',
    install_requires=install_requires,
    test_suite='tests',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2',
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Utilities'
    ],
    entry_points={
        'console_scripts': [
            'apilogs = apilogs.bin:main',
        ]
    },
    zip_safe=False
)
