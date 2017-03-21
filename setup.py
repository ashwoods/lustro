import os
import codecs
from setuptools import setup


def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()


setup(
    name="lustro",
    version="0.0.1b",
    url='https://github.com/ashwoods/lustro',
    license='MIT',
    description="mirror an oracle database to a postgresql using sqlalchemy",
    long_description=read('README.rst'),
    author='Ashley Camba Garrido',
    author_email='ashwoods@gmail.com',
    entry_points={
        'console_scripts': [
            'lustro = lustro.cli:cli',
        ],
    },
    packages=['lustro'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Topic :: Utilities',
    ],
    install_requires=[
        'click',
        'click-log',
        'cx_Oracle',
        'Psycopg2',
	'sqlalchemy'
    ],
    zip_safe=False,
)
