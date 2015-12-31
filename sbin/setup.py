from setuptools import setup

setup(
    name='hpcc-sbin',
    version='0.1',
    py_modules=['vcl'],
    include_package_data=True,
    install_requires=[
        'click',
        'executor',
    ],
    entry_points='''
        [console_scripts]
        vcl=vcl:cli
    ''',
)
