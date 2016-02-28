from setuptools import setup

setup(
    name='hpcc-sbin',
    version='0.1',
    py_modules=[
        'vcl',
        'hpcc',
        'ecl',
        'thor',
        'roxie',
    ],
    include_package_data=True,
    install_requires=[
        'click',
        'executor',
    ],
    entry_points={
        'console_scripts': [
            'vcl=vcl:cli',
            'hpcc=hpcc:cli',
            'ecl=ecl:cli',
            'thor=thor:cli',
            'roxie=roxie:cli',
        ]
    }
)
