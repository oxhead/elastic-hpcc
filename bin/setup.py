from setuptools import setup

setup(
    name='hpcc-bin',
    version='0.1',
    py_modules=[
        'hpcc_build',
    ],
    include_package_data=True,
    install_requires=[
        'click',
        'executor',
    ],
    entry_points={
        'console_scripts': [
            'hpcc-build=hpcc_build:cli',
        ]
    }
)
