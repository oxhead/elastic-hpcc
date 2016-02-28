from setuptools import setup

setup(
    name='hpcc-benchmark',
    version='0.1',
    py_modules=[
        'benchmark',
    ],
    include_package_data=True,
    install_requires=[
        'click',
        'executor',
    ],
    entry_points={
        'console_scripts': [
            'benchmark=benchmark:cli'
        ]
    }
)
