from setuptools import setup

setup(
    name='dsutils_dev',
    version='0.0.1',
    packages=['dsutils_dev'],
    license='MIT License',
    description='Data Science essentials',
    author='Roshan T John',
    author_email='roshan.github@gmail.com',
    zip_safe=False,
    install_requires=['pandas'
                      , 'numpy'
                      , 'scipy'
                      , 'matplotlib'
                      # , 'spark_testing_base' # Only required for testing
                      # , 'findspark' # Only required for testing
                      ]

)
