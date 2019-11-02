from setuptools import setup

setup(
    name='dsutils_dev',
    version='0.0.1',
    packages=['dsutils_dev'],
    license='MIT License',
    description='Data Science essentials',
    author='Roshan T John',
    author_email='roshan.github@gmail.com',
    url = 'https://github.com/RTJ19/dsutils_dev.git',
    keywords = ['PYSPARK', 'EDA']
    zip_safe=False,
    install_requires=['pandas'
                      , 'numpy'
                      , 'scikit-learn'
                      , 'scipy'
                      , 'matplotlib'
                      , 'joblib'
                      , 'pyspark_dist_explore'
                      , 'scikit-plot'
                      , 'tqdm'
                      # , 'spark_testing_base' # Only required for testing
                      # , 'findspark' # Only required for testing
                      ]
    classifiers=[
    'Development Status :: 3 - Alpha',      # "3 - Alpha", "4 - Beta" for development or "5 - Production/Stable" 
    'Intended Audience :: Developers',      
    'Topic :: Software Development :: Build Tools',
    'License :: MIT License',   
    'Programming Language :: Python :: 3',      
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],

)
