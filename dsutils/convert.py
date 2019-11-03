"""
Github user zaloogarcia have developed some function that will help in 
conversion pandas dataframe to pyspark
https://gist.github.com/zaloogarcia/11508e9ca786c6851513d31fb2e70bfc
"""
from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return DateType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(spark_session, pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    i = 0
    for column, typo in zip(columns, types): 
      #print (column,typo)
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    #print (p_schema)
    return spark_session.createDataFrame(pandas_df, p_schema)
