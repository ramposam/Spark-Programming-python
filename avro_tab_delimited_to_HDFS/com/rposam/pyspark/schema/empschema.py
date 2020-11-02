from pyspark.sql.types  import StructField, StructType, StringType, IntegerType, DoubleType, LongType, ArrayType, \
    DateType


def getEmpSchema():
    schema = StructType([
        StructField("EMPNO", IntegerType(), False),
        StructField("ENAME", StringType(), False),
        StructField("JOB", StringType(), False),
        StructField("MGR", StringType(), True),
        StructField("HIREDATE", DateType(), False),
        StructField("SAL", DoubleType(), False),
        StructField("COMM", StringType(), True),
        StructField("DEPTNO", IntegerType(), False)
    ])
    return schema
