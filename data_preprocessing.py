from pyspark.sql.functions import col, stddev, mean, avg, when, log
from pyspark.sql.types import *
import pyspark.sql.functions as spf

def dataProcessing(access_logs):
    df = access_logs.toDF()
    df.show()
    df2 = df.groupBy("host").pivot("browser").count()
#    df3 = calcEntropy(df)
    df2.cache()

    df1 = df.groupBy("host").agg(avg('ua')).alias('avg_ua')
    df3 = df.groupBy("host").count()
    df3 = df3.join(df3.select(spf.sum("count").alias("total"))).withColumn("p", spf.col("count") / spf.col("total"))
    df2 = df2.withColumnRenamed('Mozilla/3.01', 'mozilla3').withColumnRenamed('Mozilla/4.0', "mozilla4").withColumnRenamed('Mozilla/5.0', "mozilla5").withColumnRenamed('Opera/9.00', "opera9")
    keep = [df3[c] for c in df3.columns] + [df2[c] for c in df2.columns if c != 'host']

    df3.cache()
    df3.show()
    df4 = df3.join(df2, df3["host"] == df2["host"]).select(*keep).drop('total')
    df4 = df4.withColumn('total_col', df4.mozilla3 + df4.mozilla4 + df4.mozilla5 + df4.opera9)
    keep2 = [df1[c] for c in df1.columns] + [df4[c] for c in df4.columns if c != 'host']

    df4 = df4.join(df1, df4["host"] == df1["host"]).select(*keep2)

    engines = ["mozilla3", "mozilla4", "mozilla5", "opera9"]
    for engine in engines:
        df4 = calcProp(df4, engine)
        df4 = df4.withColumn(engine, when(df4[engine] == 0, 1).otherwise(df4[engine]))
        df4 = calcEntropy(df4, engine)
    df4 = df4.withColumn('entropy', df4.mozilla3 + df4.mozilla4 + df4.mozilla5 + df4.opera9)
    df4.show()
    df4 = scaleColumn(df4, "p")
    df4 = scaleColumn(df4, "entropy")
    df4 = scaleColumn(df4, "avg(ua)")
    return df4.drop("mozilla3").drop("mozilla4").drop("mozilla5").drop("opera9").drop("p").drop("total_col").drop("avg(ua)").drop("entropy")


def calcProp(df, column):
    return df.withColumn(column+"_p", (col(column) / col("total_col"))).drop(column).withColumnRenamed(column+"_p", column)
def scaleColumn(df, column):
    mean1, sttdev1 = df.select(mean(column), stddev(column)).first()
    return df.withColumn(column+"_scaled", (col(column) - mean1) / sttdev1)


def calcEntropy(df, column):
    return df.withColumn(column+"_entropy", (col(column) * log(col(column)))).drop(column).withColumnRenamed(column+"_entropy", column)
