import re
import datetime
import numpy
from pyspark.sql.functions import col, stddev, mean, avg, when, log
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.ml.feature import OneHotEncoder, StringIndexer
import sys
import os
from math import sqrt
from pyspark.mllib.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.mllib.linalg import Vectors
import pyspark.sql.functions as spf
from parse_logs import *
from data_preprocessing import *


def error(point, kmeans):
    """ Convert Apache time format into a Python datetime object
    Args:
        point: point to predict in model
        kmeans (KMeansModel object): trained k-means model
    Returns:
        float: Calculate the within cluster squared error distance and return total for model
    """
    center = kmeans.centers[kmeans.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


if __name__ == '__main__':
    # Initialize SparkContext object
    sc = SparkContext(appName="PythonDetectDDOS")
    sc.setLogLevel("ERROR")  # Reduce logging
    sqlContext = sql.SQLContext(sc)

    # Path to log input file
    logFile = "/user/root/src/Project - Developer - apache-access-log (4).txt.gz"

    # Read log text file and parse based on Apache log standard
    parsed_logs, access_logs = parseLogs(sc, logFile)

    # Process data for feature columns to be used in training
    df4 = dataProcessing(access_logs)
    df4.show()

    # Format DataFrame into Dense Vector for mllib K-means clustering
    data7 = df4.rdd.map(lambda row: Vectors.dense(row[2], row[3]))
    data7.cache()

    # Train Data for kmeans model
    kmeans = KMeans.train(data7, 3, 10)

    # Print the centers to cehck
    centers = kmeans.clusterCenters
    for center in centers:
        print(center)
    WSSSE = data7.map(lambda point: error(point, kmeans)).reduce(lambda x, y: x + y)
    print "Within Set Sum of Squared Error = " + str(WSSSE)
    # Convert DataFrame object to RDD object to add cluster predictions
    rowsRDD = df4.rdd.map(lambda r: (r[0], r[1], r[2], r[3], r[4]))
    rowsRDD.cache()
    predictions = rowsRDD.map(lambda r: (r[0], r[1], r[2], r[3], r[4], kmeans.predict(Vectors.dense(r[2], r[3]))))
    predDF = predictions.toDF()
    predDF.show()
    # Find malicious IP addresses based on cluster. Assumes that the last cluster is the cluster of malicious IP addresses.
    mali = predDF.filter(predDF._6 == 2).select("_1")

    # Write the column of IP addresses and save to the results directory
    rowsRDD = mali.rdd.map(lambda r: (r[0])).coalesce(1).saveAsTextFile("results")
    # Save model for futures use
    kmeans.save(sc, "/user/root/src/KMeansModel.model")
