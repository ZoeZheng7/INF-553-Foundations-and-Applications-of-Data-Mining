from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import numpy

import time
import sys

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 3:
        print("""Usage:
             cd $SPARK_HOME
             bin/spark-submit Xiaoyu_Zheng_task2_ModelBasedCF.py <rating file path> <testing file path>
          """)
        exit(-1)

    path = sys.argv[1]
    test = sys.argv[2]
    start = time.time()
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-latest-small/ratings.csv"
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-20m/ratings.csv"
# print(path)
#     test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_small.csv"
    # test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_20m.csv"

    spark = SparkSession.builder \
        .appName("Recommendation") \
        .getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile(path)
    rddHeader = rdd.first()
    dataAll = rdd.filter(lambda row: row != rddHeader).map(lambda x: x.split(","))
    data = dataAll.map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))
    testRdd = sc.textFile(test).map(lambda x: x.split(","))
    testHeader = testRdd.first()
    testKey = testRdd.filter(lambda row: row != testHeader).map(lambda x: (int(x[0]), int(x[1])))
    # testInter = testKey.map(lambda x: (x, None))
    print("test count: ")
    print(testKey.count())

    testData = testKey.map(lambda x: (x, None))

    trainData = data.subtractByKey(testData)

    # print(trainData.take(11))

    # header = rdd.first()
    # data = rdd.filter(lambda row: row != header).map(lambda x: x.split(","))

    ratings = trainData.map(lambda x: Rating(x[0][0], x[0][1], x[1]))

    def cutPred(x):
        # rate = 0
        if x[2] > 5:
            rate = 5
        elif x[2] < 1:
            rate = 1
        else:
            rate = x[2]
        return ((x[0], x[1]), rate)

    rank = 5
    numIterations = 10
    model = ALS.train(ratings, rank, numIterations)
    preds = model.predictAll(testKey).map(cutPred)
    noPred = testData.subtractByKey(preds).map(lambda x: (x[0], 3))
    predictions = sc.union([preds, noPred]).sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])
    # ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    # ratesAndPreds = data.join(testData).map(lambda x: (x[0], x[1][0])).sortBy(lambda x: x[1]).sortBy(lambda x: x[0]).join(predictions)
    print("result count: ")
    print(predictions.count())
    ratesAndPreds = data.join(testData).map(lambda x: (x[0], x[1][0])).join(predictions)
    diff = ratesAndPreds.map(lambda r: abs(r[1][0] - r[1][1]))
    diff01 = diff.filter(lambda x: 0 <= x < 1)
    diff12 = diff.filter(lambda x: 1 <= x < 2)
    diff23 = diff.filter(lambda x: 2 <= x < 3)
    diff34 = diff.filter(lambda x: 3 <= x < 4)
    diff4 = diff.filter(lambda x: 4 <= x)

    MSE = diff.map(lambda x: x**2).mean()
    RMSE = pow(MSE, 0.5)

    text = open("Xiaoyu_Zheng_ModelBasedCF.txt", "w")
    if predictions:
        for e in predictions.collect():
            text.write(str(e[0][0]))
            text.write(", ")
            text.write(str(e[0][1]))
            text.write(", ")
            text.write(str(e[1]))
            text.write("\n")
    text.close()

    print(">=0 and <1: "+str(diff01.count()))
    print(">=1 and <2: "+str(diff12.count()))
    print(">=2 and <3: "+str(diff23.count()))
    print(">=3 and <4: "+str(diff34.count()))
    print(">=4: "+str(diff4.count()))
    print("RMSE: "+str(RMSE))
    end = time.time()
    print("Time: "+str(end - start)+" sec")

    spark.stop()
