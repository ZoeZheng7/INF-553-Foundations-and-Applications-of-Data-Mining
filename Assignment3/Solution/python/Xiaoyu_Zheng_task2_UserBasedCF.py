from pyspark.sql import SparkSession
from itertools import combinations

import time
import sys

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 3:
        print("""Usage:
             cd $SPARK_HOME
             bin/spark-submit Xiaoyu_Zheng_task2_UserBasedCF.py <rating file path> <testing file path>
          """)
        exit(-1)

    path = sys.argv[1]
    test = sys.argv[2]
    start = time.time()
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-latest-small/ratings.csv"
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-20m/ratings.csv"
    # print(path)
    # test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_small.csv"
    # test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_20m.csv"

    spark = SparkSession.builder \
        .appName("Recommendation") \
        .getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile(path)
    rddHeader = rdd.first()
    dataAll = rdd.filter(lambda row: row != rddHeader).map(lambda x: x.split(","))
    data = dataAll.map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))
    # Linear Regression to make sparse matrix dense
    # https://spark.apache.org/docs/2.3.0/mllib-linear-methods.html#linear-least-squares-lasso-and-ridge-regression

    testRdd = sc.textFile(test).map(lambda x: x.split(","))
    testHeader = testRdd.first()
    testKey = testRdd.filter(lambda row: row != testHeader).map(lambda x: (int(x[0]), int(x[1])))
    # testInter = testKey.map(lambda x: (x, None))

    testData = testKey.map(lambda x: (x, None))

    trainData = data.subtractByKey(testData)
    # print(trainData.take(10))
    dicTrain = {}
    for e in trainData.collect():
        dicTrain[e[0]] = e[1]

    userAvg = trainData.map(lambda x: (x[0][0], [x[1]])).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], sum(x[1])/len(x[1])))
    # print(userToItemRating.take(5))
    dicAvg = {}
    for e in userAvg.collect():
        dicAvg[e[0]] = e[1]

    # movies = dataAll.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x).map(lambda x: (x[0]))
    # users = dataAll.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x).map(lambda x: (x[0]))
    #
    # comb = users.cartesian(movies).map(lambda x: (x, None))
    #
    # def addi(x):
    #     if x in dicAvg:
    #         return dicAvg[x]
    #     else:
    #         return 2.5
    #
    # sparseData = comb.subtractByKey(data).map(lambda x: ((x[0][0], x[0][1]), addi(x[0][0])))
    # # linearTrainData = data.map(lambda x: (x[1], list(x[0])))
    # trainData = sc.union([trainData, sparseData])

    # (movie_id, [(user_id, rating)])
    movieToUser = trainData.map(lambda x: (x[0][1], [(x[0][0], x[1])])).reduceByKey(lambda x, y: x+y)
    # print(movieToUser.take(10))

    def userRatingComb(x):
        for y in combinations(x[1], 2):
            # print("yyyyyyyyy")
            # print(y)
            a, b = min(y[0], y[1]), max(y[0], y[1])
            # print("a")
            # print(a)
            # print("b")
            # print(b)
            yield ((a[0], b[0]), [(a[1], b[1])])

    # ((user1_id, user2_id), [(rating1, rating2)])    user1_id < user2_id
    userToRating = movieToUser.flatMap(userRatingComb).reduceByKey(lambda x, y: x+y)
    # print(userToRating.take(10))



    def calSimi(x):
        ratings = x
        ratingA = [x[0] for x in ratings]
        ratingB = [x[1] for x in ratings]
        avg1 = sum(ratingA)/len(ratingA)
        avg2 = sum(ratingB)/len(ratingB)
        diff1 = [x-avg1 for x in ratingA]
        diff2 = [x-avg2 for x in ratingB]
        root1 = pow(sum([x**2 for x in diff1]), 0.5)
        root2 = pow(sum([x**2 for x in diff2]), 0.5)
        up = sum([diff1[i]*diff2[i] for i in range(len(ratingA))])
        if up == 0:
            return 0
        return up/(root1*root2)

    # ((user1_id, user2_id), pearson_similarity) user1_id < user2_id
    userSimi = userToRating.map(lambda x: (x[0], calSimi(x[1])))
    # print(userSimi.take(10))

    # (activeUser_id, [(user1_id, similarity)])
    userSimiMatrix = userSimi.flatMap(lambda x: [(x[0][0], [(x[0][1], x[1])]), (x[0][1], [(x[0][0], x[1])])])\
        .reduceByKey(lambda x, y: x+y)
    # print(userSimiMatrix.take(5))

    dicSimi = {}
    # for e in userSimiMatrix.collect():
    #     tmp = {}
    #     for p in e[1]:
    #         tmp[p[0]] = p[1]
    #     dicSimi[e[0]] = tmp

    for e in userSimiMatrix.collect():
        dicSimi[e[0]] = e[1]



    # a = userSimiMatrix.filter(lambda x: x[0] == 621).collect()
    # b = userSimiMatrix.filter(lambda x: x[0] == 577).collect()
    # print(a)
    # print(b)

    # def predict(x):
    #     companion = userSimiMatrix.filter(lambda y: y[0] == x[0])
    #     print(companion.collect())
    #
    # result = testKey.map(lambda x: (x, predict(x)))

    # (user_id, avg_rating)


    # print(userSimiMatrix.filter(lambda y: y[0] == 577).collect()[0][1])
    # result = []

    # for x in testKey.collect():
    #     companion = userSimiMatrix.filter(lambda y: y[0] == x[0]).collect()[0][1]
    #     # print(companion)
    #     sumi = userAvg.filter(lambda b: b[0] == x[0]).collect()[0][1]
    #     for e in companion:
    #         rate = trainData.filter(lambda a: a[0] == (e[0], x[1])).collect()
    #         # print(rate)
    #         if len(rate) > 0:
    #             rate = rate[0][1]
    #             sumi += rate*e[1]
    #     result.append(((x[0], x[1]), sumi))

    def predict(x):
        allCom = dicSimi[x[0]]
        allCom.sort(key=lambda y: y[1], reverse=True)
        length = len(allCom)
        companion = []
        if length > 5:
            companion = allCom[:5]
        else:
            companion = allCom
        avg = dicAvg[x[0]]
        sumi = 0
        down = 0
        if len(companion) == 0:
            print("no companion")
        for e in companion:
            tmp = (e[0], x[1])
            if tmp in dicTrain:
                rate = dicTrain[tmp]-dicAvg[e[0]]
                sumi += rate*e[1]
                down += abs(e[1])
        if down == 0:
            return (x, avg)
        return (x, sumi/down+avg)

    def cutPred(x):
        # rate = 0
        if x[1] > 5:
            rate = 5
        elif x[1] < 0:
            rate = 0
        else:
            rate = x[1]
        return (x[0], rate)

    result = testKey.map(predict).map(cutPred).sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])
    # noPred = testData.subtractByKey(preds).map(lambda x: (x[0], 3))
    # result = sc.union([preds, noPred]).sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])

    # print("test count: ")
    # print(testKey.count())
    # print("preds count: ")
    # print(preds.count())
    # print("result count: ")
    # print(result.count())

    # output = sc.parallelize(result)
    # print(result.take(5))

    ratesAndPreds = data.join(testData).map(lambda x: (x[0], x[1][0])).join(result)
    # ratesAndPreds = data.join(testKey).map(lambda x: (x[0], x[1][0])).join(result)
    diff = ratesAndPreds.map(lambda r: abs(r[1][0] - r[1][1]))
    diff01 = diff.filter(lambda x: 0 <= x < 1)
    diff12 = diff.filter(lambda x: 1 <= x < 2)
    diff23 = diff.filter(lambda x: 2 <= x < 3)
    diff34 = diff.filter(lambda x: 3 <= x < 4)
    diff4 = diff.filter(lambda x: 4 <= x)

    MSE = diff.map(lambda x: x**2).mean()
    RMSE = pow(MSE, 0.5)

    text = open("Xiaoyu_Zheng_UserBasedCF.txt", "w")
    if result:
        for e in result.collect():
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

    # end = time.time()
    # print("Time: "+str(end - start)+" sec")
    spark.stop()
