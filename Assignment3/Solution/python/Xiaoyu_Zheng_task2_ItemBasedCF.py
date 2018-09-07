from __future__ import division

from pyspark.sql import SparkSession
from itertools import combinations

import time
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""Usage:
             cd $SPARK_HOME
             bin/spark-submit Xiaoyu_Zheng_task2_ItemBasedCF.py <rating file path> <testing file path>
          """)
        exit(-1)

    path = sys.argv[1]
    test = sys.argv[2]
    start = time.time()
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-latest-small/ratings.csv"
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ml-20m/ratings.csv"
    # print(path)
    # test = "/Users/xisaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_small.csv"
    # test = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/testing_20m.csv"

    spark = SparkSession.builder \
        .appName("ItemBased") \
        .getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile(path)
    header = rdd.first()
    data = rdd.filter(lambda row: row != header).map(lambda x: x.split(","))

    mov = data.map(lambda x: (int(x[1]), 1)).reduceByKey(lambda x, y: x).map(lambda x: x[0])
    movies = list(mov.collect())
    movies.sort()
    dicM = {}
    for i, e in enumerate(movies):
        dicM[e] = i
    usr = data.map(lambda x: (int(x[0]), 1)).reduceByKey(lambda x, y: x).map(lambda x: x[0])
    users = list(usr.collect())
    users.sort()
    dicU = {}
    for i, e in enumerate(users):
        dicU[e] = i
    vu = sc.broadcast(dicU)

    mat = data.map(lambda x: (int(x[1]), [vu.value[int(x[0])]])).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[0])
    matrix = mat.collect()

    # print(matrix[dicM[135536]])
    # print(matrix[dicM[96114]])

    m = len(users)  # m: the number of the bins

    # hash function:
    def f(x, has):
        a = has[0]
        b = has[1]
        p = has[2]
        # return ((a * x + b) % p) % m
        return min([((a * e + b) % p) % m for e in x[1]])

    # TODO: more hash functions (or less?)
    # hash parameters [a, b, p]
    hashes = [[913, 901, 24593], [14, 23, 769], [1, 101, 193], [17, 91, 1543], \
              [387, 552, 98317], [11, 37, 3079], [2, 63, 97], [41, 67, 6151], \
              [91, 29, 12289], [3, 79, 53], [73, 803, 49157], [8, 119, 389]]
    # good hash table primes: http://planetmath.org/goodhashtableprimes

    # def hashMatrix(x):
    #     return [f(x, has) for has in hashes]

    # print(matrix[0])
    # print(matrix[0][0], matrix[1][0], matrix[2][0], matrix[100][0])
    # print(movies[0], movies[1], movies[2], movies[100])

    signatures = mat.map(lambda x: (x[0], [f(x, has) for has in hashes]))
    # print(signatures.collect()[:10])

    n = len(hashes)  # the size of the signature column
    b = 6
    r = int(n/b)

    def sig(x):
        # for e in x:
        res = []
        for i in range(b):
            res.append(((i, tuple(x[1][i*r:(i+1)*r])), [x[0]]))
        return res

    def pairs(x):
        res = []
        length = len(x[1])
        whole = list(x[1])
        whole.sort()
        for i in range(length):
            for j in range(i+1, length):
                res.append(((whole[i], whole[j]), 1))
        return res

    # cand = signatures.flatMap(sig)
    cand = signatures.flatMap(sig).reduceByKey(lambda x, y: x+y).filter(lambda x: len(x[1]) > 1).flatMap(pairs) \
        .reduceByKey(lambda x,y: x).map(lambda x: x[0])

    # for e in cand.collect():
    #     print(e)

    # sign = signatures.collect()
    # size = len(movies)
    # candidate = []
    # for i in range(size):
    #     for j in range(i+1, size):
    #         piece1 = sign[i]
    #         piece2 = sign[j]
    #         for k in range(b):
    #             if piece1[1][k*r:(k+1)*r] == piece2[1][k*r:(k+1)*r]:
    #                 candidate.append([piece1[0], piece2[0]])
    #                 break
    # print("candidate pairs:")
    # print(candidate)

    def jaccard(x):
        a = set(matrix[dicM[x[0]]][1])
        b = set(matrix[dicM[x[1]]][1])
        inter = a & b
        union = a | b
        jacc = len(inter)/len(union)
        return (x[0], x[1], jacc)

    # result = cand.map(jaccard).filter(lambda x: x[2] >= 0.5).sortBy(lambda x: x[1]).sortBy(lambda x: x[0])
    # print(result.take(10))

    # similar movie pairs from LSH: (movie1_id, movie2_id)
    simiMovie = cand.map(jaccard).filter(lambda x: x[2] >= 0.5)\
        .map(lambda x: ((min(x[0], x[1]), max(x[0], x[1])), x[2]))

    #     .reduceByKey(lambda x, y: x + y)
    # print(simiMovie.take(10))

    dataAll = data
    data = dataAll.map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))
    testRdd = sc.textFile(test).map(lambda x: x.split(","))
    testHeader = testRdd.first()
    testKey = testRdd.filter(lambda row: row != testHeader).map(lambda x: (int(x[0]), int(x[1])))
    testData = testKey.map(lambda x: (x, None))

    # ((user_id, movie_id), rating)
    trainData = data.subtractByKey(testData)
    # print(trainData.take(10))
    dicTrain = {}
    for e in trainData.collect():
        dicTrain[e[0]] = e[1]

    # (movie_id, average_rating)
    movieAvg = trainData.map(lambda x: (x[0][1], [x[1]])).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], sum(x[1])/len(x[1])))
    dicAvg = {}
    for e in movieAvg.collect():
        dicAvg[e[0]] = e[1]
    # print(movieAvg.take(10))

    # (user_id, [(movie_id, rating)])
    userToMovie = trainData.map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(lambda x, y: x+y)
    # print(userToMovie.take(10))

    # =====================================================================================================
    def movieRatingComb(x):
        for y in combinations(x[1], 2):
            a, b = min(y[0], y[1]), max(y[0], y[1])
            yield ((a[0], b[0]), [(a[1], b[1])])

    # ((movie1_id, movie2_id), [(rating1, rating2)])    movie1_id < movie2_id
    movieToRating = userToMovie.flatMap(movieRatingComb).reduceByKey(lambda x, y: x+y)
    # print(movieToRating.take(10))


    # simiMovieToRating = movieToRating.filter(lambda x: x[0] in simiMovie)
    simiMovieToRating = movieToRating.join(simiMovie.map(lambda x: (x, None))).map(lambda x: (x[0], x[1][:-1]))
    # print("simiMovieToRating: ")
    # print(simiMovieToRating.take(10))

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

    # ((movie1_id, movie2_id), pearson_similarity) movie1_id < movie2_id
    movieSimi = simiMovieToRating.map(lambda x: (x[0], calSimi(x[1])))
    # =====================================================================================================

    # =====================================================================================================
    # def movieRatingComb(x):
    #     for y in combinations(x[1], 2):
    #         a, b = min(y[0], y[1]), max(y[0], y[1])
    #         yield ((a[0], b[0]), None)
    # # ((movie1_id, movie2_id), None)
    # movieToRating = userToMovie.flatMap(movieRatingComb)
    # # ((movie1_id, movie2_id), similarity from LSH) ???????????????????????????????/
    # movieSimi = movieToRating.join(simiMovie).map(lambda x: (x[0], x[1][1]))
    # print(movieSimi.take(10))
    # =====================================================================================================

    movieSimiMatrix = movieSimi.flatMap(lambda x: [(x[0][0], [(x[0][1], x[1])]), (x[0][1], [(x[0][0], x[1])])]) \
        .reduceByKey(lambda x, y: x+y)

    dicSimi = {}
    for e in movieSimiMatrix.collect():
        dicSimi[e[0]] = e[1]

    def predict(x):
        if x[1] not in dicSimi:
            if x[1] not in dicAvg:
                return (x, 2.5)
            return (x, dicAvg[x[1]])
        allCom = dicSimi[x[1]]
        allCom.sort(key=lambda y: y[1], reverse=True)
        length = len(allCom)
        companion = []
        if length > 2:
            companion = allCom[:2]
        else:
            companion = allCom
        avg = 0
        sumi = 0
        down = 0
        if len(companion) == 0:
            print("no companion")
        for e in companion:
            tmp = (x[0], e[0])
            if tmp in dicTrain:
                rate = dicTrain[tmp]
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
    print(result.take(10))

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

    text = open("Xiaoyu_Zheng_ItemBasedCF.txt", "w")
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
    spark.stop()
