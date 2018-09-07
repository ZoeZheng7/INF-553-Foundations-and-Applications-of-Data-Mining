from __future__ import division

from pyspark.sql import SparkSession

import time
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""Usage:
             cd $SPARK_HOME
             bin/spark-submit Xiaoyu_Zheng_task1_Jaccard.py <rating file path>
          """)
        exit(-1)

    path = sys.argv[1]
    start = time.time()
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/ratings.csv"
    # print(path)

    spark = SparkSession.builder \
        .appName("LSH") \
        .getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile(path)
    header = rdd.first()
    data = rdd.filter(lambda row: row != header).map(lambda x: x.split(","))

    mov = data.map(lambda x: (int(x[1]), 1)).reduceByKey(lambda x, y: x).map(lambda x: x[0])
    movies = list(mov.collect())
    movies.sort()
    # print("movies")
    # print(movies)
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
    cand = signatures.flatMap(sig).reduceByKey(lambda x, y: x+y).filter(lambda x: len(x[1]) > 1).flatMap(pairs)\
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

    # cand = sc.parallelize(candidate)
    result = cand.map(jaccard).filter(lambda x: x[2] >= 0.5).sortBy(lambda x: x[1]).sortBy(lambda x: x[0])

    # for e in result.collect():
    #     print(e)
    rr = result.collect()

    ground = sc.textFile("/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_03/data/SimilarMovies.GroundTruth.05.csv") \
        .map(lambda x: x.split(",")).map(lambda x: (int(x[0]), int(x[1])))

    rrr = result.map(lambda x: (x[0], x[1]))
    rrrrr = list(rrr.collect())
    ggggg = list(ground.collect())
    # tp = rrr.filter(lambda x: x in ggggg)
    # r = rrr.toDF()
    # g = ground.toDF()
    tp = rrr.intersection(ground)

    # fp = rrr.filter(lambda x: x not in ggggg)
    # fn = ground.filter(lambda x: x in rrrrr)

    ttttt= list(tp.collect())
    precision = len(ttttt)/len(rrrrr)
    recall = len(ttttt)/len(ggggg)
    # print(len(ttttt))
    # print("tp:")
    # for e in ttttt:
    #     print(e)
    print("precision:")
    print(precision)
    print("recall:")
    print(recall)

    text = open("Xiaoyu_Zheng_SimilarMovies_Jaccard.txt", "w")
    if result:
        for e in rr:
            text.write(str(e[0]))
            text.write(", ")
            text.write(str(e[1]))
            text.write(", ")
            text.write(str(e[2]))
            text.write("\n")
    text.close()

    end = time.time()
    print("Elapsed time: ")
    print(str(end-start))

    spark.stop()
