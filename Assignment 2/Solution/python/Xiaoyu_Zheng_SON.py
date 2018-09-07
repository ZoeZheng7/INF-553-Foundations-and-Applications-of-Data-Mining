from __future__ import print_function
from __future__ import division
from copy import copy
from itertools import combinations
import time


import sys
import csv

from pyspark.sql import SparkSession

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 4:
        print("""Usage:
                 cd $SPARK_HOME
                 bin/spark-submit Xiaoyu_Zheng_SON.py <case number> <csv file path> <support>
              """)
        exit(-1)

    path = sys.argv[2]
    case = sys.argv[1]
    support = int(sys.argv[3])

    spark = SparkSession \
        .builder \
        .appName("SON") \
        .getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile(path)
    header = rdd.first()
    data = rdd.filter(lambda row: row != header)
    baskets = data

    if case == "1":
        baskets = data.map(lambda x: x.split(",")) \
            .filter(lambda line: len(line)>1) \
            .map(lambda x: (x[0], {int(x[1])})) \
            .reduceByKey(lambda x, y: x | y)

    elif case == "2":
        baskets = data.map(lambda x: x.split(",")) \
            .filter(lambda line: len(line)>1) \
            .map(lambda x: (x[1], {int(x[0])})) \
            .reduceByKey(lambda x, y: x | y)

    else:
        print("case number should only be 1 for case 1, or 2 for case 2")
        exit(-1)

    transactions = baskets.values()

    p = transactions.getNumPartitions()

    def apriori(newChunk, p, support):

        chunk = list(newChunk) #TODO remove collect()

        singleCount = {}
        preRes = []
        newChunk = chunk

        for basket in newChunk:
            # print(basket)
            for elem in basket:
                # print(elem)
                if elem in singleCount:
                    # print(elem, singleCount[elem])
                    if singleCount[elem] < support/p:
                        # print(elem, singleCount[elem])
                        singleCount[elem] += 1
                        if singleCount[elem] >= support/p:
                            preRes.append({elem})
                            yield ((elem), 1)
                else:
                    singleCount[elem] = 1
                    if singleCount[elem] >= support/p:
                        yield ((elem), 1)
                        preRes.append({elem})
        size = 1

        newCand = []
        # print(preRes)
        for i in range(len(preRes)):
            x = preRes[i]
            # print(x)
            for j in range(i+1, len(preRes)):
                y = preRes[j]
                # if x == y:
                #     continue
                tmp = x | y
                if tmp not in newCand:
                    if len(tmp) == size+1:
                        flag = True
                        inter = x & y
                        diff = x ^ y
                        for element in inter:
                            t = ({element} | diff)
                            if t not in preRes:
                                flag = False
                                break
                            else:
                                continue
                        if flag:
                            newCand.append(tmp)

        while newCand:
            preRes = []
            size += 1
            count = {}
            newChunk = chunk
            # newChunk = copy(chunk)
            # tmp = copy(chunk)
            # print("??????????")
            # print("chunk in repeatA")
            # print(str(chunk))
            # print("newCand in repeatA")
            # print(str(newCand))

            for basket1 in chunk:
                # print("!!!!!!!!!!!")
                for cand in newCand:

                    if cand.issubset(basket1):
                        # print(cand)
                        if count.has_key(tuple(cand)):
                            # print("in")
                            if count[tuple(cand)] < support/p:
                                count[tuple(cand)] += 1
                                if count[tuple(cand)] >= support/p:
                                    preRes.append(cand)
                                    yield (tuple(cand), 1)
                        else:
                            # print("not")
                            count[tuple(cand)] = 1
                            if count[tuple(cand)] >= support/p:
                                preRes.append(cand)
                                yield (tuple(cand), 1)
            # newCand = geneCand(preRes, size)

            newCand = []
        # print(preRes)
            for i in range(len(preRes)):
                x = preRes[i]
                # print(x)
                for j in range(i+1, len(preRes)):
                    y = preRes[j]
                # if x == y:
                #     continue
                    tmp = x | y
                    if tmp not in newCand:
                        if len(tmp) == size+1:
                            flag = True
                            # inter = x & y
                            # diff = x ^ y
                            t = combinations(tmp, size)
                            for element in t:
                                # t = ({element} | diff)
                                if set(element) not in preRes:
                                    flag = False
                                    break
                                else:
                                    continue
                            if flag:
                                newCand.append(tmp)

            # print("newCand")
            # for x in newCand:
            #     print(x)
            # repeatA(newChunk, p, support, newCand, size)
            # for x in tmp:
            #     print(x)
        # print("bbbbbbbbbb")

    # aprioriRes = apriori(transactions, p, support)

    # print(support/p)
    aprioriRes = transactions.mapPartitions(lambda x: apriori(x, p, support)) \
        .reduceByKey(lambda x, y: x) \
        .keys()

    # aprioriRes = aprioriRes.toLocalIterator()
    aprioriRes = [x for x in aprioriRes.toLocalIterator()]
    # for x in aprioriRes.collect():
    #     print(x)

    def countVeri(x, aprioriRes):
        # data = list(aprioriRes)
        # tempCount = []
        data = list(x)
        for basket in data:
            for subset in aprioriRes:
                if isinstance(subset, int):
                    subset = {subset}
                else:
                    subset = set([y for y in subset])
                if subset.issubset(basket):
                    yield(tuple(subset), 1)
                # tempCount.append((tuple(subset), 1))
        # return tempCount

    # for x in transactions.collect():
    #     print(x)

    res = transactions.mapPartitions(lambda x: countVeri(x, aprioriRes)) \
                      .reduceByKey(lambda x, y: x+y) \
                      .filter(lambda x: x[1] >= support) \
                      .map(lambda x: x[0])
    # res = []
    # for x in transactions.collect():
    #     for subset in aprioriRes:
    #         # print({subset})
    #         if isinstance(subset, int):
    #             subset = {subset}
    #         else:
    #             subset = set([y for y in subset])
    #         if subset.issubset(x):
    #             res.append((tuple(subset), 1))

    # print("x")
    # for x in res.collect(): # TODO: add .collect()
    #     print(x)

    finals = list(res.collect())

    finals.sort()
    finals.sort(key=lambda x: len(x))

    # for x in finals: # TODO: add .collect()
    #     print(x)

    # start = path.rindex("/")
    name = path[:-3]
    #
    text = open("Xiaoyu_Zheng_SON_"+name+"case"+case+"-"+str(support)+".txt", "w")
    # text = open("text.txt", "w")
    if finals:
        size = len(finals[0])
        for j in range(len(finals)):
            e = finals[j]

            if len(e) > size:
                text.write("\n\n")
            elif j != 0:
                text.write(", ")
            text.write("(")
            for i in range(len(e)):
                if i != 0:
                    text.write(", ")
                text.write(str(e[i]))
            text.write(")")
            size = len(e)

    text.close()
    end = time.time()
    print("!!!!!!!!!!!!")
    print(str(end-start))

    spark.stop()

# TODO: 1. change for cand to combinations 2. combine count in the chunk of phase 2
