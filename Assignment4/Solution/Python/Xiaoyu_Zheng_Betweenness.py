# from __future__ import division

from pyspark.sql import SparkSession
from itertools import combinations

import time
import sys

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 2:
        print("""Usage:
             cd $SPARK_HOME
             bin/spark-submit Xiaoyu_Zheng_Betweenness.py <rating file path>
          """)
        exit(-1)

    path = sys.argv[1]
    # path = "/Users/xiaoyuzheng/Desktop/usc/18summer/553/Assignments/Assignment_04/data/ratings.csv"

    spark = SparkSession.builder \
        .appName("Assignment4") \
        .getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile(path)
    rddHeader = rdd.first()

    # ((user_id, movie_id), rating)
    data = rdd.filter(lambda row: row != rddHeader).map(lambda x: x.split(",")) \
        .map(lambda x: ((int(x[0]), int(x[1])), float(x[2])))

    # (movie_id, [(user_id, rating)])
    # movieToUser = data.reduceByKey(lambda x, y: x+y)
    movieToUser = data.map(lambda x: (x[0][1], [(x[0][0], x[1])])).reduceByKey(lambda x, y: x+y) \
        # .filter(lambda x: len(x[1]) >= 9)
    # print(movieToUser.take(10))

    def userRatingComb(x):
        for y in combinations(x[1], 2):
            a, b = min(y[0], y[1]), max(y[0], y[1])
            yield ((a[0], b[0]), [(a[1], b[1])])

    # ((user1_id, user2_id), [(rating1, rating2)])    user1_id < user2_id
    userToRating = movieToUser.flatMap(userRatingComb).reduceByKey(lambda x, y: x+y)
    # print(userToRating.take(10))

    # (user1_id, user2_id) for all edges
    edges = userToRating.filter(lambda x: len(x[1]) >= 9).map(lambda x: x[0])
    # print(edges.take(10))

    # (user1_id, [other_user_id])
    edgeUsers = edges.flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y)
    # print(edgeUsers.take(10))

    dicTree = {}
    for e in edgeUsers.collect():
        dicTree[e[0]] = e[1]

    # print(sorted(dicTree[137]))

    # ind = 0
    # for e in dicTree:
    #     if ind > 10:
    #         break
    #     else:
    #         ind += 1
    #         print(e, dicTree[e])

    def GN(root):
        # step 1: find the bfs tree graph
        # root = x[0]
        head = root
        bfsTraversal = [head]  # the list of bfs traversal order
        nodes = set(bfsTraversal)
        treeLevel = {head: 0}  # {node: level}
        dicParent = {}  # {childNode: [parent nodes in bfs graph]}
        index = 0  # the current index of the bfsTraversal
        while index < len(bfsTraversal):
            head = bfsTraversal[index]
            children = dicTree[head]
            for child in children:
                if child not in nodes:
                    bfsTraversal.append(child)
                    nodes.add(child)
                    treeLevel[child] = treeLevel[head] + 1
                    dicParent[child] = [head]
                else:
                    if treeLevel[child] == treeLevel[head] + 1:
                        dicParent[child] += [head]
            index += 1
        # step 2: calculate betweenness for each bfs graph
        dicNodeWeight = {}
        for i in range(len(bfsTraversal) - 1, -1, -1):
            node = bfsTraversal[i]
            if node not in dicNodeWeight:
                dicNodeWeight[node] = 1
            if node in dicParent:
                parents = dicParent[node]
                parentsSize = len(parents)
                for parent in parents:
                    addition = float(dicNodeWeight[node]) / parentsSize
                    if parent not in dicNodeWeight:
                        dicNodeWeight[parent] = 1
                    dicNodeWeight[parent] += addition
                    tmpEdge = (min(node, parent), max(node, parent))
                    yield (tmpEdge, addition / 2)

    # ((user1_id, user2_id), betweenness)
    betweenness = edgeUsers.flatMap(lambda x: GN(x[0])) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])

    # tt = edgeUsers.flatMap(lambda x: GN(x[0])).sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])
    # print(tt.take(10))

    # print(betweenness.take(10))
    #
    # end = time.time()
    # print("Time: "+str(end-start))

    text = open("Xiaoyu_Zheng_Betweenness.txt", "w")
    if betweenness:
        for e in betweenness.collect():
            text.write(str(e[0][0]))
            text.write(", ")
            text.write(str(e[0][1]))
            text.write(", ")
            text.write(str(e[1]))
            text.write("\n")
    text.close()

    end = time.time()
    print("Time: " + str(end - start) + " sec")

    spark.stop()
