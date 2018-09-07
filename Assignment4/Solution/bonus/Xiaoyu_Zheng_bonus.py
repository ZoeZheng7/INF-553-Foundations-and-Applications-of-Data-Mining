from pyspark.sql import SparkSession
from itertools import combinations
import networkx as nx
from networkx.algorithms import community

import time
import sys

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 2:
        print("""Usage:
             cd $SPARK_HOME
             bin/spark-submit Xiaoyu_Zheng_bonus.py <rating file path>
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

    # allUsers = data.map(lambda x: (x[0][0], 1)).reduceByKey(lambda x, y: x).map(lambda x: x[0])
    # allUsersSet = set(allUsers.collect())

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

    graph = nx.Graph()
    graph.add_edges_from(edges.collect())

    # print("graph construction")

    communitiesGene = community.girvan_newman(graph)
    # index = 1
    # finals = next(communitiesGene)
    # while index < 224:
    #     finals = next(communitiesGene)
    first_level_communities = next(communitiesGene)
    second_level_communities = next(communitiesGene)
    third_level_communities = next(communitiesGene)

    sorted(map(sorted, third_level_communities))

    print(third_level_communities[:10])

    text = open("Xiaoyu_Zheng_bonus.txt", "w")
    for community in third_level_communities:
        text.write("[")
        i = 0
        # community = sorted(list(community))
        for node in community:
            if i != 0:
                text.write(", ")
            text.write(str(node))
            i += 1
        text.write("]")
        text.write("\n")
    text.close()
    print(graph.nodes())

    end = time.time()
    print("Time: " + str(end - start) + " sec")

    spark.stop()
