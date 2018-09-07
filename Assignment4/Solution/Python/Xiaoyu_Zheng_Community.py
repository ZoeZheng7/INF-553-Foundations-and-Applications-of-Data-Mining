# from __future__ import division

from pyspark.sql import SparkSession
from itertools import combinations
import networkx as nx
import collections
from networkx import number_connected_components
from networkx import connected_components
from networkx import dfs_preorder_nodes

import time
import sys

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 2:
        print("""Usage:
             cd $SPARK_HOME
             bin/spark-submit Xiaoyu_Zheng_Community.py <rating file path>
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

    allUsers = data.map(lambda x: (x[0][0], 1)).reduceByKey(lambda x, y: x).map(lambda x: x[0])
    allUsersSet = set(allUsers.collect())

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

    # (user1_id, set(other_user_id))
    edgeUsers = edges.flatMap(lambda x: [(x[0], {x[1]}), (x[1], {x[0]})]).reduceByKey(lambda x, y: x.union(y))
    # print(edgeUsers.take(10))

    edgeUserList = edgeUsers.collect()

    dicTree = {}
    for e in edgeUserList:
        dicTree[e[0]] = e[1]

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
        .sortBy(lambda x: x[1], ascending=False)

    # print(betweenness.take(10))

    edgeNumber = edges.count()
    # edgeNumber = graph.number_of_edges()
    # print(edgeNumber)
    nodeList = graph.nodes()

    # singletons = allUsersSet - set(nodeList)
    # print("singletons")
    # print(singletons)

    dicK = {}
    for node in nodeList:
        dicK[node] = len(dicTree[node])

    modularity = 0
    dicModu = {}
    for i in nodeList:
        for j in nodeList:
            if j in dicTree[i]:
                A = 1
            else:
                A = 0
            # ki = len(dicTree[i])
            # kj = len(dicTree[j])
            tmp = (A-0.5*dicK[i]*dicK[j]/edgeNumber)/(2*edgeNumber)
            dicModu[(i, j)] = tmp
            modularity += tmp  # null graph modularity
    # modularity = modularity

    communities = [x for x in nx.connected_components(graph)]
    maxModu = (modularity, communities)
    # commSize = nx.number_connected_components(graph)
    # commSize = 1
    print(maxModu)

    def calcModularity(community):
        res = 0
        for i in community:
            for j in community:
                res += dicModu[(i, j)]
        return res

    def dfs(i, j, visited):
        # if i == j:
        #     return True
        visited.add(i)
        children = dicTree[i]
        for child in children:
            if child == j:
                return True
            if child not in visited:
                if dfs(child, j, visited):
                    return True
        return False

    # def bfs(i, j):
    #     queue = collections.deque([i])
    #     visited = {i}
    #     while queue:
    #         node = queue.popleft()
    #         children = dicTree[node]
    #         for child in children:
    #             if child == j:
    #                 return True
    #             if child not in visited:
    #                 queue.append(child)
    #     return False

    # def bfs(x, visited):
    #     visited.add(x)
    #     children = dicTree[x]
    #     for child in children:
    #         if child not in visited:
    #             visited = bfs(child, visited)
    #     return visited

    betweennessList = betweenness.collect()

    for e in betweennessList:
        print(e)
        i = e[0][0]
        j = e[0][1]
        graph.remove_edge(i, j)
        newModu = 0
        # tmpSize = nx.number_connected_components(graph)
        dicTree[i].remove(j)
        dicTree[j].remove(i)
        # community1 = bfs(i, set([]))
        if dfs(i, j, set([])):
            continue
        # community1 = set(list(nx.dfs_preorder_nodes(graph, i)))
    #     community2 = bfs(j, set([]))
    #     if community1 == community2:
    #         continue
    #     # if commSize == tmpSize:
    #     # commSize = tmpSize
        communities = [x for x in nx.connected_components(graph)]
        # sorted(communities)
        for community in communities:
            newModu += calcModularity(community)
        modularity = newModu
        # print("modu")
        # print(modularity)
        if maxModu[0] < modularity:
            maxModu = (modularity, communities)
    # print("maxModu")
    # print(maxModu)
    # print(len(maxModu[1]))

    text = open("Xiaoyu_Zheng_Community.txt", "w")
    for community in maxModu[1]:
        text.write("[")
        i = 0
        community = sorted(list(community))
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




