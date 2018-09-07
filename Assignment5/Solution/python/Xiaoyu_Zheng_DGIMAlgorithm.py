from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

    host = "localhost"
    port = 9999
    sc = SparkContext("local[4]", "Assignment5")
    sc.setLogLevel(logLevel="OFF")
    ssc = StreamingContext(sc, 10)
    lines = ssc.socketTextStream(host, port)
    buckets = []  # (size, timestamp)
    timestamp = 0
    bits = ""
    ones = 0

    words = lines.flatMap(lambda x: x.split())

    def dgim(lis):
        # print("!!!!")
        global timestamp
        global buckets
        global ones
        global bits
        # print(lis.collect())
        for e in lis.collect():
            if len(bits) >= 1000:
                tmpBit = bits[0]
                bits = bits[1:]
                if tmpBit == "1":
                    ones -= 1
            bits = bits + e
            timestamp += 1
            if len(buckets) > 1:
                if buckets[-1][1] < timestamp - 1000:
                    buckets.pop(-1)
            if e == "0":
                continue
            else:
                ones += 1
                buckets.insert(0, (0, timestamp))
                ind = 0
                while ind+2 < len(buckets):
                    if buckets[ind][0] == buckets[ind+1][0] and buckets[ind+2][0] == buckets[ind+1][0]:
                        size = 1+buckets[ind][0]
                        minTimeStamp = buckets[ind+1][1]
                        buckets.pop(ind+1)
                        buckets.pop(ind+1)
                        buckets.insert(ind+1, (size, minTimeStamp))
                    ind += 1
            # print(buckets)
        if timestamp >= 1000:
            estimates = sum([2**x[0] for x in buckets[:-1]]) + (2**buckets[-1][0])/2
            print("Estimated number of ones in the last 1000 bits: " + str(int(estimates)))
            print("Actual number of ones in the last 1000 bits: " + str(ones))
            print("\n\n")

    words.foreachRDD(lambda rdd: dgim(rdd))

    # ones = words.map(lambda x: (x, 1)).filter(lambda x: x[0] == "1")
    # wordCounts = ones.reduceByKey(lambda x, y: x + y)

    # print("Actual number of ones in the last 1000 bits: ")
    # print("Actual number of ones:")
    # wordCounts.pprint()
    # print(ones.count())

    ssc.start()
    ssc.awaitTermination()