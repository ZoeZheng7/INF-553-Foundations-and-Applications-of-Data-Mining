import tweepy
import random
import collections


class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        super(MyStreamListener, self).__init__()
        self.count = 0
        self.list = []
        self.sumi = 0
        # self.tag = {}
        self.tag = collections.Counter()

    def on_status(self, status):
        # print(status.text)
        # print("!!!!!!!!")
        self.count += 1
        if self.count > 100:
            rmd = random.randint(1, self.count)
            if rmd < 100:
                statusOut = self.list.pop(rmd)
                self.sumi += len(status.text)-len(statusOut)
                self.list.append(status.text)
                words = status.text.split()
                tags = [x[1:] for x in words if len(x) > 1 and x[0] == "#"]
                wordsOut = statusOut.split()
                tagsOut = [x[1:] for x in wordsOut if len(x) > 1 and x[0] == "#"]
                self.tag.update(tags)
                self.tag.subtract(tagsOut)

                common5 = self.tag.most_common(5)
                print("The number of the twitter from beginning: "+ str(self.count))
                print("Top 5 hot hashtags:")
                for e in common5:
                    print(e[0]+": "+str(e[1]))
                cnt = min(100, self.count)
                print("The average length of the twitter is: "+str(1.0*self.sumi/cnt))
                print("\n\n")
        else:
            self.list.append(status.text)
            self.sumi += len(status.text)
            words = status.text.split()
            tags = [x[1:] for x in words if (len(x) > 1 and x[0] == "#")]
            self.tag.update(tags)


    def on_error(self, status_code):
        if status_code == 420:
            return False


if __name__ == "__main__":

    customerToken = "dF4iDYluulhXHEEDwhYDZcvlU"
    customerSecret = "EAnV7RzKb32Z7FukgyzoWbi2gX6XKX9vpbhYU6cEZ0LBvi8ylO"
    accessToken = "3076014876-N3WBMAgM4UE5JjoKcfYoB2dsi5qrMqWptlcgOho"
    accessSecret = "0WgCe4jRdobSFFeaHAjinVZzK3Um2XOwtDBOiQ6eKCymh"

    auth = tweepy.OAuthHandler(customerToken, customerSecret)
    auth.set_access_token(accessToken, accessSecret)
    api = tweepy.API(auth)
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    # tweets = myStream.sample(async=False)
    tweets = myStream.filter(track=['#'])

