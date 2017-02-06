
from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import socket
import json

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after

access_token = "405367480-Ls4t1ozh58SF60buJbqRJRFI4lBr9CsuyxRxLvbZ"
access_token_secret = "5WlEOrDfVkj1LuM1o4x6gXStaQsGqFv15fUcacZdQtVkn"
consumer_key = "NjCazF6SrCOuAjnBk3jxVSt2t"
consumer_secret = "1L6g5tPq5vCouqkeI3nqhtL614cdFigXfCvvUc8lRsMwHEPv2f"
# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section


class SocketListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, clientSocket):
        self.clientSocket = clientSocket
        self.printed = False
    def on_data(self, data):
        try:
            jdata = json.loads(data)
            # if 'text' in jdata:
            #      print(json.dumps(jdata))
            # else:
            #     print('F')
            #     print( jdata)
            self.clientSocket.send(data)
            return True
        except BaseException as e:
            if not self.printed:

                print("Error on_data: %s" % str(e))
                self.printed = True
        return True

    def on_error(self, status):
        print(status)
def sendData(clientSocket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, SocketListener(clientSocket))
    twitter_stream.filter(locations=[-125,27,-60,50])
if __name__ == '__main__':
    s = socket.socket()         # Create a socket object
    host = "localhost"      # Get local machine name
    port = 5555                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print( "Received request from: " + str( addr ) )

    sendData( c )