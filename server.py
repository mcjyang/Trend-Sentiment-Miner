from flask import Flask, render_template, jsonify, request
import json
import time
import thread
from threading import Lock

app = Flask(__name__)
words = []
tweets = {}
id = 0
mutex = Lock()

@app.route("/")
def main():
    return render_template('index.html')

@app.route('/getWords')
def getWords():
    mutex.acquire()
    ret = words
    mutex.release()
    return json.dumps(ret)

@app.route('/getWordData')
def getWordData():
    requestID = int(request.args['id'])
    #print(requestID)
    #print(tweets)
    if requestID in tweets:
        ret = tweets[requestID]
        return json.dumps(ret)
    else:
        return "error"

# thread to collect data
def getDataJson( threadName, delay):
    interval = 10
    global id, tweets, words
    while True:
        try:
            mutex.acquire()
            with open('sample.txt') as f:
                if id > 250:
                    id = 0
                    tweets = {}
                    words = []
                for line in f:
                    #print(line)
                    a = json.loads(line)['trends']
                    for word in a:
                        tweets[id] = word['tweets']
                        del word['tweets']
                        word['id'] = id
                        words.append(word)
                        id += 1
            mutex.release()
            time.sleep(interval)
        except IOError, e:
            print 'No such file or directory: %s' % e
            time.sleep(interval)
if __name__ == "__main__":
    extra_dirs = ['/templates',]
    try:
        thread.start_new_thread( getDataJson, ("Thread-1", 2, ) )
    except:
        print "Error: unable to start thread"
    app.run(debug=True)
