import flask
from flask import Flask, jsonify, render_template, request

import requests

import json
#import fetcher

# open stream for confirmation
import urllib

# boto SQS 
import boto.sqs
from boto.sqs.message import Message


application = Flask(__name__)

# global list 
# resultsFromSQS = []


# SNS HTTP request endpoint: subscribe, unsubscribe, notification
@application.route('/minions', methods = ['POST','GET'])
def sns():
    headers = request.headers
    print "headers", headers
    print "request", request
    arn = headers.get('X-Amz-Sns-Topic-Arn')
    print "before request.data"
    obj = json.loads(request.data)
    if arn:
        snsType = headers.get('X-Amz-Sns-Message-Type')
        if snsType == 'SubscriptionConfirmation':
            subscribe_url = obj[u'SubscribeURL']
            f = urllib.urlopen(subscribe_url)
            myfile = f.read()
            print myfile
            return '', 200
        elif snsType == 'Notification':
            notification_id = obj[u'MessageId']
            message = obj[u'Message']
            print message
            conn = boto.sqs.connect_to_region("us-east-1")
            q = conn.get_queue('kitkat_SQS')
            print "made connection"
            # global resultsFromSQS
            tempSQS = []
            for i in range(3):
                rs = q.get_messages(message_attributes=['text','sentimentStat','geoLat','geoLong'])
                text = rs[0].message_attributes['text']['string_value']
                geoLat = rs[0].message_attributes['geoLat']['string_value']
                geoLong = rs[0].message_attributes['geoLong']['string_value']
                sentimentStat = rs[0].message_attributes['sentimentStat']['string_value']
                print "SQS text = ", text
                tempSQS.append({
                    'text': text,
                    'geoLat': geoLat,
                    'geoLong': geoLong,
                    'sentimentStat': sentimentStat
                    })

            print "tempSQS = ", tempSQS
            url = 'http://160.39.131.181:9090/kitkat'
            data = {'data1': tempSQS[0], 'data2': tempSQS[1], 'data3': tempSQS[2]}
            print "data = ", data
            # return '', 200
            r = requests.post(url, data=json.dumps(data))
            print "r.content", r.content
            return r.content
        print "before OK"
        return 'OK'



if __name__ == '__main__':
	try: 
		application.config["DEBUG"] = True
		application.run(host='0.0.0.0', port=9999, threaded=True)
		#application.run(host='199.58.86.213', port=5000)


	except:
		print "application.run failed"

