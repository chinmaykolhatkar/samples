import urllib2, json, sys, websocket, thread, time, random, os, signal

m = {}
queries = {}

def getRandomRealNumber():
    return str(random.uniform(0, 1.0))

def createSubscribeSchemaQuery(ads, no):
    q = {}
    q['type'] = 'subscribe'
    q['topic'] = ads['result']['topic'] + '.' + no
    return json.dumps(q)

def createPublishSchemaQuery(ads, no):
    d = {}
    d['id'] = float(no)
    d['type'] = 'schemaQuery'
    d['context']  = ads['context']
    q = {}
    q['type'] = 'publish'
    q['topic'] = ads['query']['topic']
    q['data'] = d

    return json.dumps(q)

def createSubscribeDataQuery(ads, sq, no):
    q = {}
    q['type'] = 'subscribe'
    q['topic'] = ads['result']['topic'] + '.' + no
    return json.dumps(q)

def createPublishDataQuery(ads, sq, no):
    q = {}
    q['type'] = 'publish'
    q['topic'] = ads['query']['topic']
    q['data'] = {}
    q['data']['id'] = float(no)
    q['data']['type'] = 'dataQuery'
    q['data']['countdown'] = 299
    q['data']['incompleteResultOK'] = True
    q['data']['data'] = {}
    q['data']['data']['schemaKeys'] = ads['context']['schemaKeys']
    q['data']['data']['fields'] = []
    v = sq['data']['data'][0]['values']
    for i in v:
        q['data']['data']['fields'].append(i['name'])
    return json.dumps(q)

def sendQueries(ws, r, i):
    ws.send(createSubscribeSchemaQuery(i, r))
    ws.send(createPublishSchemaQuery(i, r))
    result = json.loads(ws.recv())
    r = getRandomRealNumber()
    ws.send(createSubscribeDataQuery(i, result, r))
    ws.send(createPublishDataQuery(i, result, r))

def getFileWriter(topic):
    if topic not in m:
        m[topic] = open(os.getcwd() + '/' + topic, 'w')
    return m[topic]


url = "http://" + sys.argv[1] + "/ws/v2/applications/" + sys.argv[2]
content = urllib2.urlopen(url).read()

j = json.loads(content)
appDataSources = j['appDataSources']

wsUrl = "ws://" + sys.argv[1] + "/pubsub"

ws = websocket.create_connection(wsUrl)

for i in appDataSources:
    r = getRandomRealNumber()
    sendQueries(ws, r, i)
    queries[r] = i

while True:
    result = ws.recv()
    r = json.loads(result)
    parts = r['topic'].split(".")
    t = parts[1] + '.' + parts[2]
    f = getFileWriter(t)
    f.write(result + '\n')
    f.flush()
    if int(r['data']['countdown']) == 1:
        id = r['data']['id']
        sendQueries(ws, id, queries[id])

ws.close()
for i in m:
    m[i].close()



# def on_message(ws, message):
#     print message

# def on_error(ws, error):
#     print error

# def on_close(ws):
#     print "### closed ###"

# def on_open(ws):
#     print "### opening ###"
#     def run(*args):
#         print "SEnding publish query"
#         ws.send('{"type":"subscribe","topic":"DTAppMetricsResult.0.36096280516265744"}')
#         ws.send('{"type":"publish","topic":"DTAppMetricsQuery","data":{"id":0.36096280516265744,"type":"dataQuery","data":{"fields":["percentFiltered","avgRecordSize"],"schemaKeys":{"appName":"Metric-Primitives-App-CCP","appUser":"chinmay","logicalOperatorName":""},"time":{"latestNumBuckets":10}},"countdown":299,"incompleteResultOK":true}}')
#         print "SEnding data query"
#         time.sleep(30000)
#         ws.close()
#         print "thread terminating..."
#     thread.start_new_thread(run, ())


# # for i in range(len(appDataSources)):
# #   print "DS " + str(i)
# #   print appDataSources[i]

# wsUrl = "ws://" + sys.argv[1] + "/pubsub"

# ws = websocket.WebSocketApp(wsUrl,
#                             on_message = on_message,
#                             on_error = on_error,
#                             on_close = on_close)
# ws.on_open = on_open
# ws.run_forever()
