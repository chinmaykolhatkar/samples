This python script needs websocket-client tnd urllib2 o be installed.
Install using following command:

```
sudo pip install websocket-client urllib2
```

To run the script you need to know following things:
1. Pubsub server's IP:PORT. For eg in case of DTGateway it would be "localhost:9090".
2. appId for the application which you're interested in listening

To run the python script use following command:
```
python pubsub.py <host:port> <appId>
```

For eg:
```
python pubsub.py localhost:9090 application_1496047982254_0034
```

The script will query all the datasources for give application id and fire queries towards all the datasources.
The result will be logged in files named as pubsub query id of the query.

