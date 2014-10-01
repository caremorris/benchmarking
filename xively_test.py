import xively
api = xively.XivelyAPIClient("kWBAwJLxWde90LutdSTue1W6sKEyFyFNh6KTH3sjK9ce162W")
feed = api.feeds.get(1128532470)
datastream = feed.datastreams[0]
print(datastream)
# First create the datapoints.
datastream.datapoints = [
     xively.Datapoint(at="2010-05-20T11:01:43Z", value=294),
     xively.Datapoint(at="2010-05-20T11:01:44Z", value=295),
     xively.Datapoint(at="2010-05-20T11:01:45Z", value=296),
     xively.Datapoint(at="2010-05-20T11:01:46Z", value=297),
]

# Then send them to the server.
datastream.update()

