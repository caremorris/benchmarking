#!/usr/local/bin/python
# coding: utf-8

"""
use with python 2.7, and tempodb v1.0 library

"""

import os, sys
import time, datetime
import random
import timeit
from collections import OrderedDict
import tabulate
import csv
import sqlite3
import influxdb
from influxdb import client as _influxClient
import tempodb
from tempodb import client
from tempodb.protocol import DataPoint
import xively

class Point:
    time = 0
    value = 0.
    def __init__(self, t, v):
        self.time = t
        self.value = v

class CL_Database(object):
    name = ""
    def __init__(self):
        print("I'm a new db!")
    def insert_range(self, points_array):
        for i in points_array:
            print(i.time, i.value)
    def select_first(self):
        print("first")
    def select_last(self):
        print("last")
    def select_range(self, start, end):
        print("not implemented")
        
class CL_SQLite(CL_Database):
    def __init__(self):
        conn = ''
        c = ''
        # I tried to setup the connection & cursor here, and delete them from the next 4 methods,
        # but resulted in "NameError: global name 'c' is not defined"

    def insert_range(self, points_array):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS 'points' ('time' INTEGER, 'value' INTEGER)''')
        for p in points_array:
            z = (p.time , p.value)
            c.execute('INSERT INTO points (time, value) VALUES (?, ?)', z)

        conn.commit()
        conn.close()

    def select_first(self):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        c.execute('''SELECT * FROM points ORDER BY time ASC LIMIT 1''')

        conn.commit()
        conn.close()

    def select_last(self):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        c.execute('''SELECT * FROM points ORDER BY time DESC LIMIT 1''')
        
        conn.commit()
        conn.close()

    def select_range(self, start, end):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        start=str(start)
        end=str(end)
        c.execute('SELECT * FROM points WHERE time > '+start+' and time < '+end)

        conn.commit()
        conn.close()

class CL_Influx(CL_Database):
    _influxClient = influxdb.InfluxDBClient()
    def __init__(self):
        host = 'sandbox.influxdb.com'
        port = 8086
        username = 'cl_testing'
        password = 'pass123'
        dbname = 'peaches'
        self._influxClient = influxdb.InfluxDBClient(host, port, username, password, dbname)

        # delete series if it's already there
        self._influxClient.query('drop series melon')
        # this removes all data from the series and removes it from the list of series in the db
        
    def insert_range(self, points_array):
        json_points = []
        for p in points_array:
            json_points.append([p.time , p.value])
            
        data = [{
                "points": json_points,
                "columns" : ["time", "value"],
                "name" : "melon"
                }]
            
        self._influxClient.write_points(data)

    def select_first(self):
        # ordered desc by default
        self._influxClient.query('select * from melon order asc limit 1')

    def select_last(self):
        self._influxClient.query('select * from melon limit 1')

    def select_range(self, start, end):
        # time in epoch time + suffix (or nanoseconds + no suffix)
        start = str(start)+'s'
        end = str(end)+'s'
        self._influxClient.query('select * from melon where time > '+start+' and time < '+end+'')
       
class CL_Tempo(CL_Database):
    API_KEY = ''
    DATABASE_ID = ''
    API_SECRET = ''
    SERIES_KEY = ''
    _client = None
    
    def __init__(self):
        # get api key, secret, connect to tempo server, create series
        self.API_KEY = '3f88a674a3a94dc7b08ac07858c5c8fe'
        self.API_SECRET = 'c36ea269d1494a77ba1550794f7169f0'
        self.DATABASE_ID = 'CLPOINTS'
        self.SERIES_KEY = 'apple'
        self._client = tempodb.client.Client(self.DATABASE_ID, self.API_KEY, self.API_SECRET)
        # delete series if it's already there.
        self._client.delete_series(self.SERIES_KEY)
        # create new series.
        self._client.create_series(self.SERIES_KEY)
        
    def insert_range(self, points_array):
        data = []
        # put the points into the right format
        for point in points_array:
            pointDateTime = datetime.datetime.utcfromtimestamp(point.time)
            tempoPt = DataPoint.from_data(pointDateTime, point.value)
            data.append(tempoPt)
        # split data into chunks of 100 points
        for i in range(0, len(data), 100):
            chunk = data[i:i+100]
            # write points out
            self._client.write_data(self.SERIES_KEY, chunk)
        # if len(data) is not a multiple of 100, we have some points left over
        r = len(data)%100
        last_chunk = data[-r:]
        self._client.write_data(self.SERIES_KEY, chunk)

    def select_first(self):
        self._client.single_value(self.SERIES_KEY, datetime.datetime(1950, 1, 1), 'nearest')
        
    def select_last(self):
        self._client.single_value(self.SERIES_KEY, datetime.datetime(2050, 1, 1), 'nearest')

    def select_range(self, start, end):
        ## convert from timestamp to ISO 8601
        start = datetime.datetime.utcfromtimestamp(start)
        end = datetime.datetime.utcfromtimestamp(end)
        self._client.read_data(self.SERIES_KEY, start, end)

class CL_Xively(CL_Database):
    api=None
    feed=None
    datastream=None
    def __init__(self):
        self.api = xively.XivelyAPIClient("kWBAwJLxWde90LutdSTue1W6sKEyFyFNh6KTH3sjK9ce162W")
        self.feed = self.api.feeds.get(1128532470)
        self.datastream = self.feed.datastreams.get("fruit")
    def insert_range(self, points_array):
        step_size = 1000 # Xively's maximum 1000 data-point per request requirement
        for i in range(0, len(points_array), step_size):
            datapts = []
            for point in points_array[i:i+step_size]:
                time = datetime.datetime.utcfromtimestamp(point.time)
                xive_point = xively.Datapoint(at=str(time), value=point.value)
                datapts.append(xive_point)
            self.datastream.datapoints = datapts
            self.datastream.update()
        print("inserted")
    def select_first(self):
        datapoint = self.datastream.datapoints.get(at=datetime.datetime.utcfromtimestamp(1388535000))
        # we can do datapoint.value, datapoint.time
    def select_last(self):
        print("selected last")
    def select_range(self, startTime, endTime):
        s = datetime.datetime.utcfromtimestamp(startTime)
        e = datetime.datetime.utcfromtimestamp(endTime)
        datapoints = self.datastream.datapoints.history(start = s, end = e)  # fetch and return a list of datapoints
        for i in datapoints:
            print(i)
            
########################
####  TESTING CODE  ####
########################


week = [[], []]
month = [[], []]
year = [[], []]
calendar_units = [week, month, year]
for i in calendar_units:
    yyyy = 2014
    mm = 1
    dd = 1
    date1 = date2 = datetime.datetime(yyyy, mm, dd) # date1 for 5 minute intervals, date2 for 1 hour intervals
    if i == week:
        dd = dd + 7
    elif i == month:
        if mm != 12:
            mm = mm + 1
        else:
            mm = 1
            yyyy = yyyy + 1
    else:
        yyyy = yyyy + 1
    next_date = datetime.datetime(yyyy, mm, dd) # either 1 week, 1 month, or 1 year into future
    while date1 < next_date:
        timestamp = (date1 - datetime.datetime(1970, 1, 1)).total_seconds() 
        i[0].append(Point(timestamp, random.randint(1,100))) # append Point to week/month/year[0]
        date1 = date1 + datetime.timedelta(minutes=5) # change date1 // 5 minute intervals
    while date2 < next_date:
        timestamp = (date2 - datetime.datetime(1970, 1, 1)).total_seconds()
        i[1].append(Point(timestamp, random.randint(1,100))) # append Point to week/month/year[1]
        date2 = date2 + datetime.timedelta(minutes=60) # change date2 // 1 hour intervals

pt1 = week[0][0] #1388534400.0
pt2 = week[0][5] #1388535900.0

# pt2.time - pt1.time = 1500

hr = 3600
day = 86400
wk = 604800
mo = 2629743
yr = 31556926 

pt0 = pt1.time - mo
# can go back in time one week + six days
# when going back in time over two weeks: "HTTPError: 403 Client Error: Forbidden"
# 

xive = CL_Xively()
xive.select_range(pt0, pt2.time - mo)

   
start = 1388535000
end = 1388535000 + 300*5
table = []
list_of_data = [week[0], week[1], month[0], month[1], year[1], year[0]]
databases = ['CL_SQLite', 'CL_Influx', 'CL_Tempo', 'CL_Xivley']
for db in databases:
    for data in list_of_data:
        setup_str = "from __main__ import "+db+", data, start, end; dbObj = "+db+"()"

        sel_str = "dbObj.insert_range(data)"
        insert_time = timeit.timeit(sel_str, setup_str, number=1)
    
        sel_str = "dbObj.select_first()"
        sel_first_time = timeit.timeit(sel_str, setup_str, number=1)
    
        sel_str = "dbObj.select_last()"
        sel_last_time = timeit.timeit(sel_str, setup_str, number=1)

        sel_str = "dbObj.select_range(start, end)"
        sel_range_time = timeit.timeit(sel_str, setup_str, number=1)

        num_pts = len(data)

        # deleted 'db' from first spot in 'row' array for csv
        row = [num_pts, insert_time, sel_first_time, sel_last_time, sel_range_time]
        table.append(row)
        
    # export table as a csv
    with open('/Users/carolyn/'+db+'.csv', 'w') as f:
        w = csv.writer(f, delimiter=',',)
        w.writerow(['points', 'insert', 'first', 'last', 'range'])
        for i in table:
            w.writerow(i)
            
    table = []
    print(db + ": check")

### pretty table ###
headers = ["DB Name", "# of Pts","Insert", "Sel First", "Sel Last", "Sel Range"]
print(tabulate.tabulate(table, headers=headers, tablefmt="rst"))
    
# with tempo trial you are limited by the number of points you can insert
# at one time.  it's 26,942.
