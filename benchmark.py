#!/usr/local/bin/python
# coding: utf-8

"""
use with python 2.7, and tempodb v1.0 library

"""


import os, sys
import time, datetime

import random

import sqlite3

from influxdb import client as _influxClient
import influxdb

import tempodb
from tempodb import client
from tempodb.protocol import DataPoint

import timeit

from collections import OrderedDict

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
        dbname = 'pineapple'
        self._influxClient = influxdb.InfluxDBClient(host, port, username, password, dbname)     
        
    def insert_range(self, points_array):
        json_points = []
        for p in points_array:
            json_points.append([p.time , p.value])
            
        data = [{
                "points": json_points,
                "columns" : ["time", "value"],
                "name" : "danjou"
                }]
            
        self._influxClient.write_points(data)

    def select_first(self):
        # ordered desc by default
        self._influxClient.query('select * from danjou order asc limit 1')

    def select_last(self):
        self._influxClient.query('select * from danjou limit 1')

    def select_range(self, start, end):
        # time in microseconds (1000 milliseconds)
        start = start*1000
        end = end*1000
        start = str(start)
        end = str(end)
        self._influxClient.query('select * from json_points where time > '+start+' and time < '+end+'')
       
class CL_Tempo(CL_Database):
    API_KEY = ''
    DATABASE_ID = ''
    API_SECRET = ''
    SERIES_KEY = ''
    _client = None
    
    def __init__(self):
        # get api key, secret, connect to tempo server, create series
        self.API_KEY = '70cedcd39a304c30a61607e994d8a1b1'
        self.API_SECRET = 'df900c9bb67a4fffa9127c548599f7e6'
        self.DATABASE_ID = 'fruit'
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

        # write points out    
        resp = self._client.write_data(self.SERIES_KEY, data)

    def select_first(self):
        self._client.single_value(self.SERIES_KEY, datetime.datetime(1950, 1, 1), 'nearest')
        
    def select_last(self):
        self._client.single_value(self.SERIES_KEY, datetime.datetime(2050, 1, 1), 'nearest')

    def select_range(self, start, end):
        ## convert from timestamp to ISO 8601
        start = datetime.datetime.utcfromtimestamp(start)
        #start = date1.isoformat() + 'Z'
        end = datetime.datetime.utcfromtimestamp(end)
        #end = date2.isoformat() + 'Z'
        self._client.read_data(self.SERIES_KEY, start, end)

########################
####  TESTING CODE  ####
########################
        
points = []
i=0
theDate = datetime.datetime(2014,1, 1)
for i in range(100):
    timestamp = (theDate - datetime.datetime(1970, 1, 1)).total_seconds()
    points.append(Point(timestamp, random.randint(1,100)))
    theDate = theDate + datetime.timedelta(minutes=5)

start = 1388535600
end = 1388537100
databases = ['CL_SQLite', 'CL_Influx', 'CL_Tempo']
for db in databases:
    db_name = db

    setup_str = "from __main__ import " + db_name + ", points, start, end; dbObj = " + db_name + "()"

    sel_str = "dbObj.insert_range(points)"
    insert_time = timeit.timeit(sel_str, setup_str, number=1)

    sel_str = "dbObj.select_first()"
    sel_first_time = timeit.timeit(sel_str, setup_str, number=1)

    sel_str = "dbObj.select_last()"
    sel_last_time = timeit.timeit(sel_str, setup_str, number=1)

    sel_str = "dbObj.select_range(start, end)"
    sel_range_time = timeit.timeit(sel_str, setup_str, number=1)

    results = OrderedDict()
    results['db_name'] = 'CL_SQLite'
    results['insert_time'] = insert_time
    results['sel_first_time'] = sel_first_time
    results['sel_last_time'] = sel_last_time
    results['sel_range_time'] = sel_range_time

    print("Results Object: "+db_name+"\n")
    print(results)
    print("\n\n")
