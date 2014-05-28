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

class Point:
    time = 0
    value = 0.
    def __init__(self, t, v):
        self.time = t
        self.value = v

class CLDatabase(object):
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
    def select_range(self, first, last):
        print("not implemented")
        
class CLSQLite(CLDatabase):
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

    def select_range(self, first, last):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        c.execute('SELECT * FROM points WHERE time > ' + first + ' and time < ' + last)

        conn.commit()
        conn.close()

class CLInflux(CLDatabase):
    # ordered desc by default
    # time in microseconds (1000 milliseconds)
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
        self._influxClient.query('select * from danjou order asc limit 1')

    def select_last(self):
        self._influxClient.query('select * from danjou limit 1')

    def select_range(self, first, last):
        self._influxClient.query('select * from json_points where time > '+first+' and time < '+last+'')
       
class CLTempo(CLDatabase):
    # some private vars
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
        resp = self._client.write_key(self.SERIES_KEY, data)

    def select_first(self):
        self._client.single_value(self.SERIES_KEY, datetime.datetime(1950, 1, 1), 'nearest')
        
    def select_last(self):
        self._client.single_value(self.SERIES_KEY, datetime.datetime(2050, 1, 1), 'nearest')

    def select_range(self, first, last):
        # time format ISO 8601
        self._client.read_data(self.SERIES_KEY, first, last)
    

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

sqlite = CLSQLite()
sqlite.insert_range(points)

influx = CLInflux()
#influx.insert_range(points)

tempo = CLTempo()
#tempo.insert_range(points)

# utc timestamp
first = '1388535600'
last = '1388537100'

# ISO 8601
start = '2014-01-01T00:00:00Z'
end = '2014-01-01T03:00:00Z'

tic = time.time()
tempo.select_range(start, end)
toc = time.time()
print(toc-tic)


