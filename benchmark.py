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
#from tempodb import client
#from tempodb.protocol import DataPoint
from tempodb import Client, DataPoint

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
    def select_range(self, first, last):
        print("not implemented")
    def select_first(self):
        print("first")
    def select_last(self):
        print("last")
        
class CLSQLite(CLDatabase):
    def __init__(self):
        conn = ''
        c = ''
        #this should delete the db if it exists
        #or connect to the db and delete the pts if there are any

    def insert_range(self, points_array):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS 'points' ('time' INTEGER, 'value' INTEGER)''')
        #i = 0
        #while i < 5:
            #z = (points[i].time , points[i].value)
            #c.execute('INSERT INTO points (time, value) VALUES (?, ?)', z)
            #i = i + 1
        for p in points_array:
            z = (p.time , p.value)
            c.execute('INSERT INTO points (time, value) VALUES (?, ?)', z)

        conn.commit()
        conn.close()

    def select_range(self, first, last):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        first = c.execute('SELECT * FROM points WHERE time > ' + first + ' and time < ' + last)

        conn.commit()
        conn.close()

    def select_first(self):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        first = c.execute('''SELECT * FROM points ORDER BY time ASC LIMIT 1''')

        conn.commit()
        conn.close()

    def select_last(self):
        conn = sqlite3.connect("grapes.db")
        c = conn.cursor()
        first = c.execute('''SELECT * FROM points ORDER BY time DESC LIMIT 1''')

        conn.commit()
        conn.close()       

class CLInflux(CLDatabase):
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
       
class CLTempo(CLDatabase):
    # some private vars
    API_KEY = ''
    DATABASE_ID = ''
    API_SECRET = ''
    SERIES_KEY = ''
    _client = None
    
    def __init__(self):
        # get api key, secret, connect to tempo server, create series
        self.API_KEY = '444011c85ac2426fb5f9bebc52d13298'
        self.API_SECRET = '410c22c673584f568e01ac471d5570dc'
        self.DATABASE_ID = 'dev-test'
        self.SERIES_KEY = 'fruit'
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
        print ('Temo Response code: ', resp.status)
    


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

first = '1388535600'
last = '1388537100'
sqlite = CLSQLite()
#sqlite.insert_range(points)
tic = time.time()
sqlite.select_range(first, last)
toc = time.time()
print(toc-tic)


#influx = CLInflux()
#influx.insert_range(points)

#tempo = CLTempo()
#tempo.insert_range(points)
