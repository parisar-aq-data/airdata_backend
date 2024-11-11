#!/usr/bin/env python
##
# API Version 2
# The API is being fleshed out more to support the front end.

# author Shweta Purushe
##

import dbconnect
import commonfuncs as cf
import datetime
import time
import os
import json
from tornado import escape
from tornado import concurrent
import tornado.web

class GetAqdata1(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def get(self):
        status, result = yield self.get_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def get_func(self):

      
        query = f"""
                select * from aqdata1
        """
        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)
        
class GetAqdata2(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def get(self):
        status, result = yield self.get_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def get_func(self):

      
        query = f"""
                select * from aqdata2
        """
        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)

class GetAqdata3(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def get(self):
        status, result = yield self.get_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def get_func(self):

      
        query = f"""
                select * from aqdata3
        """
        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)
        
class GetLocations(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def get(self):
        status, result = yield self.get_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def get_func(self):

      
        query = f"""
                select * from locations
        """
        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)
        
class GetMarathiName(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def get(self):
        status, result = yield self.get_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def get_func(self):

      
        query = f"""
                select * from WardMarathiNameMap
        """
        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)