# rail_launch.py

import datetime
import time
import os
import json
import dbconnect
import commonfuncs as cf
import uuid
import pandas as pd
from tornado import escape
from tornado import concurrent
import tornado.web
import tornado.ioloop

import api1
import api_v2

portnum = 8000

print("Loading dependencies")
cf.logmessage("All dependences loaded.")

root = os.path.dirname(__file__)  # needed for tornado

# from https://stackoverflow.com/a/55762431/4355695 : restrict direct browser access to .py files and stuff


class MyStaticFileHandler(tornado.web.StaticFileHandler):
    def set_extra_headers(self, path):
        # https://stackoverflow.com/a/12031093/4355695
        self.set_header("Cache-control", "no-cache")

    def validate_absolute_path(self, root, absolute_path):
        page = os.path.basename(absolute_path)
        # do blocking as necessary
        # print(absolute_path, page)

        if 'config' in absolute_path:
            cf.logmessage('caught snooping!')
            return os.path.join(root, 'redirect.html')
        return super().validate_absolute_path(root, absolute_path)  # you may pass


class WelcomeAPIHandler(cf.BaseHandler):
    def get(self):
        self.write({'message': 'Welcome to the AirQuality API hey!'})


class Application(tornado.web.Application):
    _routes = [
        # tornado.web.url(r"/API/fetchBreezoMap", api1.fetchBreezoMap),
        tornado.web.url(r"/", WelcomeAPIHandler),
        tornado.web.url(r"/API/wardsAndMonitors", api_v2.getWardsAndMonitors),
        tornado.web.url(r"/API/wardCentroids", api_v2.getWardCentroids),
        tornado.web.url(r"/API/wardPolygons", api_v2.getGeoMappedPollutant),
        tornado.web.url(r"/API/rankedPm25Units", api_v2.getPm25Ranks),
        tornado.web.url(r"/API/getWardOrMonitorHistory",
                        api_v2.getWardOrMonitorPollutantHistory),
        tornado.web.url(r"/API/wardOrMonitorSummary",
                        api_v2.getWardOrMonitorSummary),
        tornado.web.url(r"/API/pollutantHistory",
                        api_v2.getPollutantHistory),
        tornado.web.url(r"/API/getLatestData", api1.getLatestData),
        tornado.web.url(r"/(.*)", MyStaticFileHandler,
                        {"path": root, "default_filename": "redirect.html"}),
        # tornado.web.url(r"/index.html", TemplateHandler)
    ]

    def __init__(self):
        settings = {
            "debug": False,  # make this false when pushing to openshift
            "cookie_secret": "jkgujdrjfsdfgdsftryhlfg",
            "compress_response": True  # https://stackoverflow.com/a/11872086/4355695
        }
        super(Application, self).__init__(self._routes, **settings)


if __name__ == "__main__":
    app = Application()
    app.listen(port=portnum)
    print(f"Launched on port {portnum}")
    tornado.ioloop.IOLoop.current().start()
