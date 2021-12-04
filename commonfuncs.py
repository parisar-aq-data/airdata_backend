#commonfuncs.py
import json, os, time, datetime
import pandas as pd
import tornado.web
from tornado import concurrent

root = os.path.dirname(__file__)
timeOffset = 5.5
maxThreads = 8

logFolder = os.path.join(root,'logs')
os.makedirs(logFolder, exist_ok=True)


def logmessage( *content ):
    global timeOffset
    timestamp = '{:%Y-%m-%d %H:%M:%S} :'.format(datetime.datetime.utcnow() + datetime.timedelta(hours=timeOffset)) # from https://stackoverflow.com/a/26455617/4355695
    line = ' '.join(str(x) for x in list(content)) # from https://stackoverflow.com/a/3590168/4355695
    print(line) # print to screen also
    with open(os.path.join(logFolder,'log.txt'), 'a') as f:
        print(timestamp, line, file=f) # file=f argument at end writes to file. from https://stackoverflow.com/a/2918367/4355695

def logmessageBreezo( *content ):
    global timeOffset
    timestamp = '{:%Y-%m-%d %H:%M:%S} :'.format(datetime.datetime.utcnow() + datetime.timedelta(hours=timeOffset)) # from https://stackoverflow.com/a/26455617/4355695
    line = ' '.join(str(x) for x in list(content)) # from https://stackoverflow.com/a/3590168/4355695
    print(line) # print to screen also
    with open(os.path.join(logFolder,'breezo_log.txt'), 'a') as f:
        print(timestamp, line, file=f) # file=f argument at end writes to file. from https://stackoverflow.com/a/2918367/4355695


def logmessageIUDX( *content ):
    global timeOffset
    timestamp = '{:%Y-%m-%d %H:%M:%S} :'.format(datetime.datetime.utcnow() + datetime.timedelta(hours=timeOffset)) # from https://stackoverflow.com/a/26455617/4355695
    line = ' '.join(str(x) for x in list(content)) # from https://stackoverflow.com/a/3590168/4355695
    print(line) # print to screen also
    with open(os.path.join(logFolder,'iudx_log.txt'), 'a') as f:
        print(timestamp, line, file=f) # file=f argument at end writes to file. from https://stackoverflow.com/a/2918367/4355695

def logmessageSafar( *content ):
    global timeOffset
    timestamp = '{:%Y-%m-%d %H:%M:%S} :'.format(datetime.datetime.utcnow() + datetime.timedelta(hours=timeOffset)) # from https://stackoverflow.com/a/26455617/4355695
    line = ' '.join(str(x) for x in list(content)) # from https://stackoverflow.com/a/3590168/4355695
    print(line) # print to screen also
    with open(os.path.join(logFolder,'safar_log.txt'), 'a') as f:
        print(timestamp, line, file=f) # file=f argument at end writes to file. from https://stackoverflow.com/a/2918367/4355695

def logmessageMpcb( *content ):
    global timeOffset
    timestamp = '{:%Y-%m-%d %H:%M:%S} :'.format(datetime.datetime.utcnow() + datetime.timedelta(hours=timeOffset)) # from https://stackoverflow.com/a/26455617/4355695
    line = ' '.join(str(x) for x in list(content)) # from https://stackoverflow.com/a/3590168/4355695
    print(line) # print to screen also
    with open(os.path.join(logFolder,'mpcb_log.txt'), 'a') as f:
        print(timestamp, line, file=f) # file=f argument at end writes to file. from https://stackoverflow.com/a/2918367/4355695


def makeError(message):
    logmessage(message)
    return 400, json.dumps({"status":"error","message":message}, default=str)

def makeSuccess(returnD={}):
    returnD['status'] = 'success'
    return 200, json.dumps(returnD, default=str)


def makeTimeString(x, offset=5.5, format="all"):
    '''
    format values: all, time, date
    '''
    # print(type(x))
    if isinstance(x, pd._libs.tslibs.nattype.NaTType) : return ''
    
    if isinstance(x, (pd._libs.tslibs.timestamps.Timestamp,datetime.datetime, datetime.date) ):
        if format == 'time':
            return (x + datetime.timedelta(hours=offset)).strftime('%H:%M:%S')
        elif format == 'date':
            return (x + datetime.timedelta(hours=offset)).strftime('%Y-%m-%d')
        else:
            # default: all
            return (x + datetime.timedelta(hours=offset)).strftime('%Y-%m-%d %H:%M')
    else:
        return ''


def quoteNcomma(a):
    # turn array into sql IN query string: 'a','b','c'
    holder = []
    for n in a:
        holder.append("'{}'".format(n))
    return ','.join(holder)


def keyedJson(df, key='trainNo'):
    arr = df.to_dict(orient='records')
    keysList = sorted(df[key].unique().tolist())
    returnD = {}
    for keyVal in keysList:
        returnD[keyVal] = df[df[key]==keyVal].to_dict(orient='records')
    return returnD
    

def getDate(offset=5.5):
    return (datetime.datetime.utcnow()+ datetime.timedelta(hours=offset)).strftime('%Y-%m-%d')

def IRdateConvert(x):
    # sample: "26 Feb 2021", "4 Mar 2021", "-"
    if x == '-': return None
    x2 = datetime.datetime.strptime(x, '%d %b %Y').strftime('%Y-%m-%d')
    return x2


################# API CALL HANDLERS ##########
# common cors settings to be inherited by other API handling classes
# from https://stackoverflow.com/a/42435732/4355695
class BaseHandler(tornado.web.RequestHandler):
    executor = concurrent.futures.ThreadPoolExecutor(maxThreads)

    @tornado.gen.coroutine
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with,x-access-token,content-type,authorization") # multiple headers, from https://stackoverflow.com/a/49504274/4355695
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Credentials', 'true')

    @tornado.gen.coroutine
    def options(self):
        self.set_status(204) # for pre-flight request in POST call. from https://stackoverflow.com/a/35259440/4355695
        self.finish()
