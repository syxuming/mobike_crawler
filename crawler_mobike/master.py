import datetime
import os,os.path
import random
import threading
import time
import ujson
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import requests
from retrying import retry
from modules.ProxyProvider import ProxyProvider

class Crawler:
    def __init__(self):
        self.csv_path = './db/' + datetime.datetime.now().strftime('%Y%m%d')
        os.makedirs(self.csv_path,exist_ok=True)
        self.csv_name = self.csv_path + '/' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + '.csv'
        self.lock = threading.Lock()
        self.proxyProvider = ProxyProvider()


    def get_nearby_bikes(self,args):
        try:
            url = 'https://mwx.mobike.com/mobike-api/rent/nearbyBikesInfo.do'
            payload = 'latitude=%s&longitude=%s&errMsg=getMapCenterLocation' % (args[0],args[1])
            headers = {
                'charset':'utf-8',
                'platform':'4',
                'referer':'https://servicewechat.com/wx40f112341ae33edb/1/',
                'content-type':'application/x-www-form-urlencoded',
                'user-agent':'MicroMessenger/6.5.4.1000 NetType/WIFI Language/zh_CN',
                'host':'mwx.mobike.com',
                'connection':'Keep-Alice',
                'accept-encoding':'gzip',
                'cache-control':'no-cache'
            }

            self.request(headers,payload,url)
        except Exception as ex:
            print(ex)

    def request(self,headers,payload,url):
        while True:
            proxy = self.proxyProvider.pick()
            try:
                response = requests.request(
                    'POST',url,data=payload,headers=headers,
                    proxies={'https':proxy.url},
                    timeout=5
                )
                with self.lock:
                    try:
                        #print(response.text)
                        decoded = ujson.decode(response.text)['object']
                        with open(self.csv_name, 'a') as f:
                            for x in decoded:
                                f.write('%s,%s,%s,%s,%s,%s,%s,%s\n'% (
                                    datetime.datetime.fromtimestamp(int(time.time())),
                                    x['bikeIds'], x['biketype'],x['distId'],x['distNum'], x['type'],
                                    x['distX'],x['distY']))

                    except Exception as ex:
                        print(ex)
                break
            except Exception as ex:
                proxy.fatal_error()

    def start(self):
        left = 30.800307
        top = 120.718722
        right = 30.730301
        bottom = 120.80151

        offset = 0.002

        executor = ThreadPoolExecutor(max_workers=250)
        print('Start')
        lat_range = np.arange(left,right,-offset)
        for lat in lat_range:
            lon_range = np.arange(top,bottom,offset)
            for lon in lon_range:
                executor.submit(self.get_nearby_bikes,(lat,lon))

        executor.shutdown()



Crawler().start()