import os
import shutil
import sys
import requests
import time

import json
from contextlib import closing
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

class Onav_Proiler():

    def __init__(self, url):
        self.base_url = url        
        self.test_point = [[45,-45]]
        self.test_line = [[45,-45], [40,-40]]
        self.test_area = [[45,-45], [40,-45], [40,-40], [45,-40]]
        self.datasets = self.get_datasets()

    def send_req(self, url):
        # print(url)
        # print(time.time())
        resp = requests.get(url, timeout=30)

        if resp.status_code == 200:
            # print(time.time())
            # print('Response recieved.')
            return resp
        else:
            print('Request failed.')  
            return 

    def get_datasets(self):
        data = self.send_req(self.base_url + 'datasets/')
        return [d for d in json.loads(data.content)]

    def get_variables(self, dataset):
        data = self.send_req(self.base_url + f'variables/?dataset={dataset}')
        return [d for d in json.loads(data.content)]

    def get_timestamps(self, dataset, variable): 
        data = self.send_req(self.base_url + f"timestamps/?dataset={dataset}&variable={variable}")  
        return [d['id'] for d in json.loads(data.content)]        

    def get_depths(self, dataset, variable):
        data = self.send_req(self.base_url +f"depth/?dataset={dataset}&variable={variable}")
        if data:
            return [d['id'] for d in json.loads(data.content)] 
        else:
            return '**Failed**'

    def get_tile(self, dataset, variable, time, depth, x, y):
        return 

    def profile_plot(self, dataset, quantum, station, time, variable):
        #http://localhost:5000/api/v1.0/plot/?query=%7B%22dataset%22%3A%22giops_day%22%2C%22names%22%3A%5B%5D%2C%22plotTitle%22%3A%22%22%2C%22quantum%22%3A%22day%22%2C%22showmap%22%3Afalse%2C%22station%22%3A%5B%5B30.313746696469252%2C-50.76904296874999%5D%5D%2C%22time%22%3A2268432000%2C%22type%22%3A%22profile%22%2C%22variable%22%3A%5B%22votemper%22%5D%7D&format=json
        query = {"dataset":self.datasetDict[self.datasetCB.currentText()],
                "names":[],
                "plotTitle":"",
                "quantum":self.quantum,
                "showmap":0,
                "station":[p],
                "time":self.timestampDict[self.profileStartTimeCB.currentText()],
                "type":"profile",
                "variable":[self.variableDict[self.variableCB.currentText()]]
                }
        self.send_req(self.base_plot_url + urlencode({'query': json.dumps(query)}) + '&format=json')

    def run(self):
        for ds in self.datasets:
            print('*****************************************************************************************************')
            print(ds['value'])
            print('*****************************************************************************************************')
            variables = self.get_variables(ds['id']) 
            for v in variables:   
                timestamps = self.get_timestamps(ds['id'], v['id'])
                depths = self.get_depths(ds['id'], v['id'])

                depth_msg = []
                if isinstance(depths, str):
                    depth_msg = depths
                else:
                    depth_msg = str(len(depths))

                print(v['value'] + ' depths length: ' + depth_msg)
                


                



        return

if __name__ == '__main__':
    # test_suite = Onav_Proiler('http://lxc-on-02.ent.dfo-mpo.ca:5000/api/v1.0/')
    test_suite = Onav_Proiler('http://navigator.oceansdata.ca/api/v1.0/')
    test_suite.run()
    