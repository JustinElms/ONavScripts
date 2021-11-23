import requests
import time
import logging

import json
from contextlib import closing
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

class ONav_API_Proiler():

    def __init__(self, base_url, config_url, enable_logging = True, max_attempts = 3):
        self.base_url = base_url     
        self.logging = logging
        self.max_attempts = max_attempts

        if enable_logging:
            logging.basicConfig(filename='api_profile_testing.log', level=logging.DEBUG)

        self.datasets = self.get_datasets()
        with open(config_url) as f:
            self.test_config = json.load(f)   

    def send_req(self, url):
        for i in range(self.max_attempts):
            logging.info('Attempt ' + str(i+1) + ':')
            start_time = time.time()
            try:
                resp = requests.get(url, timeout=120)
            except requests.ReadTimeout:
                logging.warning('*** Request timed out. ***')
                
            if resp.status_code == 200:
                end_time = time.time()
                logging.info('*** Response recieved. ***')
                logging.info('Total time: ' + str(end_time - start_time))
                return resp
            else:
                logging.warning('*** Request failed. ***')  
        logging.critical('Could not complete request after ' + str(self.max_attempts) + ' attempt(s).')

    def get_datasets(self):
        logging.info('Requesting dataset info...')
        data = self.send_req(self.base_url + 'datasets/')
        return [d for d in json.loads(data.content)]

    def get_variables(self, dataset):
        logging.info('Requesting variables...')
        data = self.send_req(self.base_url + f'variables/?dataset={dataset}')
        return [d for d in json.loads(data.content)]

    def get_timestamps(self, dataset, variable): 
        logging.info('Requesting timestamps...')
        data = self.send_req(self.base_url + f"timestamps/?dataset={dataset}&variable={variable}")  
        return [d for d in json.loads(data.content)]       

    def get_depths(self, dataset, variable):
        logging.info('Requesting depths...')
        data = self.send_req(self.base_url +f"depth/?dataset={dataset}&variable={variable}")
        return [d for d in json.loads(data.content)]

    def get_quantum(self,ds):
        ds_data = next(item for item in self.datasets if item["id"] == ds)
        return ds_data['quantum']

    def get_plot(self,query):
        logging.info('Requesting plot...')
        self.send_req(self.base_url + 'plot/?' + urlencode({'query': json.dumps(query)}) + '&format=json')

    def profile_test(self):
        logging.info('****************** Profile Plot Tests ******************')
        config = self.test_config['profile_plot']

        for ds in config['datasets']:
            logging.info("Dataset: " + ds)
            quantum = self.get_quantum(ds)
            for v in config['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                self.get_plot({
                    "dataset" : ds,
                    "names" : [],
                    "plotTitle" : "",
                    "quantum" : quantum,
                    "showmap" : False,
                    "station" : config['station'],
                    "time" : timestamps[-1]['id'], 
                    "type" : "profile",
                    "variable" : v
                })
    
    def virtual_mooring_test(self):
        logging.info('****************** Virtual Mooring Plot Tests ******************')
        config = self.test_config['vm_plot']

        for ds in config['datasets']:
            logging.info("Dataset: " + ds)
            quantum = self.get_quantum(ds)
            for v in config['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                self.get_plot({
                    "colormap" : "default",
                    "dataset" : ds,
                    "depth" : 0,
                    "endtime" : timestamps[-1]['id'],
                    "names" : [],
                    "plotTitle" : "",
                    "quantum" : quantum,
                    "scale" : "-5,30,auto",
                    "showmap" : 0,
                    "starttime" : timestamps[0]['id'], 
                    "station" : config['station'], 
                    "type" : "timeseries",
                    "variable" : v
                })

    def transect_test(self):
        logging.info('****************** Transect Plot Tests ******************')
        config = self.test_config['transect_plot']

        for ds in config['datasets']:
            logging.info("Dataset: " + ds)
            quantum = self.get_quantum(ds)
            for v in config['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                self.get_plot({
                    "colormap" : "default",
                    "dataset" : ds,
                    "depth_limit" : 0,
                    "linearthresh" : 200,
                    "name" : config['name'],
                    "path" : config['path'], 
                    "plotTitle" : "",
                    "quantum" : quantum,
                    "scale" : "-5,30,auto",
                    "selectedPlots" : "0,1,1",
                    "showmap" : 1,
                    "surfacevariable" : "none",
                    "time" : timestamps[-1]['id'], 
                    "type" : "transect",
                    "variable" : v
                })

    def hovmoller_test(self):
        logging.info('****************** Hovmoller Plot Tests ******************')
        config = self.test_config['hovmoller_plot']

        for ds in config['datasets']:
            logging.info("Dataset: " + ds)
            quantum = self.get_quantum(ds)
            for v in config['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                self.get_plot({
                    "colormap" : "default",
                    "dataset" : ds,
                    "depth" : 0,
                    "endtime" : timestamps[-1]['id'],
                    "name" : config['name'],
                    "path" : config['path'],
                    "plotTitle" : "",
                    "quantum" : quantum,
                    "scale" : "-5,30,auto",
                    "showmap" : 1,
                    "starttime" : timestamps[0]['id'],
                    "type" : "hovmoller",
                    "variable" : v
                })

    def area_test(self):
        logging.info('****************** Starting Area Plot Tests ******************')
        config = self.test_config['area_plot']

        for ds in config['datasets']:
            logging.info("Dataset: " + ds)
            quantum = self.get_quantum(ds)
            for v in config['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                self.get_plot({
                    "area":[{"innerrings" : [],
                        "name" : "",
                        "polygons" : config['polygons']}
                    ],
                    "bathymetry" : 1,
                    "colormap" : "default",
                    "contour" : {"colormap":"default",
                        "hatch" : 0,
                        "legend" : 1,
                        "levels" : "auto",
                        "variable" : "none"
                    },
                    "dataset" : ds,
                    "depth" : 0,
                    "interp" : "gaussian",
                    "neighbours" : 10,
                    "projection" : "EPSG:3857",
                    "quantum" : quantum,
                    "quiver" : {"colormap" : "default",
                        "magnitude" : "length",
                        "variable" : config["quiver_variable"]
                    },
                    "radius" : 25,
                    "scale" : "-5,30,auto",
                    "showarea" : 1,
                    "time" : timestamps[-1]['id'],
                    "type" : "map",
                    "variable" : v
                })                

    def run(self):
        start_time = time.time()
        logging.info('Profile testing start time: ' + str(start_time))

        self.profile_test()
        self.virtual_mooring_test()
        self.transect_test()
        self.hovmoller_test()
        self.area_test()

        end_time = time.time()
        logging.info('Profile testing end time: ' + str(end_time))
        total_time = end_time - start_time
        logging.info('Time to complete all tests: ' + str(total_time))

if __name__ == '__main__':
    
    #api_profiler = ONav_API_Proiler('http://lxc-on-03.ent.dfo-mpo.ca:5000/api/v1.0/', 'api_testing_config.json')
    api_profiler = ONav_API_Proiler('https://navigator.oceansdata.ca/api/v1.0/', 'api_testing_config.json')
    #api_profiler = ONav_API_Proiler('http://durmstrang.ent.dfo-mpo.ca:5000/api/v1.0/', 'api_testing_config.json', enable_logging=False)
    api_profiler.run()
    