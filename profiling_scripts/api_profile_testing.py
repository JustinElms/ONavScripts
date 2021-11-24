import requests
import time
import logging

import json
from contextlib import closing
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


class ONav_API_Profiler():


    def __init__(self, base_url, config_url, enable_logging = True, max_attempts = 3, max_time = 120):
        self.base_url = base_url     
        self.logging = logging
        self.max_attempts = max_attempts
        self.max_time = max_time

        if enable_logging:
            logging.basicConfig(
                filename='api_profile_testing.log', 
                level=logging.DEBUG, 
                format='%(asctime)s %(levelname)s \n %(message)s', 
                datefmt='%H:%M:%S'
            )
            logging.info('\n****************** Starting Profiler Tests ******************\n')

        with open(config_url) as f:
            self.test_config = json.load(f)   


    def send_req(self, url):
        for i in range(self.max_attempts):
            logging.info('Attempt ' + str(i+1) + ':')
            start_time = time.time()
            try:
                resp = requests.get(url, timeout=self.max_time)
                if resp.status_code == 200:
                    end_time = time.time()
                    logging.info('*** Response recieved. ***\n Total request time: ' + str(end_time - start_time))
                    return resp
                else:
                    logging.warning('*** Request failed. ***')  
            except requests.ReadTimeout:
                logging.warning('*** Request timed out. ***')
            except requests.exceptions.ConnectionError:
                logging.warning('*** Connection aborted. ***')
            
        logging.critical('Could not complete request after ' + str(self.max_attempts) + ' attempt(s).')


    def get_datasets(self):
        logging.info('Requesting dataset meta data...')
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


    def get_plot(self,query):
        logging.info('Requesting plot...')
        self.send_req(self.base_url + 'plot/?' + urlencode({'query': json.dumps(query)}) + '&format=json')


    def profile_test(self):
        logging.info('\n****************** Profile Plot Tests ******************\n')
        config = self.test_config['profile_plot']

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                self.get_plot({
                    "dataset" : ds,
                    "names" : [],
                    "plotTitle" : "",
                    "quantum" :  config['datasets'][ds]['quantum'],
                    "showmap" : False,
                    "station" : config['station'],
                    "time" : timestamps[-1]['id'], 
                    "type" : "profile",
                    "variable" : v
                })
    

    def virtual_mooring_test(self):
        logging.info('\n****************** Virtual Mooring Plot Tests ******************\n')
        config = self.test_config['vm_plot']

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                start_idx = len(timestamps) - 10 # len(timestamps) - config['datasets'][ds]['time_range']

                self.get_plot({
                    "colormap" : "default",
                    "dataset" : ds,
                    "depth" : 0,
                    "endtime" : timestamps[-1]['id'],
                    "names" : [],
                    "plotTitle" : "",
                    "quantum" :  config['datasets'][ds]['quantum'],
                    "scale" : "-5,30,auto",
                    "showmap" : 0,
                    "starttime" : timestamps[start_idx]['id'], 
                    "station" : config['station'], 
                    "type" : "timeseries",
                    "variable" : v
                })


    def transect_test(self):
        logging.info('\n****************** Transect Plot Tests ******************\n')
        config = self.test_config['transect_plot']

        for ds in config['datasets']:
            logging.info("----------\nDataset: " + ds + "\n----------")
            
            for v in config['datasets'][ds]['variables']:
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
                    "quantum" :  config['datasets'][ds]['quantum'],
                    "scale" : "-5,30,auto",
                    "selectedPlots" : "0,1,1",
                    "showmap" : 1,
                    "surfacevariable" : "none",
                    "time" : timestamps[-1]['id'], 
                    "type" : "transect",
                    "variable" : v
                })


    def hovmoller_test(self):
        logging.info('\n****************** Hovmoller Plot Tests ******************\n')
        config = self.test_config['hovmoller_plot']

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                start_idx = len(timestamps) - 10 # len(timestamps) - config['datasets'][ds]['time_range']

                self.get_plot({
                    "colormap" : "default",
                    "dataset" : ds,
                    "depth" : 0,
                    "endtime" : timestamps[-1]['id'],
                    "name" : config['name'],
                    "path" : config['path'],
                    "plotTitle" : "",
                    "quantum" :  config['datasets'][ds]['quantum'],
                    "scale" : "-5,30,auto",
                    "showmap" : 1,
                    "starttime" : timestamps[start_idx]['id'],
                    "type" : "hovmoller",
                    "variable" : v
                })


    def area_test(self):
        logging.info('\n****************** Starting Area Plot Tests ******************\n')
        config = self.test_config['area_plot']

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            for v in config['datasets'][ds]['variables']:
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
                    "quantum" :  config['datasets'][ds]['quantum'],
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
        logging.info('Profile testing start time: ' + time.ctime(start_time) + ' (' + str(start_time) + ')')

        self.profile_test()
        self.virtual_mooring_test()
        self.transect_test()
        self.hovmoller_test()
        self.area_test()

        end_time = time.time()
        logging.info('Profile testing start time: ' + time.ctime(end_time) + ' (' + str(end_time) + ')')
        logging.info('Time to complete all tests: ' + str(end_time - start_time))

if __name__ == '__main__':
    
    #api_profiler = ONav_API_Profiler('http://lxc-on-02.ent.dfo-mpo.ca:5000/api/v1.0/', '/home/ubuntu/ONavScripts/profiling_scripts/api_testing_config.json', max_time = 200)
    api_profiler = ONav_API_Profiler('http://durmstrang.ent.dfo-mpo.ca:5000/api/v1.0/', '/home/ubuntu/ONavScripts/profiling_scripts/api_testing_config.json', max_time = 200)
    #api_profiler = ONav_API_Profiler('http://navigator.oceansdata.ca/api/v1.0/', '/home/ubuntu/ONavScripts/profiling_scripts/api_testing_config.json')
    api_profiler.run()
    