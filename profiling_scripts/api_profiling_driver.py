import requests
import time
import logging
import numpy as np
import pandas as pd

import os, shutil

import json
from urllib.parse import urlencode


class ONav_Profiling_Driver():


    def __init__(self, base_url, config_url, max_attempts = 3, max_time = 120, enable_logging = True, save_csv = True):
        self.base_url = base_url     
        self.logging = enable_logging
        self.save_csv = save_csv
        self.max_attempts = max_attempts
        self.max_time = max_time
        self.start_time = f'{time.time():.0f}'
        self.log_filename = f'/dev/shm/api_profile_testing_{self.start_time}.log'
        self.results = {}

        if enable_logging:
            logging.basicConfig(
                filename = self.log_filename, 
                level = logging.DEBUG, 
                format = '%(created)f %(asctime)s %(levelname)s \n %(message)s', 
                datefmt = '%H:%M:%S'
            )
            logging.info('\n****************** Starting Profile Driver ******************\n')

        with open(config_url) as f:
            self.test_config = json.load(f)   


    def send_req(self, url):
        logging.info('URL: ' + url)
        for i in range(self.max_attempts):
            logging.info('Attempt ' + str(i+1) + ':')
            start_time = time.time()
            try:
                resp = requests.get(url, timeout=self.max_time)
                if resp.status_code == 200:
                    end_time = time.time()
                    total_time = end_time - start_time
                    logging.info('*** Response recieved. ***\n Total request time: ' + str(total_time))
                    return resp, total_time
                else:
                    logging.warning('*** Request failed. ***')  
            except requests.ReadTimeout:
                logging.warning('*** Request timed out. ***')
            except requests.exceptions.ConnectionError:
                logging.warning('*** Connection aborted. ***')
            
        logging.critical('Could not complete request after ' + str(self.max_attempts) + ' attempt(s).')
        return [], np.nan


    def get_datasets(self):
        logging.info('Requesting dataset meta data...')
        data, _ = self.send_req(self.base_url + 'datasets/')
        return [d for d in json.loads(data.content)]


    def get_variables(self, dataset):
        logging.info('Requesting variables...')
        data, _ = self.send_req(self.base_url + f'variables/?dataset={dataset}')
        return [d for d in json.loads(data.content)]


    def get_timestamps(self, dataset, variable): 
        logging.info('Requesting timestamps...')
        data, _ = self.send_req(self.base_url + f"timestamps/?dataset={dataset}&variable={variable}")  
        return [d for d in json.loads(data.content)]       


    def get_depths(self, dataset, variable):
        logging.info('Requesting depths...')
        data, _ = self.send_req(self.base_url +f"depth/?dataset={dataset}&variable={variable}")
        return [d for d in json.loads(data.content)]


    def get_plot(self,query):
        logging.info('Requesting plot...')
        return self.send_req(self.base_url + 'plot/?' + urlencode({'query': json.dumps(query)}) + '&format=json')


    def profile_test(self):
        logging.info('\n****************** Profiling Profile Plot ******************\n')
        config = self.test_config['profile_plot']
        results_dict = {}

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            ds_dict = {}
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                
                _, resp_time = self.get_plot({
                                    "dataset" : ds,
                                    "names" : [],
                                    "plotTitle" : "",
                                    "quantum" : config['datasets'][ds]['quantum'],
                                    "showmap" : 0,
                                    "station" : config['station'],
                                    "time" : timestamps[-1]['id'], 
                                    "type" : "profile",
                                    "variable" : v
                                })

                ds_dict[v] = resp_time

            results_dict[ds] = ds_dict

        return results_dict
    

    def virtual_mooring_test(self):
        logging.info('\n****************** Profiling Virtual Mooring Plot ******************\n')
        config = self.test_config['vm_plot']
        results_dict = {}

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            ds_dict = {}
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                start_idx = len(timestamps) - 10

                _, resp_time = self.get_plot({
                                    "colormap" : "default",
                                    "dataset" : ds,
                                    "depth" : 0,
                                    "endtime" : timestamps[-1]['id'],
                                    "names" : [],
                                    "plotTitle" : "",
                                    "quantum" : config['datasets'][ds]['quantum'],
                                    "scale" : "-5,30,auto",
                                    "showmap" : 0,
                                    "starttime" : timestamps[start_idx]['id'], 
                                    "station" : config['station'], 
                                    "type" : "timeseries",
                                    "variable" : v
                                })

                ds_dict[v] = resp_time

            results_dict[ds] = ds_dict

        return results_dict

    def transect_test(self):
        logging.info('\n****************** Profiling Transect Plot ******************\n')
        config = self.test_config['transect_plot']
        results_dict = {}

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            ds_dict = {}
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)

                _, resp_time = self.get_plot({
                                    "colormap" : "default",
                                    "dataset" : ds,
                                    "depth_limit" : 0,
                                    "linearthresh" : 200,
                                    "name" : config['name'],
                                    "path" : config['path'], 
                                    "plotTitle" : "",
                                    "quantum" : config['datasets'][ds]['quantum'],
                                    "scale" : "-5,30,auto",
                                    "selectedPlots" : "0,1,1",
                                    "showmap" : 1,
                                    "surfacevariable" : "none",
                                    "time" : timestamps[-1]['id'], 
                                    "type" : "transect",
                                    "variable" : v
                                })

                ds_dict[v] = resp_time

            results_dict[ds] = ds_dict

        return results_dict                                


    def hovmoller_test(self):
        logging.info('\n****************** Profiling Hovmoller Plot ******************\n')
        config = self.test_config['hovmoller_plot']
        results_dict = {}

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            ds_dict = {}
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)
                start_idx = len(timestamps) - 10 

                _, resp_time = self.get_plot({
                                    "colormap" : "default",
                                    "dataset" : ds,
                                    "depth" : 0,
                                    "endtime" : timestamps[-1]['id'],
                                    "name" : config['name'],
                                    "path" : config['path'],
                                    "plotTitle" : "",
                                    "quantum" : config['datasets'][ds]['quantum'],
                                    "scale" : "-5,30,auto",
                                    "showmap" : 1,
                                    "starttime" : timestamps[start_idx]['id'],
                                    "type" : "hovmoller",
                                    "variable" : v
                                })

                ds_dict[v] = resp_time

            results_dict[ds] = ds_dict

        return results_dict

    def area_test(self):
        logging.info('\n****************** Profiling Area Plot ******************\n')
        config = self.test_config['area_plot']
        results_dict = {}

        for ds in config['datasets']:
            logging.info("\nDataset: " + ds + "\n")
            ds_dict = {}
            for v in config['datasets'][ds]['variables']:
                logging.info("Variable: " + v)
                timestamps = self.get_timestamps(ds,v)

                _, resp_time = self.get_plot({
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
                                    "quantum" : config['datasets'][ds]['quantum'],
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

                ds_dict[v] = resp_time

            results_dict[ds] = ds_dict

        return results_dict

    def run(self):
        start_time = time.time()
        logging.info('Profile testing start time: ' + time.ctime(start_time) + ' (' + str(start_time) + ')')

        self.results['profile'] = self.profile_test()
        self.results['virtual_mooring'] = self.virtual_mooring_test()
        self.results['transect'] = self.transect_test()
        self.results['hovmoller'] = self.hovmoller_test()
        self.results['area'] = self.area_test()

        end_time = time.time()
        logging.info('Profile testing start time: ' + time.ctime(end_time) + ' (' + str(end_time) + ')')
        logging.info('Time to complete all tests: ' + str(end_time - start_time))

        if self.logging:
            shutil.move(self.log_filename, os.getcwd())

        if self.save_csv:
            with open(f'api_profiling_results_{self.start_time}.csv', 'a') as csv_stream:
                for key, value in self.results.items():
                    csv_stream.write(key + '\n')
                    df = pd.DataFrame.from_dict(value,  orient='index')
                    df.to_csv(csv_stream)
                    csv_stream.write('\n')               
        

if __name__ == '__main__':
    
    api_profiler = ONav_Profiling_Driver('http://lxc-on-02.ent.dfo-mpo.ca:5000/api/v1.0/', '/home/ubuntu/ONavScripts/profiling_scripts/api_profiling_config.json', max_time = 200)
    api_profiler.run()