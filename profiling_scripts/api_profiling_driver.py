import requests
import time
import logging
import numpy as np
import getopt
import csv

import sys, os, shutil

import json
from urllib.parse import urlencode


class ONav_Profiling_Driver():


    def __init__(self, base_url, config_url, file_id, max_attempts = 3, max_time = 120, enable_logging = True, save_csv = True):
        """
        Initializes the profiling driver. Input arguments are:

        base_url: the url of the Navigator intance being profiled
        config_url: the filepath/name of the configuration file
        file_id: a unique identifier for output file names
        max_attempts: the number of attempts allowed to connect to API endpoints (default 3)
        max_time: the maxium time to wait for a response from each endpoint in seconds (default 120)
        enable_logging: Enables logging of the profile driver (default True)
        save_csv: Enables csv output of client-side results (default True)

        """
        self.base_url = base_url + '/api/v1.0/'
        self.file_id = file_id
        self.logging = enable_logging
        self.save_csv = save_csv
        self.max_attempts = max_attempts
        self.max_time = max_time
        self.start_time = time.time() 
        self.log_filename = f'/dev/shm/{self.file_id}_api_profiling.log'
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
            self.test_list = list(self.test_config.keys())


    def send_req(self, url):
        """
        This method sends the requests to the given url and logs the results and time taken. It 
        also handles and logs raised exceptions.
        """
        logging.info(f'URL: {url}')
        for i in range(self.max_attempts):
            logging.info(f'Attempt {i+1}:')
            start_time = time.time()
            try:
                resp = requests.get(url, timeout=self.max_time)
                end_time = time.time()

                if resp.status_code == 200: 
                    total_time = end_time - start_time
                    logging.info(f'*** Response recieved. ***\n Total request time: {total_time} seconds.')
                    return resp, start_time, total_time
                elif resp.status_code == 500:
                    logging.info(f'*** Request failed. ***\n{resp.content}')
                elif resp.status_code == 504:
                    logging.info(f'*** Server timed-out after {end_time-start_time} seconds. ***')
                else:
                    logging.warning('*** Request failed. ***\nReason unknown.')  
            except requests.ReadTimeout:
                end_time = time.time()
                logging.warning(f'*** Client timed out after {end_time-start_time} seconds (max_time = {self.max_time} seconds). ***')
            except requests.exceptions.ConnectionError:
                logging.warning('*** Connection aborted. ***')
            
        logging.critical(f'Could not complete request after {self.max_attempts} attempt(s).')
        return [], start_time, np.nan


    def get_datasets(self):
        logging.info('Requesting dataset meta data...')
        data, _, _ = self.send_req(self.base_url + 'datasets/')
        return [d for d in json.loads(data.content)]


    def get_variables(self, dataset):
        logging.info('Requesting variables...')
        data, _, _ = self.send_req(self.base_url + f'variables/?dataset={dataset}')
        return [d for d in json.loads(data.content)]


    def get_timestamps(self, dataset, variable): 
        logging.info('Requesting timestamps...')
        data, _, _ = self.send_req(self.base_url + f"timestamps/?dataset={dataset}&variable={variable}")  
        return [d for d in json.loads(data.content)]       


    def get_depths(self, dataset, variable):
        logging.info('Requesting depths...')
        data, _, _ = self.send_req(self.base_url + f"depth/?dataset={dataset}&variable={variable}")
        return [d for d in json.loads(data.content)]


    def get_plot(self, query):
        logging.info('Requesting plot...')
        return self.send_req(self.base_url + 'plot/?' + urlencode({'query': json.dumps(query)}) + '&format=json')


    def profile_test(self):
        logging.info('\n****************** Profiling Profile Plot ******************\n')
        config = self.test_config['profile_plot']
        results = [['Dataset', 'Variable', 'Start Time', 'Response Time']]

        for ds in config['datasets']:
            logging.info(f"\nDataset: {ds}\n")
            for v in config['datasets'][ds]['variables']:
                logging.info(f"Variable: {v}")
                timestamps = self.get_timestamps(ds,v)
                
                _, start_time, resp_time = self.get_plot({
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

                results.append([ds, v, start_time, resp_time])

        return results 
    

    def virtual_mooring_test(self):
        logging.info('\n****************** Profiling Virtual Mooring Plot ******************\n')
        config = self.test_config['vm_plot']
        results = [['Dataset', 'Variable', 'Start Time', 'Response Time']]

        for ds in config['datasets']:
            logging.info(f"\nDataset: {ds}\n")
            for v in config['datasets'][ds]['variables']:
                logging.info(f"Variable: {v}")
                timestamps = self.get_timestamps(ds,v)
                start_idx = len(timestamps) - 10

                _, start_time, resp_time = self.get_plot({
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

                results.append([ds, v, start_time, resp_time])

        return results

    def transect_test(self):
        logging.info('\n****************** Profiling Transect Plot ******************\n')
        config = self.test_config['transect_plot']
        results = [['Dataset', 'Variable', 'Start Time', 'Response Time']]

        for ds in config['datasets']:
            logging.info(f"\nDataset: {ds}\n")
            for v in config['datasets'][ds]['variables']:
                logging.info(f"Variable: {v}")
                timestamps = self.get_timestamps(ds,v)

                _, start_time, resp_time = self.get_plot({
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

                results.append([ds, v, start_time, resp_time])

        return results                            


    def hovmoller_test(self):
        logging.info('\n****************** Profiling Hovmoller Plot ******************\n')
        config = self.test_config['hovmoller_plot']
        results = [['Dataset', 'Variable', 'Start Time', 'Response Time']]

        for ds in config['datasets']:
            logging.info(f"\nDataset: {ds}\n")
            for v in config['datasets'][ds]['variables']:
                logging.info(f"Variable: {v}")
                timestamps = self.get_timestamps(ds,v)
                start_idx = len(timestamps) - 10 

                _, start_time, resp_time = self.get_plot({
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

                results.append([ds, v, start_time, resp_time])

        return results

    def area_test(self):
        logging.info('\n****************** Profiling Area Plot ******************\n')
        config = self.test_config['area_plot']
        results = [['Dataset', 'Variable', 'Start Time', 'Response Time']]

        for ds in config['datasets']:
            logging.info(f"\nDataset: {ds}\n")
            for v in config['datasets'][ds]['variables']:
                logging.info(f"Variable: {v}")
                timestamps = self.get_timestamps(ds,v)

                _, start_time, resp_time = self.get_plot({
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

                results.append([ds, v, start_time, resp_time])

        return results


    def write_csv(self):
        with open(f'{self.file_id}_api_profiling_results.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter = ',')
            for key, value in self.results.items():
                writer.writerow([key])
                for v in value:
                    writer.writerow(v)
                writer.writerow([])


    def run(self):
        logging.info(f'Profile testing start time: {time.ctime(self.start_time)} ({self.start_time:.0f}).')

        if 'profile_plot' in self.test_list:
            self.results['profile'] = self.profile_test()
        if 'vm_plot' in self.test_list:
            self.results['virtual_mooring'] = self.virtual_mooring_test()
        if 'transect_plot' in self.test_list:
            self.results['transect'] = self.transect_test()
        if 'hovmoller_plot' in self.test_list:
            self.results['hovmoller'] = self.hovmoller_test()
        if 'area_plot' in self.test_list:
            self.results['area'] = self.area_test()

        end_time = time.time()
        logging.info(f'Profile testing start time:  {time.ctime(self.start_time)} ({self.start_time:.0f}).')
        logging.info(f'Profile testing end time:  {time.ctime(end_time)} ({end_time}).')
        logging.info(f'Time to complete all tests: {(end_time - self.start_time):.0f} seconds.')

        if self.logging:
            shutil.move(self.log_filename, os.getcwd())

        if self.save_csv:
            self.write_csv()          


if __name__ == '__main__':
    """
    The api_profiling_driver scripts is intendend to target Ocean Navigator API endpoints as specified in a
    configuration file so that profiles for these functions can be collected for performance analysis while 
    collecting client-side metrics for each. This script can log the status of each request and produce a csv 
    file contained the tabulated results (enabled by default). It is designed to be run from the command line
    with flags specifying file locaitons and options as described in the example below:

    python api_profiling_driver.py --url https://navigator.oceansdata.ca --config api_profiling_config.json --id usr_1 -a 3 -t 120 -l -c

    where:

    --url: the url of the Navigator instance that's being profiled
    --config: the path of configuration file
    --id: a unique identifer for output file names 
    -a: the number of attempts to reach each end point allowed 
    -t: the maxium time to wait for a response from each endpoint
    -l: use this flag to DISABLE logging
    -c: use this flag to DISABLE csv output of client-side results

    """
    # default options
    url = 'https://navigator.oceansdata.ca'
    config = '/home/ubuntu/ONavScripts/profiling_scripts/api_profiling_config.json'
    id=f'test_usr_{np.random.randint(1,100)}'
    max_attempts = 3
    max_time = 120
    enable_logging = True
    save_csv = True

    try:
        opts, args = getopt.getopt(sys.argv[1:], ':a:t:lc', ['url=', 'config=', 'id='])
    except getopt.GetoptError as err:
        print(err) 
        sys.exit()

    for o, a in opts:
        if o == '--url':
            url = a
        elif o == '--config':
            config = a    
        elif o == '--id':
            id = a
        elif o == '-a':
            max_attempts = int(a)
        elif o == '-t':
            max_time = int(a)
        elif o == '-l':
            enable_logging = False
        elif o == '-c':
            save_csv = False

    api_profiler = ONav_Profiling_Driver(
                    url,
                    config,
                    id,
                    max_attempts,
                    max_time,
                    enable_logging,
                    save_csv
                )

    api_profiler.run()    