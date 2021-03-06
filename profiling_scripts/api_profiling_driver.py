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


    def __init__(self, base_url, config_url, csv_file, prof_path, user_id, max_attempts = 3, max_time = 120):
        """
        Initializes the profiling driver. Input arguments are:

        base_url: the url of the Navigator intance being profiled
        config_url: the filepath/name of the configuration file
        user_id: a unique identifier for output file names
        prof_path: the path to the directory containing server profiling results
        max_attempts: the number of attempts allowed to connect to API endpoints (default 3)
        max_time: the maxium time to wait for a response from each endpoint in seconds (default 120)
        enable_logging: Enables logging of the profile driver (default True)
        save_csv: Enables csv output of client-side results (default True)

        """
        if base_url[-1] == '/':
            base_url = base_url[:-1]
        self.base_url = base_url + '/api/v1.0/'
        self.csv_file = csv_file
        self.user_id = user_id
        self.prof_path = prof_path
        self.max_attempts = max_attempts
        self.max_time = max_time
        self.start_time = time.time() 
        self.log_filename = f'/dev/shm/{self.user_id}_api_profiling.log'
        self.results = []

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
                    if total_time < 1:
                        time.sleep(1)
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


    def format_time(self, in_time):
        return time.strftime("%Y.%m.%d-%H:%M:%S", time.gmtime(in_time))


    def profile_test(self):
        logging.info('\n****************** Profiling Profile Plot ******************\n')
        config = self.test_config['profile_plot']

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

                self.results.append(['profile', ds, v, start_time, resp_time])
    

    def virtual_mooring_test(self):
        logging.info('\n****************** Profiling Virtual Mooring Plot ******************\n')
        config = self.test_config['vm_plot']

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

                self.results.append(['virtual mooring', ds, v, start_time, resp_time])


    def transect_test(self):
        logging.info('\n****************** Profiling Transect Plot ******************\n')
        config = self.test_config['transect_plot']

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

                self.results.append(['transect', ds, v, start_time, resp_time])                         


    def hovmoller_test(self):
        logging.info('\n****************** Profiling Hovmoller Plot ******************\n')
        config = self.test_config['hovmoller_plot']

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

                self.results.append(['hovmoller', ds, v, start_time, resp_time])


    def area_test(self):
        logging.info('\n****************** Profiling Area Plot ******************\n')
        config = self.test_config['area_plot']

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

                self.results.append(['area', ds, v, start_time, resp_time])


    def get_profile_paths(self):
        prof_files = os.listdir(self.prof_path)
        plot_profs = [p for p in prof_files if 'plot' in p]
        plot_times = np.array([p.split('.')[-2] for p in plot_profs]).astype(np.int)
        for row in self.results:
            if row[0] != 'Dataset' and not np.isnan(row[3]):
                if self.prof_path:
                    time = row[-2]
                    diff = plot_times - np.floor(time)
                    diff_times = plot_times[np.where(diff > 0)]
                    min_diff = np.min(diff_times)
                    prof_name = [i for i in plot_profs if str(min_diff) in i][0]
                    prof_renamed = f'{self.prof_path}/{self.format_time(time)}_{self.user_id}_{row[0]}_{row[1]}_{row[2]}.prof'
                    os.rename(f'{self.prof_path}/{prof_name}', prof_renamed)
                    row.append(prof_renamed)
                else:
                    row.append('')


    def write_csv(self):
        if self.csv_file:
            csv_name = self.csv_file
        else: 
            csv_name = f'{self.user_id}_api_profiling_results.csv'

        with open(csv_name, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter = ',')

            if os.stat(csv_name).st_size == 0:
                writer.writerow(['Test', 'Dataset', 'Variable', 'Start Time', 'Response Time (s)', 'Profile File Path'])
            for row in self.results:
                writer.writerow([*row[:3],self.format_time(row[3]),f'{row[4]:.4f}',row[5]])


    def run(self):
        logging.info(f'Profile testing start time: {time.ctime(self.start_time)} ({self.start_time:.0f}).')

        if 'profile_plot' in self.test_list:
            self.profile_test()
        if 'vm_plot' in self.test_list:
            self.virtual_mooring_test()
        if 'transect_plot' in self.test_list:
            self.transect_test()
        if 'hovmoller_plot' in self.test_list:
            self.hovmoller_test()
        if 'area_plot' in self.test_list:
            self.area_test()

        end_time = time.time()
        logging.info(f'Profile testing start time:  {time.ctime(self.start_time)} ({self.start_time:.0f}).')
        logging.info(f'Profile testing end time:  {time.ctime(end_time)} ({end_time}).')
        logging.info(f'Time to complete all tests: {(end_time - self.start_time):.0f} seconds.')

        shutil.move(self.log_filename, os.getcwd())

        if self.prof_path:
            self.get_profile_paths()

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
    --prof: the path to the directory containing server profiling results
    --id: a unique identifer for output file names 
    -a: the number of attempts to reach each end point allowed 
    -t: the maxium time to wait for a response from each endpoint
    """

    # default options
    url = 'https://navigator.oceansdata.ca'
    config = '/home/ubuntu/ONavScripts/profiling_scripts/api_profiling_config.json'
    csv_file = None
    prof_path = None
    id = f'test_usr_{np.random.randint(1,100)}'
    max_attempts = 3
    max_time = 120

    try:
        opts, args = getopt.getopt(sys.argv[1:], ':a:t:', ['url=', 'config=', 'csv=', 'prof=', 'id='])
    except getopt.GetoptError as err:
        print(err) 
        sys.exit()

    for o, a in opts:
        if o == '--url':
            url = a
        elif o == '--config':
            config = a    
        elif o == '--prof':
            prof_path = a   
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
                        csv_file,
                        prof_path,
                        id,
                        max_attempts,
                        max_time
                    )
    api_profiler.run()    


"""
TODO:
Get git hash. One possible way: 

pip install gitpython

import git
repo = git.Repo(search_parent_directories=True)
sha = repo.head.object.hexsha 

Or:

import subprocess

def get_git_revision_hash() -> str:
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()

def get_git_revision_short_hash() -> str:
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()
"""