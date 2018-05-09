import sys
import os
import streetview_edited as streetview
import pandas as pd
import json
import requests
import multiprocessing as mproc
import time
import urllib
import csv
import signal
from urllib2 import urlopen
import getopt
import numpy as np
import argparse
import shutil
import dask.dataframe as dd
import dask.array as da
import boto3
import s3fs
fs = s3fs.S3FileSystem(anon=False)

parser = argparse.ArgumentParser(description='Generate historic panoids')



# Required positional argument
parser.add_argument('input_folder', type=str,
                    help='Input folder')

parser.add_argument('-b', type=str,
                    help='Bucket name')

parser.add_argument('-m', type=str,
                    help='Mode')

# Optional positional argument
parser.add_argument('-k', type=str,
                    help='API key')

parser.add_argument('-p', type=int,
                    help='Num cores')

# Optional argument
parser.add_argument('-t', type=int,
                    help='timeout seconds')



def panoids_with_timeout(lst_result, lat, lon):
    lst_result.extend(streetview.panoids(lat, lon))

DATA_FOLDER = '../data'
apicallbase = 'https://maps.googleapis.com/maps/api/streetview/metadata?&pano='
apikey = ''

key_idx = 0
with open(os.path.join(DATA_FOLDER, 'keys.csv'), 'r') as f:
    keys = csv.reader(f)
    keys = list(keys)[0]


class TimeOutException(Exception):
    def __init__(self, message, errors):
        super(TimeOutException, self).__init__(message)
        self.errors = errors

def signal_handler(signum, frame):
    raise TimeOutException("Timeout!", frame)

signal.signal(signal.SIGALRM, signal_handler)


def get_historic_panoids(res, timeout_s, filename):

    results = []
    for index, row in res.iterrows():
        lst_result = []
        
	for i in range(10):
            signal.alarm(timeout_s) #Edit this to change the timeout seconds
            try:	
                panoids_with_timeout(lst_result, row['coords.x2'], row['coords.x1'])
            except TimeOutException as exc:
                print('Skipping '+ str(int(row['Unnamed: 0'])))
                break
	    except Exception as exc:
	        print("API Error. Retrying.")
                continue
            
            signal.alarm(0)
            results.extend([{'ID': int(row['Unnamed: 0']),
                             'P_LAT': row['coords.x2'],
                             'P_LNG': row['coords.x1'],
                             'PANOID': record['panoid'],
                             'LAT': record['lat'],
                             'LNG': record['lon'],
                             'YEAR': record['year'] if 'year' in record else '',
                             'MONTH': record['month'] if 'month' in record else ''} for record in lst_result])
            break

    if len(results) > 0:
        results = pd.DataFrame(results)
	if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        results.to_csv( filename, index=False, header=True)




def get_month_and_year_from_api(res, keys, key_idx):
    nrows = res.shape[0]
    print("\tStarted processing chunk with " + str(nrows) + " rows")
    i = 0	
    for index, row in res.iterrows():
        if row['YEAR'] == '' or row['MONTH'] == '' or pd.isnull(row['YEAR']) or pd.isnull(row['MONTH']):
            attempt = 0
            while True:
                requesturl = apicallbase + row['PANOID'] + '&key=' + keys[key_idx]
                try:
                    metadata = json.load(urlopen(requesturl))
                    if 'date' in metadata:
                        res.loc[index, 'YEAR'] = (metadata['date'])[:4]
                        res.loc[index, 'MONTH'] = (metadata['date'])[-2:]
                        attempt = 0
                        i = i+1
                        if i % 1000 == 0:
                            print("\t\tProcessed " + str(i) + " of " + str(nrows) + " rows")
                        break
                    else:
                        attempt += 1
                        if attempt > 9:
                            print ("Skipping " + requesturl)
                            attempt = 0
                            break
                        if 'error_message' in metadata:
                            key_idx += 1
                            key_idx = key_idx % len(keys)
     
                except Exception:
                    print("Timed out : " + apicallbase + row['PANOID'] + '&key=' + keys[key_idx])
                    key_idx += 1
                    key_idx = key_idx % len(keys)
                    continue
    return res
    #res.to_csv(file_name, index= False, header=True)

def write_historic_panoids(input_folder, bucket_name, keys, key_idx, timeout_s, cores):
    s3 = boto3.resource('s3')
    key_name_header = 's3://' + bucket_name + '/' + input_folder
    bucket = s3.Bucket(bucket_name)
    for file in bucket.objects.filter(Prefix=input_folder + "/"):
        inputfile = os.path.basename(file.key)

        if inputfile.endswith("_random_points.csv"): 
            lst_already_downloaded = list(bucket.objects.filter(Prefix=input_folder + '_panoids/' + 'panoids_' + inputfile))   
            panoid_complete = 1
            if not (len(lst_already_downloaded) > 0 and lst_already_downloaded[0].key == input_folder + '_panoids/' + 'panoids_' + inputfile):
	        panoid_complete = 0
                print("Processing " + inputfile)
                results = pd.read_csv(key_name_header + '/' + inputfile, index_col=None, header=0)
                i = 0
                lst_subfile = []
                procs = []
                for res in np.array_split(results, cores):
                    lst_subfile.append(os.path.join(DATA_FOLDER, os.path.basename(input_folder), 'part_' + str(i) + '_' + os.path.basename(inputfile)))

                    proc = mproc.Process(target=get_historic_panoids,
                                    args=(res,
                                        timeout_s,
                                        os.path.join(DATA_FOLDER, os.path.basename(input_folder),
                                                'part_' + str(i) + '_' + os.path.basename(inputfile)),
                                        )
                                     )
                    procs.append(proc)
                    proc.start()
                    i += 1
                for proc in procs:
                    proc.join()

                lst_result = []
                for file in lst_subfile:
                    if os.path.isfile(file):
                        try:
                            result = pd.read_csv(file, index_col=None, header=0)
                            lst_result.append(result)
                        except pd.errors.EmptyDataError:
                            continue
                
                try:
                    result = pd.concat(lst_result)
                    result.drop_duplicates(subset=['PANOID'], inplace=True)

		    with fs.open(key_name_header +  '_panoids/' + 'panoids_' + inputfile,'wb') as f:
                        result.to_csv(f)
                
                    #if not os.path.exists(os.path.join(DATA_FOLDER, os.path.basename(input_folder)+'panoids')):
                    #    os.makedirs(os.path.join(DATA_FOLDER, os.path.basename(input_folder)+'panoids'))
                    #result.to_csv(os.path.join(DATA_FOLDER, os.path.basename(input_folder) + 'panoids', 'panoids_' + os.path.basename(inputfile)), index=False, header=True)
                    panoid_complete = 1
                
                    for file in lst_subfile:
                        if os.path.isfile(file):
                            os.remove(file)
             	    if os.path.exists(os.path.join(DATA_FOLDER, os.path.basename(input_folder))):
                        shutil.rmtree(os.path.join(DATA_FOLDER, os.path.basename(input_folder)), ignore_errors=True)
                except Exception as ex:
                    print(lst_result)
                    print("Invalid file")

            if panoid_complete:
                fill_year_month(inputfile, bucket_name,input_folder + '_panoids', keys, key_idx, cores)

def fill_year_month(inputfile, bucket_name, input_folder, keys, key_idx, cores):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    key_name_header = 's3://' + bucket_name + '/' + input_folder + '/' 
    lst_already_downloaded = list(bucket.objects.filter(Prefix=input_folder + '/final_panoids_' + inputfile))
    if not (len(lst_already_downloaded) > 0 and lst_already_downloaded[0].key == input_folder + '/final_panoids_' + inputfile):
        pts = dd.read_csv(key_name_header + 'panoids_' + os.path.basename(inputfile), blocksize=2000000, header=0)
   
        print("Filling missing month and year values with " + str(pts.npartitions) + "data chunks")
        pts_empty = pts[da.isnan(pts.YEAR) |
                da.isnan(pts.MONTH)]
        pts_full = pts[~(da.isnan(pts.YEAR) |
                da.isnan(pts.MONTH))]
    
        pts_filled = pts_empty.map_partitions(get_month_and_year_from_api, keys, 0)
        pts = dd.concat([pts_full,pts_filled],axis=0,interleave_partitions=True).compute()
        with fs.open(key_name_header +  'final_panoids_' + inputfile,'wb') as f:
            pts.to_csv(f)
        #pts.to_csv(os.path.join(os.path.join(DATA_FOLDER, os.path.basename(input_folder), 'final_panoids_' + inputfile)), index=False, header=True)
        pts = pts.loc[~(pd.isnull(pts.YEAR) |
                pd.isnull(pts.MONTH))]

        with fs.open(key_name_header +  'final_panoids_' + inputfile,'wb') as f:
            pts.to_csv(f)
   
        #pts.to_csv(os.path.join(os.path.join(DATA_FOLDER, os.path.basename(input_folder), 'final_panoids_' + inputfile)), index=False, header=True)  

def main(argv):
    apikey = ''
    inputfile = ''
    timeout_s = 10

    args = parser.parse_args()
    inputfolder = args.input_folder
    apikey = args.k
    bucket_name = args.b
    if args.t is not None:
        timeout_s = args.t
    mode = args.m
    cores = args.p
    if mode == 'full':
        print("On Panoid fetch mode")
	write_historic_panoids(inputfolder, bucket_name, keys, key_idx, timeout_s, cores)
    else:
        print("On month/year fill mode")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        for file in bucket.objects.filter(Prefix=input_folder + "/"):
            input_file = file.key
            if input_file.endswith("_random_points.csv"):
                fill_year_month(input_file, bucket_name, inputfolder, keys, key_idx, cores)

if __name__ == "__main__":
    main(sys.argv[1:])
