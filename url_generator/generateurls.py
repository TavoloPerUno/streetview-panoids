import os
from shapely.geometry import Point
import random
from numpy import asarray
import pandas as pd
import googlemaps
import argparse
import multiprocessing as mproc
import numpy as np
from urllib.request import urlopen
from urllib.parse import urlparse, parse_qs, parse_qsl, urlunparse, urlencode
import csv
import sys
import math


DATA_FOLDER = os.path.join('..', 'data')
SV_META_API = 'https://maps.googleapis.com/maps/api/streetview/metadata?&location='
SV_EXAMPLE_URL = 'https://maps.googleapis.com/maps/api/streetview?size=640x640&pano=yth7Y8zQnKH1R_Z4oFVJHw&heading=180&pov=90&pitch=0&key=0'

with open(os.path.join(DATA_FOLDER, 'keys.csv'), 'r') as f:
    keys = csv.reader(f)
    keys = list(keys)[0]


dct_prov_key = {'buenosaires' : 0,
                 'entrerios': 1,
                 'santafe': 2,
                 'sanluis': 3,
                 'formosa': 4,
                 'cordoba': 5,
                 'catamarca': 6}

parser = argparse.ArgumentParser(description='Generate URLs')
# Required positional argument
parser.add_argument('input_file', type=str,
                    help='Input file')
parser.add_argument('-p', type=int,
                    help='Num cores')
parser.add_argument('-m', type=str,
                    help='Mode')

def generate_random(number, polygon):
    list_of_points = []
    minx, miny, maxx, maxy = polygon.bounds
    counter = 0
    while counter < number:
        pnt = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
        if polygon.contains(pnt):
            list_of_points.append(asarray(pnt))
            counter += 1
    return list_of_points

def generate_points_for_shapes(gdf_city_boundaries, df_route_pts):

    df_points = pd.DataFrame(columns=('PLACE', 'LNG', 'LAT'))

    for idx, city in gdf_city_boundaries.iterrows():
        result = generate_random(400000, city['geometry'])
        df_points = df_points.append(pd.DataFrame(
            [{'PLACE': city['NAME_2'], 'LNG': pt[0], 'LAT': pt[1]} for pt in result if
             not ((df_route_pts['LAT'] == pt[1]) & (df_route_pts['LNG'] == pt[0])).any()]),
                                     ignore_index=True)

    return df_points

def snap_to_roads(df_points, df_route_pts):
    df_snapped = pd.DataFrame(columns=('PLACE', 'LNG', 'LAT'))
    keyid = 0
    gmaps = googlemaps.Client(key=keys[keyid])
    for idx, pt in df_points.iterrows():

        if idx != 0 and ((idx % 2500) == 0):
            keyid += 1
            if keyid > (len(keys) - 1):
                print("API limit exhausted")
                break
            gmaps = googlemaps.Client(key=keys[keyid])
            print("Using key " + str(keyid))

        tpts = str(df_points.iloc[idx]["LAT"]) + ',' + str(df_points.iloc[idx]["LNG"])
        rgPts = gmaps.snap_to_roads(tpts, interpolate=False)
        if len(rgPts) > 0 and not (((df_snapped['LAT'] == rgPts[0]['location']['latitude']) & (
            df_snapped['LNG'] == rgPts[0]['location']['longitude'])).any()) and not (((df_route_pts['LAT'] ==
                                                                                           rgPts[0]['location'][
                                                                                               'latitude']) & (
            df_route_pts['LNG'] == rgPts[0]['location']['longitude'])).any()):
            df_snapped = df_snapped.append(pd.DataFrame([{'PLACE': df_points.iloc[idx]["PLACE"],
                                                          'LNG': rgPts[0]['location']['longitude'],
                                                          'LAT': rgPts[0]['location']['latitude']}]),
                                           ignore_index=True)

    return (df_snapped)

def generate_urls(df_points, filename, key_idx):
    df_image = pd.DataFrame(pd.DataFrame(
        columns=('FULL_IMG_ID', 'PT_ID', 'PT_LNG', 'PT_LAT', 'LNG', 'LAT', 'PANOID', 'HEADING', 'MONTH', 'YEAR', 'URL')))
    key_id = 0
    gmaps = googlemaps.Client(key=keys[key_idx[key_id]])
    for idx, pt in df_points.iterrows():

        print(str(idx))
        panoid = pt['PANOID']
        year = pt['YEAR']
        month = pt['MONTH']
        heading = 0

        while True:
            count = df_image.shape[0]
            if count != 0 and count % 24700 == 0:
                key_id = (key_id + 1) % 10
                print("Using key: " + str(key_idx[key_id]))
            dct_param = dict((f, v) for (f, v) in parse_qsl(urlparse(SV_EXAMPLE_URL).query))
            dct_param.update({'heading': heading, 'key': keys[key_idx[key_id]], 'pano': panoid})
            url = urlunparse((
                urlparse(SV_EXAMPLE_URL).scheme,
                urlparse(SV_EXAMPLE_URL).netloc,
                urlparse(SV_EXAMPLE_URL).path,
                urlparse(SV_EXAMPLE_URL).params,
                urlencode([(f, v) for f, v in list(dct_param.items())]),
                urlparse(SV_EXAMPLE_URL).fragment
            )
            )
            df_image = df_image.append(pd.DataFrame([{'FULL_IMG_ID': pt['ID'],
                                                      'PT_ID': pt['PT_ID'],
                                                      'LNG': pt['LNG'],
                                                      'LAT': pt['LAT'],
                                                      'PT_LNG': pt['P_LNG'],
                                                      'PT_LAT': pt['P_LAT'],
                                                      'MONTH': month,
                                                      'YEAR': year,
                                                      'PANOID': panoid,
                                                      'HEADING' : heading,
                                                      'URL': url}]),
                                       ignore_index=True)
            heading += 90
            if heading > 270:
                break



    df_image.to_csv(filename, header=True, index=False)

def filter_by_heading(df, filename, heading):
    df_image_parsed = pd.DataFrame(columns=('FULL_IMG_ID', 'PT_ID', 'PT_LNG', 'PT_LAT', 'LNG', 'LAT', 'PANOID', 'MONTH', 'YEAR', 'URL', 'ID'))
    for index, row in df.iterrows():
        parsed = urlparse(str(row['URL'])).query
        parsed = parse_qs(parsed)
        if int(parsed['heading'][0]) == heading:
            df_image_parsed = df_image_parsed.append(row)
    df_image_parsed['HEADING'] = heading
    df_image_parsed.to_csv(filename, header=True, index=False)

def sample_images(inputfile, heading, ncores, nsample):
    df_images_full = pd.read_csv(os.path.join(DATA_FOLDER, 'images_full_' + os.path.basename(inputfile)), index_col=None,
                            header=0)
    del df_images_full['ID']
    df_images_full['ID'] = df_images_full.index
    df_images_full.to_csv(os.path.join(DATA_FOLDER, 'images_full_' + os.path.basename(inputfile)), index=False, header=True)
    prov_id = dct_prov_key[os.path.splitext(os.path.basename(inputfile))[0]]
    key_idx = list(range(prov_id * 10, prov_id * 10 + 10))

    if 'HEADING' not in df_images_full.columns:

        i = 0
        lst_subfile = []
        procs = []

        for res in np.array_split(df_images_full, ncores):
            subfile_name = os.path.join(DATA_FOLDER, 'part_' + str(i) + '_images_sample_' + os.path.basename(inputfile))
            lst_subfile.append(subfile_name)
            filter_by_heading(res, subfile_name, heading)

            proc = mproc.Process(target=filter_by_heading,
                                 args=(res,
                                       subfile_name,
                                       heading,
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
        df_images_full = pd.concat(lst_result)

        df_images_full.to_csv(os.path.join(DATA_FOLDER, 'images_full_' + os.path.basename(inputfile)), index=False, header=True)

        for file in lst_subfile:
            if os.path.isfile(file):
                os.remove(file)

    df_images_full = df_images_full.loc[df_images_full['HEADING'] == 90,]

    df_images_full = df_images_full.sample(n=nsample)

    for index, row in df_images_full.iterrows():
        key_id = key_idx[0]
        dct_param = dict((f, v) for (f, v) in parse_qsl(urlparse(row['URL']).query))
        dct_param.update({'heading': heading, 'key': keys[key_id]})
        url = urlunparse((
            urlparse(row['URL']).scheme,
            urlparse(row['URL']).netloc,
            urlparse(row['URL']).path,
            urlparse(row['URL']).params,
            urlencode([(f, v) for f, v in list(dct_param.items())]),
            urlparse(row['URL']).fragment
        )
        )
        df_images_full.set_value(index, 'URL', url)

    df_images_full.to_csv(os.path.join(DATA_FOLDER, 'images_sample_' + os.path.basename(inputfile)), index=False, header=True)




def generate_urls_with_mproc(inputfile, cores):
    df_points = pd.read_csv(os.path.join(DATA_FOLDER, 'panoids_final_' + os.path.basename(inputfile)), index_col=None,
                            header=0)
    key_idx = range(dct_prov_key[os.path.splitext(os.path.basename(inputfile))[0]] * 10, 10)
    df_points['PT_ID'] = df_points['ID']
    df_points['ID'] = df_points.index
    i = 0
    lst_subfile = []
    procs = []


    for res in np.array_split(df_points, cores):
        subfile_name = os.path.join(DATA_FOLDER, 'part_' + str(i) + '_images_' + os.path.basename(inputfile))
        lst_subfile.append(subfile_name)

        proc = mproc.Process(target=generate_urls,
                             args=(res,
                                   subfile_name,
                                   key_idx,
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
    result = pd.concat(lst_result)

    result['ID'] = result.index

    result.to_csv(os.path.join(DATA_FOLDER, 'images_full_' + os.path.basename(inputfile)), index=False, header=True)

    for file in lst_subfile:
        if os.path.isfile(file):
            os.remove(file)




def main(argv):
    inputfile = ''
    args = parser.parse_args()
    inputfile = os.path.join(DATA_FOLDER, args.input_file)
    cores = args.p
    mode = args.m
    if mode == 'URL':
        generate_urls_with_mproc(inputfile, cores)
    else:
        sample_images(inputfile, 90, cores, 20000)

if __name__ == "__main__":
    main(sys.argv[1:])
