import time
import concurrent.futures
import json
from glob import glob
import logging
import os
import sys
import requests as req
import pickle
sys.setrecursionlimit(20000)

logging.basicConfig(filename='debug.log', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
URL = '''https://buy.housing.com/api/v1/buy/similar-properties?source=web&limit=50&flat_id={:s}'''

completed_dump = os.sep.join(["data", "completed_flat_list.dump"])
pending_dump = os.sep.join(["data", "pending_flat_list.dump"])
saved_to = 'data'

def get_flatID_from_path(path):
    return path.split(os.sep)[-1].split('.')[0]


def sieve_links(data):
    new_flat_list = [ _['inventory_configs'][0]['id'] for _ in data
                    if str(_['inventory_configs'][0]['id']) not in COMPLETED_FLATIDs]
    # logging.info("Found {:d} flat ids".format(len(new_flat_list)))
    return new_flat_list


def parsed_flat_listings():
    return glob(os.sep.join(['data', '*json']))


def generate_flatids(flatlist):
    flat_list = map(int, flatlist)
    lo, hi = min(flat_list), max(flat_list)
    return range(lo, hi)


def split_data(data):
    for _ in data:
        id = str(_['id'])
        target = os.sep.join([saved_to, id + '.json'])
        json.dump(_, open(target, 'w'))

def scrape(flat_id):
    int_flat_id = flat_id
    flat_id = str(flat_id)
    url = URL.format(flat_id)
    filepath = os.sep.join(['data', flat_id+'.json'])
    data = []
    if filepath not in parsed_flat_listings() and flat_id not in COMPLETED_FLATIDs:
        logging.info("NOT CACHED: {:s}".format(flat_id))
        response = req.get(url).text
        try:
            print len(response)
            if len(response)>1:
                print "length of resp : ", len(response)
                data = json.loads(response)
                #split_data(data)
                json.dump(json.loads(response), open(filepath, 'w'))
                print len(data), filepath
                sys.exit()
                new_flats = sieve_links(data)
                new_flats = generate_flatids(new_flats)    
                map(scrape, new_flats)
        except ValueError, e:
            logging.error("ERR: {:s} in {:s}".format(e, url))
        finally:
            COMPLETED_FLATIDs.append(flat_id)
    else:
        #logging.info("CACHED: {:s}. TODO: {:d}".format(flat_id, len(PENDING_FLATIDs)))
        if flat_id in PENDING_FLATIDs:
            PENDING_FLATIDs.pop(PENDING_FLATIDs.index(flat_id))


if __name__ == '__main__':
    try:
        PENDING_FLATIDs = []
        COMPLETED_FLATIDs = []

        if os.path.isfile(pending_dump):
            PENDING_FLATIDs = pickle.load(open(pending_dump))

        if os.path.isfile(completed_dump):
            COMPLETED_FLATIDs = pickle.load(open(completed_dump))

        file_list = glob(os.sep.join(['data', '*.json']))
        if len(PENDING_FLATIDs):
            logging.info("starting with pending flat list ")
            map(scrape, PENDING_FLATIDs)
        elif len(file_list):
            logging.info("bruteforcing")
            file_list = map(lambda v: v.split('.')[0].split(os.sep)[1], file_list)
            COMPLETED_FLATIDs.append(file_list)
            updated_file_list = set(generate_flatids([0, 3000000]))
            updated_file_list = updated_file_list.difference(set(file_list))
            for flat_id in updated_file_list:
                if flat_id not in COMPLETED_FLATIDs:
                    scrape(flat_id)
        else:
            scrape('908237')
    except (KeyboardInterrupt, SystemExit), error:
        logging.info(error)
    finally:
        with open(pending_dump, 'wb') as f: pickle.dump(PENDING_FLATIDs, f)
        with open(completed_dump, 'wb') as f: pickle.dump(COMPLETED_FLATIDs, f)
