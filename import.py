import argparse
import json
import time

import pp
import requests
import traceback2
from tqdm import tqdm

from google import JWTProvider


def main(args):
    print("started at {0}".format(time.time()))
    session = requests.Session()
    parallel_jobs = pp.Server()
    parallel_jobs.set_ncpus(args.threads)
    pbar = tqdm(total=564759)
    jobs = []
    auth = None
    sender = send_data
    if args.auth is not None:
        auth = args.auth
        sender = send_data_with_auth
    elif args.auth_file is not None:
        auth = JWTProvider(args.auth_file)
        sender = send_data_with_auth_file

    with open(args.json_file) as json_file:
        json_data = json.load(json_file)
        for attribute, value in json_data.iteritems():
            url = args.firebase_url
            url += attribute + '/' + '.json'
            if args.silent:
                url += '?print=silent'
            try:
                jobs.append(
                    parallel_jobs.submit(sender, (url, value, session, auth), (),
                                         ("json", "requests", "google")))
                # pbar.update(1)
                # sendData(url, value, session, args)
            except Exception, e:
                print('Caught an error: ' + traceback2.format_exc())
                print attribute, value

    for job in jobs:
        job()
        pbar.update(1)
    # If we don't wait for all jobs to finish, the script will end and kill all still open threads
    parallel_jobs.wait()
    print("finished at {0}".format(time.time()))


def send_data_with_auth(url, data_object, session, access_token):
    auth_obj = {'access_token': access_token}
    response = session.patch(url, data=json.dumps(data_object), params=auth_obj)
    if response.status_code != 200:
        print(response.status_code)
    response.close()


def send_data_with_auth_file(url, data_object, session, credentials):
    access_token = credentials.get_credentials().get_access_token()
    auth_obj = {'access_token': access_token}
    response = session.patch(url, data=json.dumps(data_object), params=auth_obj)
    if response.status_code != 200:
        print(response.status_code)
    response.close()


def send_data(url, data_object, session):
    response = session.patch(url, data=json.dumps(data_object))
    if response.status_code != 200:
        print(response.status_code)
    response.close()


if __name__ == '__main__':
    argParser = argparse.ArgumentParser(description="Import a large json file into a Firebase via json Streaming.\
                                                     Uses HTTP PATCH requests.  Two-pass script, run once normally,\
                                                     then again in --priority_mode.")
    argParser.add_argument('firebase_url',
                           help="Specify the Firebase URL (e.g. https://test.firebaseio.com/dest/path/).")
    argParser.add_argument('json_file', help="The JSON file to import.")
    argParser.add_argument('-a', '--auth', help="Optional Auth token if necessary to write to Firebase.")
    argParser.add_argument('-af', '--auth_file', help="Optional Path to Service Account JWT file.")
    argParser.add_argument('-t', '--threads', type=int, default=8, help='Number of parallel threads to use, default 8.')
    argParser.add_argument('-s', '--silent', action='store_true',
                           help="Silences the server response, speeding up the connection.")
    argParser.add_argument('-p', '--priority_mode', action='store_true',
                           help='Run this script in priority mode after running it in normal mode to write all priority values.')

    main(argParser.parse_args())
