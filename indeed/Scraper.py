# -*- coding: utf-8 -*-
from __future__ import unicode_literals
"""
Created on Sun Nov 20 17:30:38 2016

@author: Lucas Bony
"""


import sys
#reload(sys)
import os
wd = os.path.abspath('.')
sys.path.append(wd + '/../')
#sys.setdefaultencoding('utf8')


import multiprocessing
import datetime
import pytz
from functools import partial
from general_utilities.query_utilities import format_query
from general_utilities.parsing_utilities import parse_num
from redis import StrictRedis
#import cPickle
#import pandas as pd
import re

from threading import Thread
from requests import get
from bs4 import BeautifulSoup
from general_utilities.parsing_utilities import find_visible_texts
import requests


class RequestInfoThread(Thread): 
    """Threading based class to issue get requests and store the results.  
    
    RequestInfoThread issues a get request on the href from an inputted row (which
    represents a job), after grabbing all of its relevant information. It then 
    stores the results as an attribute available for later access. The motivation 
    for using a class instead of simply passing a function to ThreadPool was to
    avoid creating a new connection with the database (here Mongo) for each get
    request (this would most likely overwhelm the comp. with threads). 


    Args: 
    ----
        row: bs4.BeautifulSoup object.
            Holds a job, including all of the info. that we want to parse from it. 
        job_title: str
        job_location: str
    """

    def __init__(self, row, job_title, job_location): 
        super(RequestInfoThread, self).__init__()
        self.row = row
        self.job_title = job_title
        self.job_location = job_location

    def run(self): 
        self.json_dct = self._request_info()

    def _request_info(self): 
        """Grab relevant information from the row.

        Return: 
        ------
            json_dct: dct
        """
        
        current_date = str(datetime.datetime.now(pytz.timezone('US/Mountain')))
        json_dct = {'search_title': self.job_title, \
                'search_location': self.job_location, \
                'search_date': current_date, 'job_site': 'indeed'}
        # Holds the actual CSS selector as the key and the label to store the info.
        # as as the key. 
        possible_attributes = {'.jobtitle': "job_title", '.company': "company", \
                '.location': "location", '.date': "date", \
                '.iaLabel': "easy_apply"}
        for k, v in possible_attributes.items(): 
            res = self.row.select(k)
            if res: 
                json_dct[v] = res[0].text
        # Grab the href and pass that on to another function to get that info. 
        href = self.row.find('a').get('href')
        json_dct['href'] = href
        json_dct['posting_txt'] = self._query_href(href)

        return json_dct

    def _query_href(self, href): 
        """Grab the text from the href. 

        Args: 
        ----
            href: str 

        Return: str
        """
        try:
            html = get('http://www.indeed.com' + href) if href.startswith('/') \
                    else get(href)
            soup = BeautifulSoup(html.content, 'html.parser')

            texts = soup.findAll(text=True)
            visible_texts = filter(find_visible_texts, texts)
        except Exception as e: 
            print(e)
            visible_texts = ['SSLError', 'happened']

        return ' '.join(visible_texts)
        

    

def multiprocess_pages(base_URL, job_title, job_location, page_start):
    """Grab the URLS and other relevant info. from job postings on the page.

    The Indeed URL used for job searching takes another parameter, `start`, that
    allows you to start the job search at jobs 10-20, 20-30, etc. Use this to grab
    job results from multiple pages at once, passing the result from a page on to
    a thread to grab the details from each job posting.

    Args:
    ----
        base_URL: str
        job_title: str
        job_location: str
        page_start: int
    """

    url = base_URL + '&start=' + str(page_start)
    html = get_html(url)
    # Each row corresponds to a job.
    rows = html.select('.row')
    threads = []
    mongo_update_lst = []
    for row in rows:
        thread = RequestInfoThread(row, job_title, job_location)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
        mongo_update_lst.append((thread.json_dct))

    #r = StrictRedis(host='localhost', port=6379, db=0)
	
    #r.lpush("JOB-"+job_title+"-"+job_location, *mongo_update_lst)
    return mongo_update_lst
    #print(mongo_update_lst)
    #store_in_mongo(mongo_update_lst, 'job_postings', 'indeed')
	#print(mongo_update_lst[0].keys())
	#for i in range(len(mongo_update_lst)):
	#print(mongo_update_lst[i]['job_title'])
	
def get_html(url): 
    """Issue a get request on the inputted URL and parse the results.  

    Issue a get request on the inputted `url`, and then parse the content using
    BeautifulSoup. 

    Args: 
    ----
        url: str
    
    Returns: 
    ------
        soup: bs4.BeautifulSoup object
    """
    print(url)
    try: 
        response = requests.get(url)
        good_response = check_response_code(response)
        if not good_response: 
            # Check the bad_url to see what happened.
            print('Bad URL: {}'.format(url))
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    except Exception as e: 
        print(e)
        error = "Error in contacting the URL - check that it is a valid URL!"
        raise RuntimeError(error)
        
def check_response_code(response): 
    """Check the response status code. 

    Args: 
    ----
        response: requests.models.Response

    Returns: bool
    """

    status_code = response.status_code
    if status_code == 200: 
        return True
    else: 
        print("Status code is not 200, it's {}".format(status_code))
        return False

def scrap(job_title, job_location, radius, result_nb):
    """
    Retourne une liste de dictionnaires en effectuant une recherche
    sur indeed pour l'emploi job_title autour de job_location
    avec un rayon de radius, le nombre de résultats est limité à result_nb
    """

    base_URL = 'http://www.indeed.fr/emplois?'
    query_parameters = ['q={}'.format('+'.join(job_title.split())),
        '&l={}'.format('+'.join(job_location.split())),
        '&rq={}'.format(radius), '&sort=date', '&fromage=last']

    query_URL = format_query(base_URL, query_parameters)
    print(query_URL)

#    html = get_html(query_URL)
#    try:
#        num_jobs_txt = str(html.select('#searchCount'))
#        num_jobs = int(parse_num(num_jobs_txt, 2))
#    except:
#        print('No jobs for search {} in {}'.format(job_title, job_location))
#        sys.exit(0)
#
#    current_date = str(datetime.datetime.now(pytz.timezone('US/Mountain')))
#
#
#    # Cycle through all of the job postings that we can and grab the url pointing to
#    # it, to then query it. All of the jobs should be available via the
#    # .turnstileLink class, and then the href attribute will point to the URL.
#    max_start_position = 1000 if num_jobs >= 1000 else num_jobs
#    start_positions = range(0, max_start_position, 10)

    jobs = []
    for i in range(0,result_nb,10):
        try:
            jobs.extend(multiprocess_pages(query_URL, job_title, job_location, i))
        except RuntimeError:
            pass
            #retry ?
	#cPickle.dump(jobs, "jobs")

    return jobs