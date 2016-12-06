from __future__ import unicode_literals
"""A module for scraping Indeed for jobs.

This module is the driver for an Indeed scraper. It controls the process of issuing
requests, parsing the contents of those requests, and storing them. It also handles
the threading and multiprocessing that is used to speed up the scraping process.

Usage:

    python job_scraper.py <job title> <job location> <radius>
"""

import sys
reload(sys)
import os
wd = os.path.abspath('.')
sys.path.append(wd + '/../')
sys.setdefaultencoding('utf8')


import multiprocessing
import datetime
import pytz
from functools import partial
from general_utilities.query_utilities import get_html, format_query
from general_utilities.storage_utilities import store_in_mongo
from general_utilities.parsing_utilities import parse_num
from request_threading import RequestInfoThread
from redis import StrictRedis
import cPickle
import pandas as pd

def _latin_to_utf8(string):
    sys.setdefaultencoding('Latin-1')
    s = string.decode('Latin-1')
    return s.encode('utf8')
        
def _format_json(json):
    for key in json.keys():
        if key in ['company', 'posting_txt']:
            json[key] = str(json[key])


    

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


if __name__ == '__main__':
    try:
        job_title = sys.argv[1]
        job_location = sys.argv[2]
        radius = sys.argv[3]
        result_nb = int(sys.argv[4])
    except IndexError:
        #raise Exception('Program needs a job title, job location, and radius inputted!')
        job_title = 'developpeur'
        job_location = 'Paris'
        radius = 25
        result_nb = 60


    base_URL = 'http://www.indeed.fr/emplois?'
    query_parameters = ['q={}'.format('+'.join(job_title.split())),
        '&l={}'.format('+'.join(job_location.split())),
        '&rq={}'.format(radius), '&sort=date', '&fromage=last']

    query_URL = format_query(base_URL, query_parameters)
    print(query_URL)

    html = get_html(query_URL)
    try:
        num_jobs_txt = str(html.select('#searchCount'))
        num_jobs = int(parse_num(num_jobs_txt, 2))
    except:
        print('No jobs for search {} in {}'.format(job_title, job_location))
        sys.exit(0)

    current_date = str(datetime.datetime.now(pytz.timezone('US/Mountain')))
    storage_dct = {'job_site': 'indeed', 'num_jobs': num_jobs,
        'date': current_date, 'title': job_title, 'location': job_location}

    # Cycle through all of the job postings that we can and grab the url pointing to
    # it, to then query it. All of the jobs should be available via the
    # .turnstileLink class, and then the href attribute will point to the URL.
    max_start_position = 1000 if num_jobs >= 1000 else num_jobs
    start_positions = range(0, max_start_position, 10)
    db_path = "".join(["db_", job_title, "_", job_location, "_", str(radius), "_", str(result_nb)])
	
    jobs = []
    for i in range(0,result_nb,10):
        try:
            jobs.extend(multiprocess_pages(query_URL, job_title, job_location, i))
        except RuntimeError:
            pass
            #retry ?
	#cPickle.dump(jobs, "jobs")
    with open("".join([db_path, ".pkl"]), 'w') as f:
        cPickle.dump(jobs, f)
        f.close()
	pd.DataFrame(jobs).to_excel("".join([db_path, ".xlsx"]))
	
#        execute_queries = partial(multiprocess_pages, query_URL, \
#            job_title, job_location)
#        pool = multiprocessing.Pool(multiprocessing.cpu_count())
#        pool.map(execute_queries, start_positions)
 
def scrap(job_title, job_location, radius, result_nb):


    base_URL = 'http://www.indeed.fr/emplois?'
    query_parameters = ['q={}'.format('+'.join(job_title.split())),
        '&l={}'.format('+'.join(job_location.split())),
        '&rq={}'.format(radius), '&sort=date', '&fromage=last']

    query_URL = format_query(base_URL, query_parameters)
    print(query_URL)

    html = get_html(query_URL)
    try:
        num_jobs_txt = str(html.select('#searchCount'))
        num_jobs = int(parse_num(num_jobs_txt, 2))
    except:
        print('No jobs for search {} in {}'.format(job_title, job_location))
        sys.exit(0)

    current_date = str(datetime.datetime.now(pytz.timezone('US/Mountain')))
    storage_dct = {'job_site': 'indeed', 'num_jobs': num_jobs,
        'date': current_date, 'title': job_title, 'location': job_location}

    # Cycle through all of the job postings that we can and grab the url pointing to
    # it, to then query it. All of the jobs should be available via the
    # .turnstileLink class, and then the href attribute will point to the URL.
    max_start_position = 1000 if num_jobs >= 1000 else num_jobs
    start_positions = range(0, max_start_position, 10)
    db_path = "".join(["db_", job_title, "_", job_location, "_", str(radius), "_", str(result_nb)])
	
    jobs = []
    for i in range(0,result_nb,10):
        try:
            jobs.extend(multiprocess_pages(query_URL, job_title, job_location, i))
        except RuntimeError:
            pass
            #retry ?
	#cPickle.dump(jobs, "jobs")
    with open("".join([db_path, ".pkl"]), 'w') as f:
        cPickle.dump(jobs, f)
        f.close()
    return jobs