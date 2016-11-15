# -*- coding: utf-8 -*-
"""
Created on Fri Nov 11 18:31:53 2016

@author: Lucas Bony
"""

import os
import cPickle as pkl
import csv
import json
import time

from match_lat_lon import find_town, load_csv, clean_town, clean_string
from keywords_processing import get_study, to_text, get_keywords, get_salary

import sys
reload(sys)
import os
wd = os.path.abspath('.')
sys.path.append(wd + '/../')
sys.setdefaultencoding('utf8')

import datetime
import pytz
from functools import partial
from query_utilities import get_html, format_query
from general_utilities.storage_utilities import store_in_mongo
from general_utilities.parsing_utilities import parse_num
from request_threading import RequestInfoThread

#%%

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
        pkl.dump(jobs, f)
        f.close()
    return jobs


#%%

if __name__ == "__main__":
    t_start = time.time()
    current_date = str(datetime.date.today())
    try:
        if len(sys.argv) > 3:
            scrap_keywords_fp = sys.argv[1]
            scrap_locations_w_radius_fp = sys.argv[2]
            description_keywords_fp = sys.argv[3]
            if len(sys.argv) > 4:
                #on a passe en argument le nombre de resultat max qu'on prend en compte par requete
                result_nb = sys.argv[4]
            else:
                result_nb = 1100
                scrap_keywords = json.loads(open(scrap_keywords_fp, "rb"))["keywords"]
                scrap_locations_w_radius = json.loads(open(scrap_locations_w_radius_fp, "rb"))
                
                description_keywords = json.loads(open(description_keywords_fp, "rb"))
        else:
            #TODO : ecrire l'usage
        
            raise IndexError
            pass
        
    except IndexError:
        scrap_keywords = ["programmeur"]
        scrap_locations_w_radius = {"Paris" : {"name" : "Paris",
                                               "radius" : 25}}
        description_keywords = ["java", "backend", "frontend"]
        result_nb = 1000


    print("Initialising...")
    #le fichier recensant les coordonnees geographiques de chaque ville avec leur nom    
    geo_loc_db = load_csv("villes_france.csv")    
    
    #on utilise les url des pages pour etre ser qu'on a pas de doublons    
    _hrefs = []
    #db sera la liste qui contiendra les dictionnaires de la BDD    
    db = []
    #villes_coor contiendra des dictionnaires representant les coordonnees geographiques de chaque ville 
    #pour eviter d'avoir a parser le .csv contenant chaque ville
    villes_coor = {}
    t_init = time.time()
    print("Initalized in %f seconds" % ((t_init - t_start)))
    t_intermediate = t_init    
    for job_title in scrap_keywords:
        t_intermediate = time.time()
        
        for job_location in scrap_locations_w_radius.values():
            print "Scrapping with : %s, around %s with a radius of %d" % (job_title, job_location['name'], job_location['radius'])
            jobs = scrap(job_title, job_location['name'], job_location['radius'], result_nb)
            #on parse les jobs que l'on a obtenu pour les cleaner
            job_cnt = 0
            print("    Processing %d jobs" % (len(jobs)))
            for job in jobs:
                job_cnt += 1
                print("        Job %d" % job_cnt)
                #on ne prend que les offres qui ne sont pas des doublons
                if "href" in job.keys():
                    if not job["href"] in _hrefs:
                        _hrefs.append(job["href"])
                        #on trouve les coordonnees de la localisation
                        job['location'] = clean_string(job['location']).lower()
                        if job['location'] in villes_coor.keys():
                            #si la ville où se trouve ce job a deja ete rencontree alors on connait deja ses coordonnees et on a pas besoin
                            #de parser a nouveau le .csv avec les coordonnees des villes
                            job["coordonnees"] = villes_coor[job['location']]
                        else:
                            #si on a jamais rencontre cette ville on parse le fichier
                            villes_coor[job['location']] = find_town(job["location"], geo_loc_db)
                            job["coordonnees"] = villes_coor[job['location']]          
                        #on clean la description de l'offre (pour l'instant melangee avec du html et du javascript)
                        job['posting_txt'] = to_text(job['posting_txt'])
                        #TODO trouver les etudes et les keywords
                        #on extrait le niveau apres le bac requis pour le job si on le trouve
                        job['study'] = get_study(job['posting_txt'])
                        #on extrait les keywords que l'on trouve conformement aux keywords passe en argument (description_keywords)
                        job['keywords'] = get_keywords(job['posting_txt'], description_keywords)
                        db.append(job)
            print("Scrap de %s a %s dans un rayon %d fait en %f secondes" % (job_title, job_location["name"], job_location["radius"], (time.time() - t_intermediate)))
                #TODO faire un dump de la base
    print("Base cree en %f secondes" % ((time.time() - t_start)))
    output_db_name = "".join(["JOB-db-", str(len(db)), "_samples-", str(time.time())])
    storage_dict = {"Site d'offre d'emploi": 'indeed', 'num_jobs': len(jobs),
                   'Date de recherche': current_date, 'Recherche': job_title, 'Ville': job_location}
    json.dump(storage_dict, open(output_db_name, "wb"))
                #faire un script qui permet de sortir seulement les coordonnees
            
    