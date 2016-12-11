# -*- coding: utf-8 -*-
import pytz
import datetime
import re
import os
import sys
import json
import csv
import time
import nltk
reload(sys)
sys.setdefaultencoding('utf-8')

from Scraper import scrape_job_page, query_for_data, gen_small_output, check_if_next, get_num_jobs_txt

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from general_utilities.storage_utilities import store_in_mongo
from general_utilities.query_utilities import get_html, format_query
from general_utilities.navigation_utilities import issue_driver_query
from general_utilities.parsing_utilities import parse_num
from general_utilities.threading_utilities import HrefQueryThread

ARRONDISSEMENTS = {
                    "1e" : {"lon" : 2.339084, "lat" : 48.860046 },
                    "2e" : {"lon" : 2.336810, "lat" : 48.8677442 },
                    "3e" : {"lon" : 2.3418373, "lat" : 48.862596},
                    "4e" : {"lon" : 2.3219262 , "lat" : 48.8541321},
                    "5e" : {"lon" : 2.3164324, "lat" : 48.8454952},
                    "6e" : {"lon" : 2.2957259, "lat" : 48.8495493},
                    "7e" : {"lon" : 2.2765373, "lat" : 48.8548817},
                    "8e" : {"lon" : 2.2761463, "lat" : 48.8732857},
                    "9e" : {"lon" : 2.3029042, "lat" : 48.8771151},
                    "10e" : {"lon" : 2.327440, "lat" : 48.8760127},
                    "11e" : {"lon" : 2.346600, "lat" : 48.8601202},
                    "12e" : {"lon" : 2.2774372, "lat" : 48.8352696},
                    "13e" : {"lon" : 2.2957121, "lat" : 48.830360},
                    "14e" : {"lon" : 2.2879483, "lat" : 48.8297343},
                    "15e" : {"lon" : 2.2238351, "lat" : 48.8417485},
                    "16e" : {"lon" : 2.1930245, "lat" : 48.8571787},
                    "17e" : {"lon" : 2.2351611, "lat" : 48.8874968},
                    "18e" : {"lon" : 2.2787765, "lat" : 48.8920242},
                    "19e" : {"lon" : 2.3178616, "lat" : 48.8871872},
                    "20e" : {"lon" : 2.3616739, "lat" : 48.8625826}       
}



def load_csv(fp):
    f = csv.reader(open(fp, "rb"), delimiter=",")
    _db = []    
    for row in f:
        _db.append(row)
    return _db
    

def clean_lieu(d):
    try:
        return re.search("([^,]+), \\w+", d['lieu'].lower()).group(1)
    except AttributeError:
        return d['lieu']
        
def find_town(town, villes_db):
   #l'arrondissement avec parenthèses
    m = re.match(r"Paris \(([0-9]{1,2}e)\)", town)
    if not m is None:
        return ARRONDISSEMENTS[m.group(1)]
    #l'arrondissement sans parenthèses
    m = re.match(r"Paris ([0-9]{1,2}e)", town)
    if not m is None:
        return ARRONDISSEMENTS[m.group(1)]
    for row in villes_db:
 
        town.replace("\xc3\x89", "e").replace("\xc3\xa9", "e")    
        town = town.lower().replace("-", " ").replace("ê", "e").replace("â", "a").replace("é", "e").replace("è", "e").replace("-", " ")
        if  town == row[4]:
            return {"lon" : row[19],
                    "lat" : row[20]} 
    return {}
    





def scrap(job_title, job_location, radius, tags):
    if os.environ['OS'][0:7] == 'Windows':
        os.environ['USER'] = 'Windows'
        
    base_URL = 'http://monster.fr/emploi/recherche/?'
    query_parameters = ['q={}'.format('-'.join(job_title.split())),
                        '&where={}'.format('-'.join(job_location.split())), '&sort=dt.rv.di',
                        '&rad={}'.format(radius)]

    query_URL = format_query(base_URL, query_parameters)
    driver = issue_driver_query(query_URL)
    job_list = []
    try:
        num_jobs_txt = get_num_jobs_txt(driver)
        num_jobs = int(parse_num(num_jobs_txt, 0))
        print(num_jobs_txt)
    except:
        print('No jobs for search {} in {}'.format(job_title, job_location))
        sys.exit(0)

    current_date = str(datetime.date.today())

    is_next = True
    while is_next:
        job_list.extend(scrape_job_page(driver, job_title, job_location, tags))
        is_next = check_if_next(driver)
    driver.close()
    for d in job_list:
        d['lieu'] = clean_lieu(d)
    return job_list
    
def start_monster(scrap_keywords, scrap_locations_w_radius, tags, types_contrat):

    t_start = time.time()
    current_date = str(datetime.date.today())

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
        
        for job_location in scrap_locations_w_radius:
            print "Scrapping with : %s, around %s with a radius of %d" % (job_title, job_location['name'], job_location['radius'])
            jobs = scrap(job_title, job_location['name'], job_location['radius'], tags)
            #on parse les jobs que l'on a obtenu pour les cleaner
#            job_cnt = 0
#            print("    Processing %d jobs" % (len(jobs)))
            for job in jobs:
                if job['lieu'] in villes_coor.keys():
                    job["coordonnees"] = villes_coor[job['lieu']]
                else:
                    _coor = find_town(job['lieu'], geo_loc_db)
                    job["coordonnees"] = _coor
                    villes_coor[job['lieu']] = _coor
                    #le jot_title utilisé pour scrapper l'offre
                    job["job_title_scrap"] = job_title
#                d = dict()
#                job_cnt += 1
#                print("        Job %d" % job_cnt)
#                #on ne prend que les offres qui ne sont pas des doublons
#                if "href" in job.keys():
#                    if not job["href"] in _hrefs:
#                        _hrefs.append(job["href"])
#                        #on trouve les coordonnees de la localisation
#                        job['location'] = clean_string(job['location']).lower()
#                        if job['location'] in villes_coor.keys():
#                            #si la ville où se trouve ce job a deja ete rencontree alors on connait deja ses coordonnees et on a pas besoin
#                            #de parser a nouveau le .csv avec les coordonnees des villes
#                            d['lieu'] = job['location']
#                            d["coordonnees"] = villes_coor[job['location']]
#                        else:
#                            #si on a jamais rencontre cette ville on parse le fichier
#                            villes_coor[job['location']] = find_town(job["location"], geo_loc_db)
#                            d["coordonnees"] = villes_coor[job['location']]          
#                        #on clean la description de l'offre (pour l'instant melangee avec du html et du javascript)
#                        job['posting_txt'] = to_text(job['posting_txt'])
#                        #TODO trouver les etudes et les keywords
#                        #on extrait le niveau apres le bac requis pour le job si on le trouve
#                        d['niveau_etudes'] = get_study(job['posting_txt'])
#                        #on extrait les keywords que l'on trouve conformement aux keywords passe en argument (description_keywords)
#                        d['tags'] = get_tags(job['posting_txt'], description_keywords)
#                        d['nom_du_poste'] = job['job_title']
#                        d['type_de_contrat'] = ""
#                        d['date_publication'] = ""
#                        d['entreprise'] = job['company']
#                        db.append(job)
                db.append(job)
            print("Scrap de %s a %s dans un rayon %d fait en %f secondes" % (job_title, job_location["name"], job_location["radius"], (time.time() - t_intermediate)))
                #TODO faire un dump de la base
    print("Base cree en %f secondes" % ((time.time() - t_start)))
    output_db_name = "".join(["JOB-db-", str(len(db)), "_samples-", str(time.time())])
    storage_dict = {"Site d'offre d'emploi": 'indeed', 'num_jobs': len(jobs),
                   'Date de recherche': current_date, 'Recherche': job_title, 'Ville': job_location}
    return db
                #faire un script qui permet de sortir seulement les coordonnees

def compte_coor(db):
    cnt = 0
    for d in db:
        cnt +=  d["coordonnees"] != {}
    return cnt
