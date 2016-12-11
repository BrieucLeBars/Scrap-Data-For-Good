# -*- coding: utf-8 -*-
"""
Created on Sun Nov 20 17:40:58 2016

@author: Lucas Bony
"""

import sys
import pandas as pd
import re
import csv
import json
import time
import nltk
from bs4 import BeautifulSoup
import datetime

from Scraper import scrap
#TODO créer un fichier pour gérer les noms qui ne sont pas des communes (La Défense, Montparnasse, République...)

#%%

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

def clean_town(town):
    m = re.match(r"(.+) \(?[0-9]{2}\)?", town)
    if type(m) == type(None):
        return town
    else:
        return m.group(1)

def clean_string(s):
    s = s.replace("-", " ")
    s = s.replace("é", "e")    
    s = s.replace("è", "e")
    s = s.replace("ê", "e")
    s = s.replace("â", "a")
    s = s.replace("î", "i")
    s = s.replace("'", " ")
    s = s.replace("ë", "e")
    s = s.replace("û", "u")
    return s    
    
def load_csv(fp):
    f = csv.reader(open(fp, "rb"), delimiter=",")
    _db = []    
    for row in f:
        _db.append(row)
    return _db
    
def find_town(town, villes_db):
   #l'arrondissement avec parenthèses
    m = re.match(r"[Pp]aris \(([0-9]{1,2}e)\)", town)
    if not m is None:
        return ARRONDISSEMENTS[m.group(1)]
    #l'arrondissement sans parenthèses
    m = re.match(r"[Pp]aris ([0-9]{1,2}e)", town)
    if not m is None:
        return ARRONDISSEMENTS[m.group(1)]
    for row in villes_db:
 
        town.replace("\xc3\x89", "e").replace("\xc3\xa9", "e")    
        town = town.lower().replace("-", " ").replace("ê", "e").replace("â", "a").replace("é", "e").replace("è", "e").replace("-", " ")
        if  town == row[4]:
            return {"lon" : row[19],
                    "lat" : row[20]} 
    return {}
    
def clean_nom_entreprise(entreprise):
    entreprise = entreprise.replace("\n","")
    
    m = re.match(r'[ ]+([\w ]+)', entreprise)
    if not m is None:
        return m.group(1)
    else:
        return entreprise



def get_study(text):
    """
    Finds the required level of study specified in eah job offer if possible
    PARAMETERS:
        text : the text which is used of scanning
    RETURNS:
        the study level as a string if it's been found, otherwise returns nothing ("")
    
    """
    m = re.match(".+\(?([bB]ac ?\+ ?[0-9]/?-?[0-9]?)\)?.+", text)
    if m is None:
        return ""
    else:
        return m.group(1).lower().replace(" ", "")
        
def find_study(db, key="etudes", text_key='posting_txt'):
    """
    Applies the get_study method to a whole database on the "posting_txt" key of each dictionary
    PARAMETERS :
        db : the database as a list of dictionary
        key : the key under which the study level will be stored in dictionaries
        text_key : the key to use to find the job offer description
    RETURNS : 
        the database with the study level
    """
    for d in db:
        d[key] = _get_study(d['posting_txt'])
    return db
    
        
def get_salary(text):
    m = re.match(r".+([0-9]{1,5}k?€).+", text)
    if m is None:
        return ""
    else:
        return m.group(1)


def get_tags(text, tags):
    """
    Returns the tags found in the text arg according to the tags list
    PARAMETERS : 
        text : a text as a string
        tags : the tags to find as a list of strings
    RETURNS :
        the tags which have actually been found as a list of strings
    """
    _tags = []    
    for kw in tags:
        if kw.lower() in text.lower():
            _tags.append(kw)
    return _tags
    
def find_tags(db, tags, key="tags", text_key="posting_txt"):
    """
    Applies the get_tags method to a whole database on the "posting_txt" key of each dictionary
    PARAMETERS :
        db : the database as a list of dictionary
        tags : the tags to find as a list of strings
        key : the key under which tags will be stored in the dictionaries
        text_key : the key under which the job offer description can be found
    RETURNS : 
        the database with the tags
    """
    for d in db:
        d[key] = get_tags(d[text_key], tags)
        
    return db
    

def tags(posting_txt, file):
    with open(file, 'r') as f:
        content = f.readlines()

    liste_tag= []
    for line in content:
        tokens = decompose(line)
        liste_tag += tokens
    liste_competence = []
    offer_content = decompose(posting_txt)
    for tag in liste_tag:
        if tag in offer_content:
            liste_competence.append(tag)
    return liste_competence
    
def decompose(content):
    tokens = nltk.word_tokenize(content)
    tokens = [item.lower() for item in tokens]
    return tokens
    
def parser(posting_txt):
    tokens = decompose(posting_txt)
    try:
        salaire = re.search(r'salaire \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
    except:
        try:
            i = tokens.index('salaire')
            salaire = tokens[i : i + 10]
        except:
            salaire = None
            pass
    try:
        contrat = re.search(r'type de contrat \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
    except:
        contrat = tags(posting_txt, '../type_contrat.txt')

    try:
        niveau = re.search(r'niveau de poste \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
    except:
        try:
            i = tokens.index('expérience')
            niveau = tokens[i : i + 10]
        except:
            niveau = None
    return salaire, contrat, niveau

    
	
def to_text(html):
    text = BeautifulSoup(html).getText()
    text = text.replace("\n", "").replace("\r","").replace("\t", "")
    return text

def start_indeed(scrap_keywords, scrap_locations_w_radius, description_keywords, result_nb, types_contrat):

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
            jobs = scrap(job_title, job_location['name'], job_location['radius'], result_nb)
            #on parse les jobs que l'on a obtenu pour les cleaner
            job_cnt = 0
            print("    Processing %d jobs" % (len(jobs)))
            for job in jobs:
                d = dict()
                job_cnt += 1
                print("        Job %d" % job_cnt)
                #on ne prend que les offres qui ne sont pas des doublons
                if "href" in job.keys():
                    if not job["href"] in _hrefs:
                        _hrefs.append(job["href"])
                        #on trouve les coordonnees de la localisation
                        job['location'] = clean_town(clean_string(job['location']).lower())
                        print job['location']
                        if 'expérience' in job['posting_txt']:
                            print job['posting_txt']
                        if job['location'] in villes_coor.keys():
                            #si la ville où se trouve ce job a deja ete rencontree alors on connait deja ses coordonnees et on a pas besoin
                            #de parser a nouveau le .csv avec les coordonnees des villes
                            d['lieu'] = job['location']
                            d["coordonnees"] = villes_coor[job['location']]
                        else:
                            #si on a jamais rencontre cette ville on parse le fichier
                            villes_coor[job['location']] = find_town(job["location"], geo_loc_db)
                            d["coordonnees"] = villes_coor[job['location']]          
                        #on clean la description de l'offre (pour l'instant melangee avec du html et du javascript)
                        job['posting_txt'] = to_text(job['posting_txt'])
                        try:
                            salaire, contrat, niveau = parser(job['posting_txt'])
                        except:
                            salaire = ""
                            contrat = ""
                            niveau = ""
                        #TODO trouver les etudes et les keywords
                        #on extrait le niveau apres le bac requis pour le job si on le trouve
                        d['lieu'] = job['location']
                        d['niveau_de_poste'] = niveau
                        d['salaire'] = salaire
                        #on extrait les keywords que l'on trouve conformement aux keywords passe en argument (description_keywords)
                        d['tags'] = get_tags(job['posting_txt'], description_keywords)
                        d['nom_du_poste'] = job['job_title']
                        d['type_de_contrat'] = contrat
                        d['date_publication'] = ""
                        #le job_title utilisé pour scrapper cette offre
                        d['job_title_scrap'] = job_title
                        if 'company' in job.keys():
                            d['entreprise'] = clean_nom_entreprise(job['company'])
                        else:
                            d['entreprise'] = ""
                        db.append(d)
            print("Scrap de %s a %s dans un rayon %d fait en %f secondes" % (job_title, job_location["name"], job_location["radius"], (time.time() - t_intermediate)))
                #TODO faire un dump de la base
    print("Base cree en %f secondes" % ((time.time() - t_start)))
    output_db_name = "".join(["JOB-db-", str(len(db)), "_samples-", str(time.time())])
    storage_dict = {"Site d'offre d'emploi": 'indeed', 'num_jobs': len(jobs),
                   'Date de recherche': current_date, 'Recherche': job_title, 'Ville': job_location}
    return db
                #faire un script qui permet de sortir seulement les coordonnees




