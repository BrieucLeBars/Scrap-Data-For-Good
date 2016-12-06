# -*- coding: utf-8 -*-
"""
Created on Wed Nov 09 19:53:57 2016

@author: Lucas Bony
"""

import sys
import pandas as pd
import re
import csv
import json

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

#%%

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

#%%

if __name__ == "__main__":
    db_name = "JOB-agg-13781.csv"
    
    try:    
        if len(sys.argv) > 1:
            db_name = sys.argv[0]
        
    except IndexError:
        pass
    
    print "Loading databases..."
        
    db = pd.read_csv(db_name).values
    geo_loc_db = load_csv("villes_france.csv")
    _db = {}
    #TODO    
    """
    Formater : coords, nom, nb_offres
    """
    villes_count = {}
    print "Counting offers per town..."
    for row in db:
        ville = clean_town(row[2])
        if ville in villes_count.keys():
            villes_count[ville] = villes_count[ville] + 1
        else:
            villes_count[ville] = 1
    
    villes_coor = {}
    print "Creating coordinates database..."
    id_cnt = 0    
    for ville in villes_count.keys():
        ville = clean_town(ville)
        
        if not ville in villes_coor.keys():
            villes_coor[ville] = find_town(ville, geo_loc_db)
        
        _db[ville] = {"id" : id_cnt, 
                    "localisation" : villes_coor[ville], 
                    "count" : villes_count[ville]
        }
        id_cnt += 1
    output_name = "".join([db_name, "-coordinates"])
    print "Printing file with name : %s" % output_name
    json.dump(_db, open(output_name,  "wb"))
            
            
#%%
#METHODES DE TEST

def count_null_localisation(db):
    errors = {}
    cnt = 0    
    for ville in db.keys():
        if _db[ville]["localisation"] == {}:
            cnt += 1
            errors[ville] = _db[ville]
    print cnt
    return errors
            