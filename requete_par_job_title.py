# -*- coding: utf-8 -*-
"""
Created on Tue Dec 06 19:29:01 2016

@author: Lucas Bony
"""

import redis
import ast

def requete_par_job_title(job_title):
    r = redis.StrictRedis(db=1)
    resultat = []
    for key in r.keys():
        #la base redis renvoie des dictionaires sous forme de string, il faut donc les parser
        #avec la lib ast
        _offre = ast.literal_eval(r.get(key))
        if 'job_title_scrap' in _offre.keys():
            if _offre['job_title_scrap'] == job_title:
                resultat.append(_offre)
    return resultat