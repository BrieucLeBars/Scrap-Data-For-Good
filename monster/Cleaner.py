# -*- coding: utf-8 -*-
import pytz
import datetime
import re


def date_exacte(date_txt):
    """Regle le probleme du Publiee il y a x jours
    pour rendre la vraie date de publication"""
    current_date = datetime.date.today()
    if re.search(r"aujourd", date_txt):
        return str(current_date)
    elif re.search(r'il y a (.*) jour', date_txt):
        nb_jours = re.search(r'il y a (.*) jour', date_txt)
        new_date = current_date + datetime.timedelta(days=-int(nb_jours.group(1)))
        return str(new_date)
    else:
        return date_txt


def arrondissement_paris(location, posting_txt):
   if re.search(r'Paris', location):
       #chercher code postale dans posting_txt
       try:
           arrondissement = re.search(r'Paris 750([0-9][0-9])', posting_txt.lower()).group(1)
           lieu = 'Paris ' + arrondissement
           return lieu
       except:
           return 'Paris'
   else:
       return location

def parser(posting_txt):
    try:
        salaire = re.search(r'salaire \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
    except:
        salaire = None
        pass
    try:
        contrat = re.search(r'type de contrat \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
    except:
        contrat = None
        pass
    return salaire, contrat

def tags(posting_txt):
    liste_tag = ['python', 'java,', 'java ', 'javascript', 'front-end', 'back-end', 'angular', 'node',
                 'mongodb', 'sql', 'backbone', 'express', 'jee', 'j2ee', 'api rest', 'webservice rest',
                 'react']
    liste_competence = []
    for tag in liste_tag:
        try:
            liste_competence.append(re.search(tag, posting_txt.lower()).group(0))
        except:
            pass
    return liste_competence

