# -*- coding: utf-8 -*-
import pytz
import datetime
import re
import nltk

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
        contrat = tags(posting_txt, './type_contrat.txt')

    try:
        niveau = re.search(r'niveau de poste \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
    except:
        try:
            i = tokens.index('expérience')
            niveau = tokens[i : i + 10]
        except:
            niveau = None
    return salaire, contrat, niveau

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

