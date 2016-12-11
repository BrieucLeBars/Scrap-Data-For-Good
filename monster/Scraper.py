# -*- coding: utf-8 -*-
"""A module for scraping Monster for jobs.

This module is the driver for a Monster scraper. It controls the process of
instantiating a Selenium browser to scrape, and controlling that browser
throughout the entire process. It also handles the Threading, parsing, and
storage that takes place.

Usage:

    python job_scraper.py <job title> <job location> <radius>
"""

import sys
import os

wd = os.path.abspath('.')
sys.path.append(wd + '/../')
import datetime
import pytz
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from general_utilities.query_utilities import get_html, format_query
from general_utilities.navigation_utilities import issue_driver_query
from general_utilities.parsing_utilities import parse_num
from general_utilities.threading_utilities import HrefQueryThread
import json
import nltk

#def parser(posting_txt):
#    try:
#        salaire = re.search(r'salaire \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
#    except:
#        salaire = None
#        pass
#    try:
#        contrat = re.search(r'type de contrat \n (.*) \n \n \n \n \n', posting_txt.lower()).group(1)
#    except:
#        contrat = None
#        pass
#    return salaire, contrat

def tags(posting_txt, tags):
    if tags == []:
        tags = ['python', 'java,', 'java ', 'javascript', 'front-end', 'back-end', 'angular', 'node',
                 'mongodb', 'sql', 'backbone', 'express', 'jee', 'j2ee', 'api rest', 'webservice rest',
                 'react']
    liste_competence = []
    for tag in tags:
        try:
            liste_competence.append(re.search(tag, posting_txt.lower()).group(0))
        except:
            pass
    return liste_competence
    
    
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
    

def scrape_job_page(driver, job_title, job_location, tags):
    """Scrape a page of jobs from Monster.

    Grab everything that is possible (or relevant) for each of the jobs posted
    for a given page. This will typically include the job title, job location,
    posting company, the date posted, and the posting text.

    Args:
    ----
        driver: Selenium webdriver
        job_title: str
        job_location: str
    """
    global job_list
    titles, locations, companies, dates, hrefs = query_for_data(driver)

    current_date = str(datetime.datetime.now(pytz.timezone('Europe/Paris')))
    json_dct = {'search_title': job_title, \
                'search_location': job_location, \
                'search_date': current_date, 'job_site': 'monster'}
    job_list = []
    thread_lst = []
    for href in hrefs:
        try:
            thread = HrefQueryThread(href.get_attribute('href'))
        except:
            print('Exception in href thread builder')
            thread = HrefQueryThread('')
        thread_lst.append(thread)
        thread.start()
    for title, location, company, date, thread in \
            zip(titles, locations, companies, dates, thread_lst):
        date_txt = date_exacte(date.text)

        try:
            small_dict = gen_small_output(title, location, company, date_txt, thread, tags)
        except:
            print('Missed element in Monster!')
        job_list.append(small_dict)
    return job_list



def query_for_data(driver):
    """Grab all relevant data on a jobs page.

    Return:
    ------
        job_titles: list
        job_locations: list
        posting_companies: list
        dates: list
        hrefs: list
    """

    job_titles = driver.find_elements_by_xpath(
        "//span[@itemprop='title']")
    job_locations = driver.find_elements_by_xpath(
        "//div[@itemprop='jobLocation']")
    posting_companies = driver.find_elements_by_xpath(
        "//span[@itemprop='name']")
    dates = driver.find_elements_by_xpath(
        "//time[@itemprop='datePosted']")
    hrefs = driver.find_elements_by_xpath("//div//article//div//h2//a")

    return job_titles, job_locations, posting_companies, dates, hrefs

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





def gen_small_output(title, location, company, date, thread, tags):
    """Format the output dictionary .

    Args:
    ----
        json_dct: dict
        title: Selenium WebElement
        location: Selenium WebElement
        company: Selenium WebElement
        date: Selenium WebElement
        thread: RequestThreadInfo object

    Return:
    ------
        json_dct: dct
    """

    thread.join()
    new_json = {}
    new_json['nom_du_poste'] = title.text
    new_json['entreprise'] = company.text
    new_json['date_publication'] = date
    try:
        lieu = arrondissement_paris(location.text, thread.posting_txt)
        new_json['lieu'] = lieu
        new_json['lieu'] = clean_lieu(new_json['lieu'])
    except:
        pass

    try:
        salaire, contrat, niveau = parser(thread.posting_txt)
        new_json['salaire'] = salaire
        new_json['type_de_contrat'] = contrat
        new_json['niveau_de_poste'] = niveau
    except:
        pass

    try:
        new_json['tags'] = tags(thread.posting_txt, tags)
    except:
        pass

    return new_json




def check_if_next(driver):
    """Check if there is a next page of job results to grab.

    Grab the clickable job links on the bottom of the page, and check if one
    of those reads 'Next'. If so, click it, and otherwise return `False`.

    Args:
    ----
        driver: Selenium webdriver

    Return: bool
    """

    page_links = driver.find_elements_by_class_name('page-link')
    # page_links will now hold a list of all the links. The last link in that
    # list will hold 'Next' for the text, if we aren't on the last page of jobs.
    last_link = page_links[-1] if page_links else None
    if last_link and last_link.text == 'Suivant':
        last_link.send_keys(Keys.ENTER)
        return True
    else:
        return False


def get_num_jobs_txt(driver):
    """Get the number of jobs text.

    It turns out this acts slightly different on OSX versus Ubuntu, and this
    function should alleviate that. The order of the two elements with 'page-title'
    is swapped in Ubuntu and OSX, and as a result the best way to do this is
    grab both elements text and concatenate.

    Args:
    ----
        driver: Selenium webdriver

    Return:
    ------
        num_jobs_txt: str
    """

    page_titles = driver.find_elements_by_class_name('page-title')
    num_jobs_txt = ''.join([page_title.text for page_title in page_titles])
    return num_jobs_txt


if __name__ == '':
    try:
        job_title = sys.argv[1]
        job_location = sys.argv[2]
        radius = sys.argv[3]
    except IndexError:
        raise Exception('Program needs a job title, job location, and radius inputted!')
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
    storage_dct = {"site_offre_emploi": 'monster', 'num_jobs': num_jobs,
                   'date_de_recherche': current_date, 'recherche': job_title, 'ville': job_location}

    is_next = True
    while is_next:
        scrape_job_page(driver, job_title, job_location)
        is_next = check_if_next(driver)
    driver.close()
    storage_dct['jobs'] = job_list
    with open('test2.json', 'w') as data_file:
        json.dump(storage_dct, data_file)