"""A module for scraping Monster for jobs.

This module is the driver for a Monster scraper. It controls the process of
instantiating a Selenium browser to scrape, and controlling that browser
throughout the entire process. It also handles the Threading, parsing, and
storage that takes place.

Usage:

    python job_scraper.py <job title> <job location> <radius>
"""
# -*- coding: utf-8 -*-
import sys
import os

wd = os.path.abspath('.')
sys.path.append(wd + '/../')
import datetime
import pytz
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from general_utilities.storage_utilities import store_in_mongo
from general_utilities.query_utilities import get_html, format_query
from general_utilities.navigation_utilities import issue_driver_query
from general_utilities.parsing_utilities import parse_num
from general_utilities.threading_utilities import HrefQueryThread
import json
import Cleaner

def scrape_job_page(driver, job_title, job_location):
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
        date_txt = Cleaner.date_exacte(date.text)

        try:
            small_dict = gen_small_output(title, location, company, date_txt, thread)
        except:
            print('Missed element in Monster!')

        try:
            job_list.append(small_dict)
        except IOError as err:
            print(err)



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



def gen_small_output(title, location, company, date, thread):
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
    new_json['Nom du Poste'] = title.text
    new_json['Entreprise'] = company.text
    new_json['Date Publication'] = date
    try:
        lieu = Cleaner.arrondissement_paris(location.text, thread.posting_txt)
        new_json['Lieu'] = lieu
    except:
        pass

    try:
        salaire, contrat = Cleaner.parser(thread.posting_txt)
        new_json['Salaire'] = salaire
        new_json['Type de Contrat'] = contrat
    except:
        pass

    try:
        new_json['Tags'] = Cleaner.tags(thread.posting_txt)
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


if __name__ == '__main__':
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
    storage_dct = {"Site d'offre d'emploi": 'monster', 'num_jobs': num_jobs,
                   'Date de recherche': current_date, 'Recherche': job_title, 'Ville': job_location}

    is_next = True
    while is_next:
        scrape_job_page(driver, job_title, job_location)
        is_next = check_if_next(driver)
    driver.close()
    storage_dct['jobs'] = job_list
    with open('test2.json', 'w') as data_file:
        json.dump(storage_dct, data_file)