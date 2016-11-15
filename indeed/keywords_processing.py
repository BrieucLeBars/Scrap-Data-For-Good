# -*- coding: utf-8 -*-
import sys
import gc
import pandas as pd
import cPickle as pkl
import re

from bs4 import BeautifulSoup

def get_study(text):
    """
    Finds the required level of study specified in eah job offer if possible
    PARAMETERS:
        text : the text which is used of scanning
    RETURNS:
        the study level as a string if it's been found, otherwise returns nothing ("")
    
    """
    m = re.match(".+\(?(bac ?\+ ?[0-9]/?[0-9]?|Bac ?\+ ?[0-9]/?-?[0-9]?)\)?.+", text)
    if m is None:
        return ""
    else:
        return m.group(1).lower().replace(" ", "")

def _get_study(text):
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
    
	
def to_text(html):
    text = BeautifulSoup(html).getText()
    text = text.replace("\n", "").replace("\r","").replace("\t", "")
    return text


    
#%%
if __name__ == "__main__":
	db_names = []
	if len(sys.argv) > 2:
		for i in range(1, len(sys.argv) - 1):
			db_names.append(sys.argv[i])
		tags_path = sys.argv[len(sys.argv) - 1]
		
        	with open(tags_path, "rb") as f:
        			tags = pkl.load(f)
        			f.close()
	
	for db_name in db_names:
		with open(db_name, "rb") as f:
			db = pkl.load(f)
			f.close()
	
		_db = []
		for row in db:
			row['posting_txt'] = to_text(row['posting_text'])
			
			row['niveau étude'] = get_study(row['posting_txt'])
			
			new_tags = get_tags(row['posting_txt'], tags)
			if "tags" in row.keys():
				old_tags = row['tags']
				for keyword in old_tags:
					if not keyword in new_tags:
						new_tags.append(keyword)
			row['tags'] = new_tags	
			
			_db.append(row)
			
			with open("".join([db_name, "tags.pkl"]), "wb") as f:
				pkl.dump(f, _db)
		
	
