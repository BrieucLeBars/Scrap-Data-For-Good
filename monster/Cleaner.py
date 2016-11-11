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

        return location
   else:
       return location


if __name__ == '__main__':
    print(date_exacte("Publiée aujourd'hui"))

    print(date_exacte("il y a 6 jours"))