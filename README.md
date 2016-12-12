# A faire

Brieuc : Reste à filtrer les résultats

Lucas : harmoniser la recherche de coordonnées, cleaner le scrapper indeeed, mettre en place le filtre à l'expérience, scrapper la date de publication

Cyril : utiliser l'API google pour trouver les adresses des jobs avec le nom de l'entreprise et le nom du lieu

-> Faire l'API pour accéder aux données de la base, et notamment faire une requête qui prend en paramètre
   un nom de job et appelle la méthode requete_par_job_title qui se trouve dans le script requete_par_job_title.py

-> Regarder comment installer un serveur redis sur le serveur


# build_database.py

Créer la base de données (ne supprime pas l'anciennce pour le moment) et la stock
dans une base redis.

# requete_par_job_title.py

Permet de faire une requête dans la base de données par job_title qui a utilisé
pour scrapper l'offre.

# automatisation.py

Automatise le scrap toutes les heures 

## Notes

While it might be easier for others if I placed each individual scraper into 
its own repo (so users could download just the repo for the scraper they wanted, 
rather than this entire repo), I prefer this structure of keeping them all 
centralized. 

The majority, if not all, of these scrapers will not actively be kept up to 
date. So, if the website that they were built on changes in a way that it 
breaks the scraper, users will have to refactor the scraper to account for 
that. 

As a last quick note - all of these are built using Python 3. That might be
worth nothing given the current state of the libraries available in Python 
2 and 3 for interacting with the web.

 

