import redis
import json
import ast
base_de_jobs = redis.StrictRedis(host='localhost', port=6379, db=0)


def append_redis_database(job_list):
    """ Mettre la liste des jobs dans la base de données Redis"""
    base_de_jobs.set('job_list', job_list)
    print(base_de_jobs.get('job_list'))

def rechercher_element(base):
    """Requêter la base"""
    for key in base.scan_iter():
        value = base.get(key).decode("utf-8")
        s = ast.literal_eval(value)
        if 'java,' in s['tags'] or 'java ' in s['tags']:
            print('%s : %s' % (key.decode(), str(s)))

def preparation_requete(url):
    """Passer url à requête redis"""

if __name__ == '__main__':
    base_de_jobs = redis.StrictRedis(host='localhost', port=6379, db=0)
    rechercher_element(base_de_jobs)