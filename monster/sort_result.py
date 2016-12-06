import Cleaner

def tri_resultat(job_list):
    new_job_list = []
    with open('./experience.txt', 'r') as f:
        content = f.readlines()

    liste_experience= []
    for line in content:
        tokens = Cleaner.decompose(line)
        liste_experience += tokens
    for element in job_list:
        if type(element['niveau_de_poste']) is str:
            element['niveau_de_poste'] = Cleaner.decompose(element['niveau_de_poste'])
        elif element['niveau_de_poste'] is None:
            element['niveau_de_poste'] = []
        compteur = 0
        for tag in liste_experience:
            if tag in element['niveau_de_poste']:
                compteur += 1
        if compteur == 0:
            new_job_list.append(element)
    return new_job_list


