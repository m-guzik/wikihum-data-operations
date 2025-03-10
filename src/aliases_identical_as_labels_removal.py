"""
Aliases of an item in WikiHum should be different than the item label. This script is searching for such duplicated aliases 
and removes them. 
"""

import os
import time
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query
from wikibaseintegrator.wbi_exceptions import MWApiError, ModificationFailed



WRITE = True

load_dotenv()

wbi_config['MEDIAWIKI_API_URL'] = 'https://wikihum.lab.dariah.pl/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://wikihum.lab.dariah.pl/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'https://wikihum.lab.dariah.pl'

WIKIDARIAH_CONSUMER_TOKEN = os.environ.get('WIKIDARIAH_CONSUMER_TOKEN')
WIKIDARIAH_CONSUMER_SECRET = os.environ.get('WIKIDARIAH_CONSUMER_SECRET')
WIKIDARIAH_ACCESS_TOKEN = os.environ.get('WIKIDARIAH_ACCESS_TOKEN')
WIKIDARIAH_ACCESS_SECRET = os.environ.get('WIKIDARIAH_ACCESS_SECRET')



if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/04.1_remove_aliases_identical_as_labels.txt'

    query = """ SELECT DISTINCT ?item ?itemLabel ?itemAlias WHERE {
                    ?item rdfs:label ?itemLabel .
                    ?item skos:altLabel ?itemAlias.
                    FILTER (STR(?itemLabel) = STR(?itemAlias))
                }
                ORDER BY ?item
            """

    results = execute_sparql_query(query)
    output = []
    for result in results["results"]["bindings"]:
        output.append(result["item"]["value"])

    for entity_link in output:
        position = entity_link.rfind(r'/')
        entity_id = entity_link[position+1:]

        if entity_id.startswith('P'):
            entity = wbi.property.get(entity_id=entity_id)

        else: 
            entity = wbi.item.get(entity_id=entity_id)

        entity_labels = entity.labels.values
        entity_aliases = entity.aliases.aliases

        print(entity_link)
        report_message = f"{entity_link} "

        languages = ['pl', 'en']

        for language in languages:
            label = entity_labels.get(language)
            aliases = entity_aliases.get(language)
            if not aliases:
                continue
            for alias in aliases:
                if label == alias:
                    if WRITE:
                        try:
                            alias.remove()
                            entity.write()
                            report_message += f"{language} {alias} "
                            print(language, alias, "- Alias same as label has been removed ")
                        except (MWApiError, ModificationFailed) as e:
                            print(" - error writing to Wikibase")
                            report_message += f"- error writing to Wikibase {e}"
                    else:
                        report_message += f"{language} {alias} - Alias same as label "
                    continue

        with open(report_path, 'a', encoding='utf-8') as report:
            report.write(report_message + "\n")


    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n \n \n")

