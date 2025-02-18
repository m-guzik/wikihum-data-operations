"""
This script removes 'empty' elements that were left after moving data from them (script 'ahp_prng_data_transfer.py).
"""

import csv 
import os
import time
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config


WRITE = False

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

    report_path = 'data_2025/reports/01.2_ahp_prng_removed_elements.txt'

    # data_file = open('data_2025/01.2_AHP_PRNG_elements_to_remove_examples.csv')
    data_file = open('data_2025/01.2_AHP_PRNG_elements_to_remove.csv')
    file_reader = csv.reader(data_file)


    for row in file_reader:
        item_link = row[0]
        
        position = item_link.rfind(r'/')
        item_id = item_link[position+1:]

        item = wbi.item.get(entity_id=item_id)

        if WRITE:
            item.delete()
            print(item_link + " został usunięty.")
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"{item_link} został usunięty.\n")


    data_file.close()

    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n")
