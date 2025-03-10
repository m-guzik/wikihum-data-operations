"""
Script that checks how many values there are in a given property (AHP ID, PRNG ID) in elements from the provided file.
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

P_AHP_ID = 'P81'
P_PRNG_ID = 'P76'


if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/01.3_ahp_prng_checked_elements.txt'

    data_file = open('data_2025/01.3_AHP_PRNG_elements_to_check.csv')
    file_reader = csv.reader(data_file)

    property_to_check = P_AHP_ID

    for row in file_reader:
        item_link = row[0]
        position = item_link.rfind(r'/')
        item_id = item_link[position+1:]
        item = wbi.item.get(entity_id=item_id)

        number_of_values_for_a_property = 0
        for claim in item.claims:
            if claim.mainsnak.property_number == property_to_check:
                number_of_values_for_a_property += 1
        
        if number_of_values_for_a_property == 0:
            print("There is no value for property " + property_to_check + " in element " + item_link)
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"There is no value for property {property_to_check} in element {item_link}.\n")
        elif number_of_values_for_a_property > 1:
            print("Właściwość " + property_to_check + " ma więcej niż jedną wartość dla elementu " + item_link)

            for claim in item.claims:
                if claim.mainsnak.property_number == property_to_check:
                    print("Value " + str(claim.mainsnak.datavalue))
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"Property {property_to_check} has more than one value for element {item_link}.\n")


    data_file.close()

    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n")

