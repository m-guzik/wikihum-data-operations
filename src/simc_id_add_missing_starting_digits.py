"""
SIMC ID should consist of 7 digits. Some elements in WikiHum have a shorter identifier in which the digits '0' at the beginning
of the identifier were erroneously omitted. This script searches for elements that have exactly one value specified for SIMC ID
property and this value is shorter than 7 characters. These values are then filled with  missing zeroes.  
"""

import os
import time
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query



WRITE = False

load_dotenv()

wbi_config['MEDIAWIKI_API_URL'] = 'https://wikihum.lab.dariah.pl/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://wikihum.lab.dariah.pl/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'https://wikihum.lab.dariah.pl'

WIKIDARIAH_CONSUMER_TOKEN = os.environ.get('WIKIDARIAH_CONSUMER_TOKEN')
WIKIDARIAH_CONSUMER_SECRET = os.environ.get('WIKIDARIAH_CONSUMER_SECRET')
WIKIDARIAH_ACCESS_TOKEN = os.environ.get('WIKIDARIAH_ACCESS_TOKEN')
WIKIDARIAH_ACCESS_SECRET = os.environ.get('WIKIDARIAH_ACCESS_SECRET')

P_SIMC_ID = 'P75'



def add_leading_zeros(simc_id: str) -> str:
    """
    Adds zeros at the beginning of the provided identifier value, so that the whole string consists of seven digits

    Parameters
    ----------
    simc_id : str
        current SIMC identifier 

    Returns
    -------
    str
        new SIMC identifier in correct seven-digit format
    """
    number_of_zeros = 7 - len(simc_id)
    new_simc_id =  '{}{}'.format('0' * number_of_zeros, simc_id)
    return new_simc_id



if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/02.1_simc_id_add_missing_starting_digits.txt'

    query = """ SELECT ?item WHERE {
                    ?item p:P75 ?statement0.
                    ?statement0 (ps:P75) ?value .
                    {
                        SELECT ?item (COUNT(?value) AS ?count) WHERE {
                            ?item p:P75 ?statement0.
                            ?statement0 (ps:P75) ?value .
                        } 
                        GROUP BY ?item
                        HAVING (?count = 1) 
                    }
                    FILTER (STRLEN(STR(?value)) < 7)
                } """

    results = execute_sparql_query(query)
    output = []
    for result in results["results"]["bindings"]:
        output.append(result["item"]["value"])

    for item_link in output:
        position = item_link.rfind(r'/')
        item_id = item_link[position+1:]

        item = wbi.item.get(entity_id=item_id)
        for claim in item.claims:
            if claim.mainsnak.property_number == P_SIMC_ID:
                simc_id_claim_value = claim.mainsnak.datavalue["value"]
                new_simc_id_value = add_leading_zeros(simc_id_claim_value)

                if WRITE and len(new_simc_id_value) == 7:
                    print(item_id)
                    value_to_update = claim.mainsnak.datavalue
                    new_simc_datavalue = {'value': new_simc_id_value, 'type': 'string'}
                    value_to_update.update(new_simc_datavalue)
                    item.write()
                    with open(report_path, 'a', encoding='utf-8') as report:
                        report.write(f"{item_link} missing zeroes in SIMC ID were added {new_simc_id_value} \n")
    

    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n")

