"""
Let's say we have three elements in WikiHum: itemA, itemB and itemC. 
We had the following situation
- itemA P84('neighborhood with') itemB
- itemB was deleted and its data was moved to itemC
- itemB was left as a value of P84 property in itemA

This script replaces itemB with itemC in the property P84 in itemA and checks if itemA is a value of the property P84 in itemC.
"""

import os
import pandas as pd 
import time
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.models import references, snaks
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query
from wikibaseintegrator.wbi_exceptions import MWApiError, ModificationFailed



WRITE = False

load_dotenv()

wbi_config['MEDIAWIKI_API_URL'] = 'https://wikihum.lab.dariah.pl/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://wikihum.lab.dariah.pl/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'https://wikihum.lab.dariah.pl'

WIKIDARIAH_CONSUMER_TOKEN = os.environ.get('WIKIDARIAH_CONSUMER_TOKEN')
WIKIDARIAH_CONSUMER_SECRET = os.environ.get('WIKIDARIAH_CONSUMER_SECRET')
WIKIDARIAH_ACCESS_TOKEN = os.environ.get('WIKIDARIAH_ACCESS_TOKEN')
WIKIDARIAH_ACCESS_SECRET = os.environ.get('WIKIDARIAH_ACCESS_SECRET')


P_NEIGHBORHOOD_WITH = 'P84'


def search_for_item_in_neighborhood(item_id: str, value: str) -> bool:
    """
    Checks if provided value exists in property P84 'nieghborhood with' of the provided item

    Parameters
    ----------
    item_id : str
        ID of the item to be checked 
    value : str
        value to search for

    Returns
    -------
    bool
        True if the value does exist in the property P84 and False otherwise
    """
    value_exist_in_neighborhood = False
    item = wbi.item.get(entity_id=item_id)
    for item_claim in item.claims:
        if item_claim.mainsnak.property_number == P_NEIGHBORHOOD_WITH and item_claim.mainsnak.datavalue["value"]["id"] == value:
            value_exist_in_neighborhood = True
    return value_exist_in_neighborhood



if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/03.4_replace_deleted_items_report.txt'

    joined_tables = pd.read_csv('data_2025/03.4_joined_data.csv')


    for index, row in joined_tables.iterrows():
        if index > 0 and index < 244:
            item_link = row["item"]
            position = item_link.rfind(r'/')
            item_id = item_link[position+1:]

            old_value_link = row["value"]
            position_old = old_value_link.rfind(r'/')
            old_value_id = old_value_link[position_old+1:]

            new_value_link = row["newValue"]
            position_new = new_value_link.rfind(r'/')
            new_value_id = new_value_link[position_new+1:]
            new_value_numeric_id = new_value_id[1:]

            item = wbi.item.get(entity_id=item_id)

            for claim in item.claims:
                if claim.mainsnak.property_number == P_NEIGHBORHOOD_WITH and claim.mainsnak.datavalue["value"]["id"] == old_value_id:
                    if search_for_item_in_neighborhood(new_value_id, item_id):
                        datavalue = {'entity-type': 'item', 'numeric-id': new_value_numeric_id, 'id': new_value_id}
                        if WRITE:
                            try:
                                claim.mainsnak.datavalue["value"] = datavalue
                                item.write()
                                print(item_link + " deleted item " + old_value_id + " was replaced " + new_value_id) 
                                with open(report_path, 'a', encoding='utf-8') as report:
                                    report.write(f"{item_link} deleted item {old_value_id} was replaced {new_value_id} \n")
                            except (MWApiError, ModificationFailed) as e:
                                print(item_link + " error writing to Wikibase")
                                with open(report_path, 'a', encoding='utf-8') as report:
                                    report.write(f"{item_link} error writing to Wikibase {e} \n")

                    else:
                        print(item_link + " item does not exist in neighborhood of item " + new_value_link) 
                        with open(report_path, 'a', encoding='utf-8') as report:
                            report.write(f"{item_link} item does not exist in neighborhood of item {new_value_link} \n")



    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n")


