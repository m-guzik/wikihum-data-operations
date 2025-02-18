"""
Every element with SIMC ID should have exactly one value of that property. This script searches for elements that have two values
of SIMC identifier, checks if they are equal and if that is the case removes the second value and moves the references from
the second value to the first one.
"""

import os
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

    report_path = 'data_2025/reports/02.2_simc_id_remove_duplicates.txt'

    query = """ SELECT DISTINCT ?item WHERE {
                    ?item p:P75 ?statement0.
                    ?statement0 (ps:P75) ?value .
                    {
                        SELECT ?item (COUNT(?value) AS ?count) WHERE {
                            ?item p:P75 ?statement0.
                            ?statement0 (ps:P75) ?value .
                        } 
                        GROUP BY ?item
                        HAVING (?count = 2)
                    }
                } """
    results = execute_sparql_query(query)
    output = []
    for result in results["results"]["bindings"]:
        output.append(result["item"]["value"])


    for item_link in output:
        position = item_link.rfind(r'/')
        item_id = item_link[position+1:]

        item = wbi.item.get(entity_id=item_id)

        claims_simc_id = []
        for claim in item.claims:
            if claim.mainsnak.property_number == P_SIMC_ID:
                claims_simc_id.append(claim)

        claim_1 = claims_simc_id[0]
        claim_2 = claims_simc_id[1]

        claim_1_value = claim_1.mainsnak.datavalue["value"]
        claim_2_value = claim_2.mainsnak.datavalue["value"]

        new_claim_1_value = add_leading_zeros(claim_1_value)
        new_claim_2_value = add_leading_zeros(claim_2_value)

        if new_claim_1_value != new_claim_2_value:
            print(item_link + "incompatible SIMC ID  " + new_claim_1_value + " / " + new_claim_2_value)
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"{item_link} incompatible SIMC ID {new_claim_1_value} / {new_claim_2_value} \n")
            continue

        claim_2_number_of_qualifiers = len(claim_2.qualifiers.qualifiers)
        if claim_2_number_of_qualifiers != 0 :
            print(item_link + "There are qualifiers in the second value of the SIMC ID")
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"{item_link} There are qualifiers in the second value of the SIMC ID \n")
            continue

        claim_2_number_of_references = len(claim_2.references)
        if claim_2_number_of_references != 1 :
            print(item_link + "More than one reference in the second value of the SIMC ID")
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"{item_link} More than one reference in the second value of the SIMC ID \n")
            continue
            
        if new_claim_1_value != claim_1_value:
            claim_1.mainsnak.datavalue["value"] = new_claim_1_value
            print(item_link + "SIMC  ID value was changed " + claim_1_value + " to " + new_claim_1_value)
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"{item_link} SIMC  ID value was changed {claim_1_value} to {new_claim_1_value} \n")


        claim_2_reference = claim_2.references.references[0]

        snaks_references = snaks.Snaks()
        for reference_element in claim_2_reference:
            reference_snaktype = reference_element.snaktype 
            reference_p_number = reference_element.property_number
            reference_value = reference_element.datavalue
            reference_datatype = reference_element.datatype

            new_reference = snaks.Snak(snaktype=reference_snaktype, property_number=reference_p_number, datavalue=reference_value, datatype=reference_datatype)
            snaks_references.add(new_reference)

        references_order = claim_2_reference.snaks_order
        reference_to_add = references.Reference(snaks=snaks_references, snaks_order=references_order)

        try:
            if WRITE:
                claim_1.references.add(reference_to_add)
                claim_2.remove()
                item.write()
                print(item_link + " References from the second value was moved, second value was deleted")
                with open(report_path, 'a', encoding='utf-8') as report:
                    report.write(f"{item_link} References from the second value was moved, second value was deleted \n")
        except (MWApiError, ModificationFailed) as e:
            print(item_link + " error writing to Wikibase")
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"{item_link} error writing to Wikibase {e} \n")


    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n")

