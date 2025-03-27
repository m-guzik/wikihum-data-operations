"""
Values of the property "stated as" should not be repeated (unless with different qualifiers and references). This script 
is searching for such duplicated values of property "stated as" and removes them. 
"""

import os
import time
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.models import Claim
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

P_STATED_AS = 'P54'



def check_two_property_values_equality(claim_1: Claim, claim_2: Claim) -> tuple[bool, str]:
    """
    Checks if two claim values have the same text value, language, qualifiers and references.
    Works for properties with datatype "monolingual text".

    Parameters
    ----------
    claim_1 : Claim
        First Claim
    claim_2 : Claim
        Second Claim

    Returns
    -------
    tuple[bool, str]
        True if the claims are equal, False if ther are not and a message with inequality reason
    """
    claim_1_value = claim_1.mainsnak.datavalue["value"]["text"]
    claim_1_language = claim_1.mainsnak.datavalue["value"]["language"]

    claim_2_value = claim_2.mainsnak.datavalue["value"]["text"]
    claim_2_language = claim_2.mainsnak.datavalue["value"]["language"]

    if claim_1_value != claim_2_value or claim_1_language != claim_2_language:
        return False, " - value or language not eual"

    claim_1_qualifiers_order = claim_1.qualifiers_order
    claim_2_qualifiers_order = claim_2.qualifiers_order

    if claim_1_qualifiers_order != claim_2_qualifiers_order:
        return False, " - qualifiers order not equal"

    for qualifier_p in claim_1_qualifiers_order:
        claim_1_qualifier = claim_1.qualifiers.get(qualifier_p)
        claim_1_qualifier_datavalue = claim_1_qualifier[0].datavalue

        claim_2_qualifier = claim_2.qualifiers.get(qualifier_p)
        claim_2_qualifier_datavalue = claim_2_qualifier[0].datavalue

        if claim_1_qualifier_datavalue != claim_2_qualifier_datavalue:
            return False, " - qualifiers not equal"
        
    claim_1_references = claim_1.references.references
    claim_2_references = claim_2.references.references

    if len(claim_1_references) != len(claim_2_references):
        return False, " - number of references not equal"

    if len(claim_1_references) > 1:
        print("That's Too Much, Man!")
        return False, " - too many references"

    claim_1_reference_order = claim_1_references[0].snaks_order
    claim_1_reference = claim_1_references[0].snaks

    claim_2_reference_order = claim_2_references[0].snaks_order
    claim_2_reference = claim_2_references[0].snaks

    if claim_1_reference_order != claim_2_reference_order:
        return False, " - references order not equal"

    for reference_p in claim_1_reference_order:
        reference_1_value = claim_1_reference.get(reference_p)[0].datavalue 
        reference_2_value = claim_2_reference.get(reference_p)[0].datavalue 
        if reference_1_value != reference_2_value: 
            return False, " - references not equal"

    return True, ""




if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/04.3_remove_stated_as_duplicates.txt'


    query = """ SELECT DISTINCT ?item ?value WHERE {
                    ?item p:P54 ?statement1 .
                    ?item p:P54 ?statement2 .
                    
                    ?statement1 (ps:P54) ?value .
                    ?statement2 (ps:P54) ?value .

                    FILTER (?statement1 != ?statement2)
                }
                ORDER BY ?item
            """

    results = execute_sparql_query(query)
    outputItems = []
    outputValues = []
    for result in results["results"]["bindings"]:
        outputItems.append(result["item"]["value"])
        outputValues.append(result["value"]["value"])
    
    for i in range(0, len(outputItems)):
        entity_link = outputItems[i]
        stated_as_value = outputValues[i]

        position = entity_link.rfind(r'/')
        entity_id = entity_link[position+1:]

        entity = wbi.item.get(entity_id=entity_id)

        print(entity_link)
        report_message = f"{entity_link}"

        claims_stated_as = []
        for claim in entity.claims:
            if claim.mainsnak.property_number == P_STATED_AS and claim.mainsnak.datavalue["value"]["text"] == stated_as_value:
                claims_stated_as.append(claim)
                
        if len(claims_stated_as) > 2:
            print("That's too much, man!")
            report_message += f" - too many claims in the property 'stated as' {len(claims_stated_as)}"
            with open(report_path, 'a', encoding='utf-8') as report:
                 report.write(report_message + "\n")
            continue

        are_claims_equal, check_message = check_two_property_values_equality(claims_stated_as[0], claims_stated_as[1])

        if are_claims_equal:
            claim_to_remove = claims_stated_as[1]
            claim_to_remove_value = claim_to_remove.mainsnak.datavalue["value"]["text"]
            if WRITE:
                try:
                    claim_to_remove.remove()
                    entity.write()
                    report_message += f" {claim_to_remove_value} claim removed"
                    print(claim_to_remove_value, " claim removed")
                except (MWApiError, ModificationFailed) as e:
                    print(" - error writing to Wikibase")
                    report_message += f" - error writing to Wikibase {e}"
            else:
                report_message += f" {claim_to_remove_value} claim prepared to be removed"
        else:
            report_message += check_message

        with open(report_path, 'a', encoding='utf-8') as report:
            report.write(report_message + "\n")


    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n \n \n")

