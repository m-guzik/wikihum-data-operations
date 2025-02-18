"""
This scipt moves data from one element to another. Those elements are connected based on the PRNG identifier. Moved data from 
the first element is then deleted.  
"""

import csv 
import os
import time
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.datatypes import MonolingualText, ExternalID
from wikibaseintegrator.models import claims, qualifiers, references, snaks
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists
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



def add_prng(wh_item: entities.item.ItemEntity, prng: str):
    """
    Adds given PRNG id to the given element with appropriate qualifiers and references

    Parameters
    ----------
    wh_item : entities.item.ItemEntity
        item entity to which the property is to be added
    prng : str
        PRNG identifier 
    """

    qualifiers_to_add = qualifiers.Qualifiers()
    qualifier_snaktype = 'value'
    qualifier_p_number = 'P40'
    qualifier_value = {'value': {'time': '+2022-00-00T00:00:00Z', 'timezone': 0, 'before': 0, 'after': 0, 'precision': 9, 'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'}
    qualifier_datatype = 'time'

    new_qualifier = snaks.Snak(snaktype=qualifier_snaktype, property_number=qualifier_p_number, datavalue=qualifier_value, datatype=qualifier_datatype)
    qualifiers_to_add.add(new_qualifier)

    references_to_add = []
    snaks_references = snaks.Snaks()

    r_url_snaktype = 'value'
    r_url_p_number = 'P2'
    r_url_value = {'value': 'https://mapy.geoportal.gov.pl/wss/service/PZGiK/PRNG/WFS/GeographicalNames', 'type': 'string'}
    r_url_datatype = 'url'

    url_reference = snaks.Snak(snaktype=r_url_snaktype, property_number=r_url_p_number, datavalue=r_url_value, datatype=r_url_datatype)
    snaks_references.add(url_reference)

    r_date_snaktype = 'value'
    r_date_p_number = 'P48'
    r_date_value = {'value': {'time': '+2022-09-23T00:00:00Z', 'timezone': 0, 'before': 0, 'after': 0, 'precision': 11, 'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'}
    r_date_datatype = 'time'

    date_reference = snaks.Snak(snaktype=r_date_snaktype, property_number=r_date_p_number, datavalue=r_date_value, datatype=r_date_datatype)
    snaks_references.add(date_reference)

    references_order = ['P2', 'P48']
    reference_to_add = references.Reference(snaks=snaks_references, snaks_order=references_order)
    references_to_add.append(reference_to_add)


    claim_to_add = claims.Claim(qualifiers=qualifiers_to_add, rank='normal', references=references_to_add, snaktype='value')
    claim_to_add.mainsnak.property_number = 'P76'
    claim_to_add.mainsnak.datavalue = {'value': prng, 'type': 'string'}
    claim_to_add.mainsnak.datatype = 'external-id'
    claim_to_add.type = 'statement'

    data = [ExternalID(value=prng, prop_nr='P67')]
    wh_item.claims.add(claims=claim_to_add, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)



def prepare_new_claim_data_from_claim(claim: claims.Claim) -> claims.Claim:
    """
    Takes data from the given claim and prepares a new claim based on it. 

    Parameters
    ----------
    claim : claims.Claim
        claim from which the data should be copied

    Returns
    -------
    claims.Claim
        new claim which can be added to any item
    """
    qualifiers_to_add = qualifiers.Qualifiers()
    references_to_add = []

    for qualifier in claim.qualifiers:
        qualifier_snaktype = qualifier.snaktype
        qualifier_p_number = qualifier.property_number
        qualifier_value = qualifier.datavalue
        qualifier_datatype = qualifier.datatype

        new_qualifier = snaks.Snak(snaktype=qualifier_snaktype, property_number=qualifier_p_number, datavalue=qualifier_value, datatype=qualifier_datatype)
        qualifiers_to_add.add(new_qualifier)

    for claim_references in claim.references:
        snaks_references = snaks.Snaks()
        for reference in claim_references:
            reference_snaktype = reference.snaktype
            reference_p_number = reference.property_number
            reference_value = reference.datavalue
            reference_datatype = reference.datatype

            new_reference = snaks.Snak(snaktype=reference_snaktype, property_number=reference_p_number, datavalue=reference_value, datatype=reference_datatype)
            snaks_references.add(new_reference)

        references_order = claim_references.snaks_order
        reference_to_add = references.Reference(snaks=snaks_references, snaks_order=references_order)
        references_to_add.append(reference_to_add)

    data_snaktype = claim.mainsnak.snaktype
    data_p_number = claim.mainsnak.property_number
    data_value = claim.mainsnak.datavalue
    data_datatype = claim.mainsnak.datatype
    data_type = claim.type

    claim_to_add = claims.Claim(qualifiers=qualifiers_to_add, rank='normal', references=references_to_add, snaktype=data_snaktype)
    claim_to_add.mainsnak.property_number = data_p_number
    claim_to_add.mainsnak.datavalue = data_value
    claim_to_add.mainsnak.datatype = data_datatype
    claim_to_add.type = data_type

    return claim_to_add



if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/01.1_ahp_prng_data_transfer.txt'

    # data_file = open('data_2025/01.1_AHP_PRNG_data_examples.csv')
    data_file = open('data_2025/01.1_AHP_PRNG_data.csv')
    file_reader = csv.reader(data_file)
    file_header = []
    file_header = next(file_reader)


    for row in file_reader:
        item_link = row[0]
        item_label = row[1]
        item_prng = row[2]
        
        position = item_link.rfind(r'/')
        item_id = item_link[position+1:]

        prng_query = '"' + str(item_prng) + '"'
        query = """
            SELECT DISTINCT ?item WHERE {
                ?item p:P76 ?statement0.
                ?statement0 (ps:P76) """ + prng_query + """.
            }
            """
        
        results = execute_sparql_query(query)
        output = []
        for result in results["results"]["bindings"]:
            output.append(result["item"]["value"])


        item = wbi.item.get(entity_id=item_id)

        if len(output) == 0:
            prng_to_add = str(item_prng)
            add_prng(wh_item=item, prng=prng_to_add)
            print("PRNG prepared")
            if WRITE:
                item.write()
                print(item_link+ " PRNG added to the element.")
                with open(report_path, 'a', encoding='utf-8') as report:
                    report.write(f"{item_link} PRNG added to the element.\n")

        elif len(output) == 1:
            item_to_update_link = output[0]
            position = item_to_update_link.rfind(r'/')
            item_to_update_id = item_to_update_link[position+1:]
            item_to_update = wbi.item.get(entity_id=item_to_update_id)
            
            for claim in item.claims:
                if claim.mainsnak.property_number == 'P54':
                    alias_language = claim.mainsnak.datavalue['value']['language']
                    alias_value = claim.mainsnak.datavalue['value']['text']
                    item_to_update.aliases.set(language=alias_language, values=alias_value)

                if claim.mainsnak.property_number not in ('P27', 'P108'):
                    claim_to_add = prepare_new_claim_data_from_claim(claim)
                    item_to_update.add_claims(claims=claim_to_add, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
                    claim.remove()

            if WRITE:
                item.write()
                item_to_update.write()
                print(item_link + " data moved to the element " + item_to_update_link)
                with open(report_path, 'a', encoding='utf-8') as report:
                    report.write(f"{item_link} data moved to the element {item_to_update_link}.\n")

        else:
            print("PRNG " + item_prng + " exists in more than one element.")
            with open(report_path, 'a', encoding='utf-8') as report:
                report.write(f"PRNG {item_prng} exists in more than one element.\n")

    data_file.close()

    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n")

