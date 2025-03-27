"""
References to data from Data Atlas Fontium should consist of properties 'reference URL', 'filename' and 'retrieved'. In order to 
standardize those references property 'stated as' with value 'Data Atlas Fontium' should be removed from all statements.
"""

import os
import time
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.models import Claim, references, snaks
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


P_STATED_IN = 'P55'
P_REFERENCE_URL = 'P2'
P_FILENAME = 'P122'
P_RETRIEVED = 'P48'

DATA_ATLAS_FONTIUM = {'value': {'entity-type': 'item', 'numeric-id': 179149, 'id': 'Q179149'}, 'type': 'wikibase-entityid'}



if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/06.1_data_atlas_fontium_references_modification.txt'


    # 188209 results before the changes
    query = """ SELECT ?item ?statement WHERE {
                    ?item ?property ?statement .
                    ?statement prov:wasDerivedFrom ?reference .
                    ?reference pr:P55 wd:Q179149 .
                } ORDER BY ?item
            """

    results = execute_sparql_query(query)
    outputElements = []
    outputClaims = []
    for result in results["results"]["bindings"]:
        outputElements.append(result["item"]["value"])
        outputClaims.append(result["statement"]["value"])

    for i in range(0, len(outputElements)):
        entity_link = outputElements[i]
        position_e = entity_link.rfind(r'/')
        entity_id = entity_link[position_e+1:]
        entity = wbi.item.get(entity_id=entity_id)

        claim_to_change_link = outputClaims[i]
        position_c = claim_to_change_link.rfind(r'/')
        claim_to_change_id = claim_to_change_link[position_c+1:]
        claim_to_change_id = claim_to_change_id.replace('-', '$', 1)

        print(entity_link)
        report_message = f"{entity_link}"

        for p, statement in entity.claims.claims.items():
            for claim in statement:
                if claim.id != claim_to_change_id:
                    continue
                for reference in claim.references:
                    if P_STATED_IN in reference.snaks_order and reference.snaks.get(P_STATED_IN)[0].datavalue == DATA_ATLAS_FONTIUM:
                        snaks_references = snaks.Snaks()

                        snak_reference_url = snaks.Snak(snaktype='value', property_number='P2', datavalue={'value': 'https://data.atlasfontium.pl/documents/202', 'type': 'string'}, datatype='url')
                        snak_filename  = snaks.Snak(snaktype='value', property_number='P122', datavalue={'value': 'tabela-zbiorcza-miejscowosci-atlas-historyczny-polski-xvi-w', 'type': 'string'}, datatype='string')
                        snak_retrieved = snaks.Snak(snaktype='value', property_number='P48', datavalue={'value': {'time': '+2023-12-07T00:00:00Z', 'timezone': 0, 'before': 0, 'after': 0, 'precision': 11, 'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'}, datatype='time')
                            
                        snaks_references.add(snak_reference_url)
                        snaks_references.add(snak_filename)
                        snaks_references.add(snak_retrieved)
          
                        references_order = [P_REFERENCE_URL, P_FILENAME, P_RETRIEVED]
                        reference_to_add = references.Reference(snaks=snaks_references, snaks_order=references_order)

                        claim.references.remove(reference)
                        claim.references.add(reference_to_add)

                        if WRITE:
                            try:
                                entity.write()
                                report_message += " - stated in Data Atlas Fontium removed from reference"
                            except (MWApiError, ModificationFailed) as e:
                                print(" - error writing to Wikibase")
                                report_message += f" - error writing to Wikibase {e}"

                        with open(report_path, 'a', encoding='utf-8') as report:
                            report.write(report_message + "\n")



    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n\n\n\n\n")


