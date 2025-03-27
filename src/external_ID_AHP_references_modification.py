"""
This script modifies the references to the aHP ID external identifier. So far the references consisted of properities 'stated as', 
'reference URL', 'filename' and 'retrieved' but the property 'stated in' with value 'Data Atlas Fontium' should be removed.
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


P_AHP_ID = 'P81'
P_REFERENCE_URL = 'P2'
P_FILENAME = 'P122'
P_RETRIEVED = 'P48'



if __name__ == '__main__':
    
    start_time = time.time()

    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                        consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                        access_token=WIKIDARIAH_ACCESS_TOKEN,
                                        access_secret=WIKIDARIAH_ACCESS_SECRET)
    wbi = WikibaseIntegrator(login=login_instance)

    report_path = 'data_2025/reports/05.1_external_ID_AHP_references_modification.txt'


    query = """ SELECT ?item ?statedIn WHERE {
                    ?item p:P81 ?statement .
                    ?statement prov:wasDerivedFrom ?reference .
                    ?reference pr:P55 ?statedIn
                } ORDER BY ?item
            """

    results = execute_sparql_query(query)
    output = []
    for result in results["results"]["bindings"]:
        output.append(result["item"]["value"])

    for i in range(0, len(output)):
        entity_link = output[i]
        position = entity_link.rfind(r'/')
        entity_id = entity_link[position+1:]
        entity = wbi.item.get(entity_id=entity_id)

        print(entity_link)
        report_message = f"{entity_link}"

        ahp_id_claim = entity.claims.get(P_AHP_ID)[0]
        reference = ahp_id_claim.references.references[0]

        snaks_references = snaks.Snaks()
        snaks_references.add(reference.snaks.get(P_REFERENCE_URL)[0])
        snaks_references.add(reference.snaks.get(P_FILENAME)[0])
        snaks_references.add(reference.snaks.get(P_RETRIEVED)[0])

        references_order = [P_REFERENCE_URL, P_FILENAME, P_RETRIEVED]
        reference_to_add = references.Reference(snaks=snaks_references, snaks_order=references_order)

        ahp_id_claim.references.clear()
        ahp_id_claim.references.add(reference_to_add)

        if WRITE:
            try:
                entity.write()
                report_message += " - stated in removed from reference"
                print("stated in removed from reference")
            except (MWApiError, ModificationFailed) as e:
                print(" - error writing to Wikibase")
                report_message += f" - error writing to Wikibase {e}"

        with open(report_path, 'a', encoding='utf-8') as report:
            report.write(report_message + "\n")



    end_time = time.time()
    execution_time = end_time - start_time

    with open(report_path, 'a', encoding='utf-8') as report:
        report.write(f"Execution time: {time.strftime("%H:%M:%S", time.gmtime(execution_time))} s.\n \n \n")


