"""
This script searches for missing english and polish labels and values and saves the data about incomplete elements in .xlsx file.
"""

import os
import time
from dotenv import load_dotenv
from openpyxl import load_workbook
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_exceptions import MWApiError, MissingEntityException


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

    file_path = 'data_2025/Missing_data.xlsx'
    file_workbook = load_workbook(file_path)
    work_sheet = file_workbook['Q_without_label_or_desctiption']

    i = 1
    row_number = 1
    while i < 182020:
        entity_id = "Q" + str(i)
        print(entity_id)
        try: 
            wd_item = wbi.item.get(entity_id=entity_id)
            label_pl = wd_item.labels.get('pl')
            label_en = wd_item.labels.get('en')
            desc_pl = wd_item.descriptions.get('pl')
            desc_en = wd_item.descriptions.get('en')
            if not label_pl or not label_en or not desc_pl or not desc_en:
                link = "https://wikihum.lab.dariah.pl/wiki/Item:" + str(entity_id)
                row = [link, str(entity_id), str(label_pl), str(label_en), str(desc_pl), str(desc_en)]
                work_sheet.append(row)

        except (KeyError, MissingEntityException) as wb_error:
            print(f'ERROR: {wb_error}')
        i += 1

    file_workbook.save('data_2025/Missing_data.xlsx')


    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Czas wykonania programu: {time.strftime("%H:%M:%S", time.gmtime(elapsed_time))} s.')
    

