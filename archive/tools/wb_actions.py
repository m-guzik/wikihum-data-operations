from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_helpers, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config

import os


wbi_config['MEDIAWIKI_API_URL'] = 'https://prunus-208.man.poznan.pl/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://prunus-208.man.poznan.pl/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'https://prunus-208.man.poznan.pl'

load_dotenv()
BOT_NAME = os.environ.get('BOT_NAME')
BOT_PASSWORD = os.environ.get('BOT_PASSWORD')

login_instance = wbi_login.Login(user=BOT_NAME, password=BOT_PASSWORD)
wbi = WikibaseIntegrator(login=login_instance)


def check_if_item_exists(label: str, description: str) -> str: 
    """
    Checks if the item with given label and description exists in Wikibase

    Parameters
    ----------
    label : str
        label of the item in Polish
    description : str
        description of the item in Polish

    Returns
    -------
    str
        ID of the existing item or an empty string 
    """
    result = wbi_helpers.search_entities(search_string=label, language='pl')
    for existing_entity_id in result:
        wbi_item = wbi.item.get(entity_id=existing_entity_id)
        existing_entity_description = wbi_item.descriptions.get(language='pl')
        if (existing_entity_description == description) or (len(description) == 0):
            return existing_entity_id
    return ''


def check_if_item_exists_by_ID(id: str) -> entities.item.ItemEntity | str: 
    """ 
    Checks if the item with given ID exists in Wikibase

    Parameters
    ----------
    id : str
        ID of the item

    Returns
    -------
    str
        existing item entity or an empty string 
    """
    try: 
        item_entity = wbi.item.get(entity_id=id)
        return item_entity        
    except:
        return ''


def add_new_item(label_pl: str, description_pl: str, description_en: str) -> entities.item.ItemEntity: 
    """
    Checks if the item with given label and description (both in Polish) exists in Wikibase, if not then adds it

    Parameters
    ----------
    label : str
        label of the item in Polish
    description_pl : str
        description of the item in Polish
    description_en : str
        description of the item in English

    Returns
    -------
    entities.item.ItemEntity
        added item entity or existing item entity
    """

    potential_item_id = check_if_item_exists(label=label_pl, description=description_pl) 
    if not potential_item_id:
        wbi_new_item = wbi.item.new()
        wbi_new_item.labels.set(language='pl', value=label_pl)
        wbi_new_item.labels.set(language='en', value=label_pl)

        wbi_new_item.descriptions.set(language='pl', value=description_pl)
        wbi_new_item.descriptions.set(language='en', value=description_en)

        result = wbi_new_item.write()
        print('Item', label_pl, 'was added, ID =', result.id)
        return result
    else: 
        print('Item already exists in Wikibase with ID =', potential_item_id)
        potential_item = wbi.item.get(entity_id=potential_item_id)
        return potential_item


def search_for_item_with_property(label: str, prop_id: str, prop_value_id: str) -> str: 
    """
    Checks if the item with given label exists in Wikibase and if it has given property with given value

    Parameters
    ----------
    label : str
        label of the item in Polish
    prop_id : str
        ID of the property to check
    prop_value_id : str
        ID of the value of the property to check 

    Returns
    -------
    str
        ID of the existing item or an empty string
    """
    search_result = wbi_helpers.search_entities(search_string=label)
    if search_result != []:
        for item_id in search_result:
            item = wbi.item.get(entity_id=item_id)
            if label == item.labels.get('pl'):
                try:
                    item_property_value = item.claims.get(prop_id)[0].mainsnak.datavalue['value']
                    if 'entity-type' in item_property_value and item_property_value['entity-type'] == 'item':
                        item_property_value = item_property_value['id']
                    if item_property_value == prop_value_id:
                        return item_id
                except KeyError:
                    continue
    else:
        return ''

