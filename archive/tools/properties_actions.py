from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, entities, wbi_login
from wikibaseintegrator.models.snaks import Snak
from wikibaseintegrator.datatypes import Item, MonolingualText, String, Time, URL
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_config import config as wbi_config

import os

import tools.dates_formatter as dates_formatter
import tools.wb_actions as wb_actions


wbi_config['MEDIAWIKI_API_URL'] = 'https://prunus-208.man.poznan.pl/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://prunus-208.man.poznan.pl/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'https://prunus-208.man.poznan.pl'

load_dotenv()
BOT_NAME = os.environ.get('BOT_NAME')
BOT_PASSWORD = os.environ.get('BOT_PASSWORD')

login_instance = wbi_login.Login(user=BOT_NAME, password=BOT_PASSWORD)
wbi = WikibaseIntegrator(login=login_instance)


def add_human(wbi_item: entities.item.ItemEntity):
    """
    Adds property 'instance of' with value 'human' to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    """
    wbi_item.claims.add([Item(value='Q32', prop_nr='47')])
    print('Property "instance of" (human) was added')


def add_given_name(wbi_item: entities.item.ItemEntity, given_name: str):
    """
    Checks if given name exists in Wikibase as an item (instance of 'male given name') and 
    creates it if needed; then adds property 'given name' with given value to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    given_name : str
        name
    """
    given_name_id = wb_actions.search_for_item_with_property(given_name, 'P47', 'Q987')
    if not given_name_id:
        new_given_name_item = wb_actions.add_new_item(given_name, 'imię męskie', 'male given name')
        new_given_name_item.claims.add([Item(value='Q987', prop_nr='P47')])
        new_given_name_item.write()
        given_name_id = new_given_name_item.id        
    wbi_item.claims.add([Item(value=given_name_id, prop_nr='P184')], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
    print('Property "given name" was added')


def add_family_name(wbi_item: entities.item.ItemEntity, family_name: str):
    """
    Checks if given family name exists in Wikibase as an item (instance of 'family name') and 
    creates it if needed; then adds property 'family name' with given value to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    family_name : str
        family name
    """
    family_name_id = wb_actions.search_for_item_with_property(family_name, 'P47', 'Q34')
    if not family_name_id:
        new_family_name_item = wb_actions.add_new_item(family_name, 'nazwisko', 'family name')
        new_family_name_item.claims.add([Item(value='Q34', prop_nr='P47')])
        new_family_name_item.write()
        family_name_id = new_family_name_item.id        
    wbi_item.claims.add([Item(value=family_name_id, prop_nr='P183')])
    print('Property "family name" was added')
 

def add_location_as_string(wbi_item: entities.item.ItemEntity, location_name: str):
    """
    Adds property 'called (string)' (place where person came from) with given value to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    location_name : str
        name of the location
    """
    wbi_item.claims.add([String(value=location_name, prop_nr='P373')])
    print('Property "called (string)" was added')


def add_coat_of_arms(wbi_item: entities.item.ItemEntity, coat_of_arms_name: str):
    """
    Checks if given coat of arms name exists in Wikibase as an item (instance of 'Coat of arms') and 
    creates it if needed; then adds property 'coat of arms' with given value to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    coat_of_arms_name : str
        name of the coat of arms
    """
    coat_of_arms_id = wb_actions.search_for_item_with_property(coat_of_arms_name, 'P47', 'Q53')
    if not coat_of_arms_id:
        new_coat_of_arms_item = wb_actions.add_new_item(coat_of_arms_name, 'herb szlachecki', 'coat of arms')
        new_coat_of_arms_item.claims.add([Item(value='Q53', prop_nr='P47')])
        new_coat_of_arms_item.write()
        coat_of_arms_id = new_coat_of_arms_item.id
    wbi_item.claims.add([Item(value=coat_of_arms_id, prop_nr='P27')])
    print('Property "coat of arms" was added')
    
    
# TODO add qualifiers 
def add_date_of_birth(wbi_item: entities.item.ItemEntity, point_in_time: str):
    """
    Retrieves information about exact date, precision and qualifier from given point in time and adds 
    it as property 'date of birth' to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    point_in_time : str
        date of birth with optional additional information 
    """
    value, precision, qualifier = dates_formatter.get_numeric_value_precision_and_qualifier(point_in_time)
    wbi_time = dates_formatter.get_wbi_time(value)
    if not qualifier:
        wbi_item.claims.add([Time(time=wbi_time, precision=precision, prop_nr='P7')])
        
    print('Property "date of birth" was added')
    
    
def add_date_of_death(wbi_item: entities.item.ItemEntity, point_in_time: str):
    """
    Retrieves information about exact date, precision and qualifier from given point in time and adds 
    it as property 'date of death' to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    point_in_time : str
        date of death with optional additional information 
    """
    value, precision, qualifier = dates_formatter.get_numeric_value_precision_and_qualifier(point_in_time)
    wbi_time = dates_formatter.get_wbi_time(value)
    if not qualifier:
        wbi_item.claims.add([Time(time=wbi_time, precision=precision, prop_nr='P8')])
    elif qualifier == 'P189':        
        data_value = { 'value' : {'entity-type': 'item', 'numeric-id': '37979', 'id': 'Q37979'}, 
                    'type': 'wikibase-entityid' }
        qualifier_item = [ Snak(snaktype='value', property_number=qualifier, datavalue=data_value, datatype='wikibase-item') ]
        wbi_item.claims.add([Time(time=wbi_time, precision=precision, prop_nr='P8', qualifiers=qualifier_item)])           
    elif qualifier == 'P38' or qualifier == 'P39':
        data_value = { 'value': {'time': wbi_time, 'timezone': 0, 'before': 0, 'after': 0, 'precision': 9, 
                    'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'} 
        qualifier_item = [ Snak(snaktype='value', property_number=qualifier, datavalue=data_value, datatype='time') ]
        wbi_item.claims.add([Time(time=None, precision=precision, prop_nr='P8', snaktype='somevalue', qualifiers=qualifier_item)])
    print('Property "date of death" was added')
    
    
def add_floruit(wbi_item: entities.item.ItemEntity, point_in_time: str):
    """
    Retrieves information about exact date, precision and qualifier from given point in time and adds 
    it as property 'floruit' to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    point_in_time : str
        date with optional additional information
    """
    value, precision, qualifier = dates_formatter.get_numeric_value_precision_and_qualifier(point_in_time)
    wbi_time = dates_formatter.get_wbi_time(value)
    wbi_item.claims.add([Time(time=wbi_time, precision=precision, prop_nr='P54')])
    print('Property "floruit" was added')
    
    
def add_birth_place(wbi_item: entities.item.ItemEntity, place_name: str, prng_id: str):
    """
    Adds property 'place of birth' with given place to the item (based on the PRNG ID)

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    place_name : str
        name of the place of birth
    prng_id : str
        PRNG ID of the given place
    """
    place_id = wb_actions.search_for_item_with_property(place_name, 'P274', prng_id)
    if place_id:
        wbi_item.claims.add([Item(value=place_id, prop_nr='P55')])
        print('Property "place of birth" was added')
    else:
        print('Property "place of birth" was not added')
        

def add_stated_as(wbi_item: entities.item.ItemEntity, text: str, language: str):
    """
    Adds property 'stated as' with given text in given language to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    text : str
        text to be added as a property value
    language : str
        language of the given text
    """
    reference_URL = [[ URL(value='http://serwerone.nazwa.pl/urzednicy10/', prop_nr='P182') ]]
    wbi_item.claims.add([MonolingualText(text=text, language=language, prop_nr='P195', references=reference_URL)], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
    print('Property "stated as" was added')
    
    
def add_alias(wbi_item: entities.item.ItemEntity, text: str, language: str):
    """
    Adds 'alias' with given text in given language to the item

    Parameters
    ----------
    wbi_item : entities.item.ItemEntity
        item entity to which the property is to be added
    text : str
        text to be added as a alias
    language : str
        language of the given text
    """
    wbi_item.aliases.set(language=language, values=text)
    print('Alias was added')


def add_position_held(wbi_item: entities.item.ItemEntity, office: str, start_date: str, end_date: str, date: str):
    qualifier_items = []
    office_id = wb_actions.check_if_item_exists(office, '')
    if not office_id:
        new_office_item = wb_actions.add_new_item(office, 'urząd', 'position')
        new_office_item.claims.add([Item(value='Q65', prop_nr='47')])        
        new_office_item.write()
        office_id = new_office_item.id
    reference_book = Item(value='Q919', prop_nr='P192')
    reference_volume = String(value='2', prop_nr='P232')
    reference_notebook = String(value='2', prop_nr='P343')
    reference_URL = URL(value='http://serwerone.nazwa.pl/urzednicy10/', prop_nr='P182')
    references = [[ reference_book, reference_volume, reference_notebook ], [reference_URL]]
    
    if start_date is not None and start_date != '':
        start_time = dates_formatter.get_wbi_time(start_date) 
        data_value = { 'value': {'time': start_time, 'timezone': 0, 'before': 0, 'after': 0, 'precision': 9, 
                    'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'}      
        qualifier_start = Snak(snaktype='value', property_number='P203', datavalue=data_value, datatype='wikibase-item')
        qualifier_items.append(qualifier_start)
        
    if end_date is not None and end_date != '':
        end_time = dates_formatter.get_wbi_time(end_date) 
        data_value = { 'value': {'time': end_time, 'timezone': 0, 'before': 0, 'after': 0, 'precision': 9, 
                    'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'}      
        qualifier_end = Snak(snaktype='value', property_number='P204', datavalue=data_value, datatype='wikibase-item')
        qualifier_items.append(qualifier_end)
                     
    if date is not None and date != '':
        value, precision, qualifier = dates_formatter.get_numeric_value_precision_and_qualifier(date)
        wbi_time = dates_formatter.get_wbi_time(value)
        if not qualifier:
            if 'w.' in date:
                data_value = { 'value': {'time': wbi_time, 'timezone': 0, 'before': 0, 'after': 0, 'precision': 7, 
                  'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'} 
            else:
                data_value = { 'value': {'time': wbi_time, 'timezone': 0, 'before': 0, 'after': 0, 'precision': 9, 
                  'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'} 
            qualifier_date = Snak(snaktype='value', property_number='P252', datavalue=data_value, datatype='wikibase-item')
            qualifier_items.append(qualifier_date)
        elif qualifier == 'P38' or qualifier == 'P39':
            data_value = { 'value': {'time': wbi_time, 'timezone': 0, 'before': 0, 'after': 0, 'precision': 9, 
                        'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 'type': 'time'} 
            qualifier_date = Snak(snaktype='value', property_number=qualifier, datavalue=data_value, datatype='wikibase-item')
            qualifier_items.append(qualifier_date)
    
    if qualifier_items == []:
        wbi_item.claims.add([Item(value=office_id, prop_nr='P9', references=references)], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
    else:
        wbi_item.claims.add([Item(value=office_id, prop_nr='P9', qualifiers=qualifier_items, references=references)], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
    
    print('Property "position held" was added')
    
    
def add_described_by_source(wbi_item: entities.item.ItemEntity, source_title: str, pages: str):   
    # source_id = wb_actions.check_if_item_exists(source_title, '')

    print('Property "described_by_source" was added') 
    
    
