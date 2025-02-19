from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config

import os
import xml.etree.ElementTree as ET

import tools.properties_actions as properties
import tools.wb_actions as wb_actions
import tools.xml_parser as xml_parser


wbi_config['MEDIAWIKI_API_URL'] = 'https://prunus-208.man.poznan.pl/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://prunus-208.man.poznan.pl/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'https://prunus-208.man.poznan.pl'

load_dotenv()
BOT_NAME = os.environ.get('BOT_NAME')
BOT_PASSWORD = os.environ.get('BOT_PASSWORD')

login_instance = wbi_login.Login(user=BOT_NAME, password=BOT_PASSWORD)
wbi = WikibaseIntegrator(login=login_instance)

data_file_path = 'archive/data/persons.xml'
# data_file_path = 'archive/data/test.xml'
data_file = ET.parse(data_file_path)

for person in data_file.getroot():
    label, description = xml_parser.get_label_and_description(person)
    print('\n------------------------------------------------------------------------')
    print('Label', label, '\nDescription', description)
    
    added_item = wb_actions.add_new_item(label, description, description)
    item_id = added_item.id
    
    properties.add_human(added_item)
               
    name = person.find('name')
    if name is not None:
        for single_name in name.text.split():
            properties.add_given_name(added_item, single_name)
    
    surname = person.find('surname')
    if surname is not None:
        properties.add_family_name(added_item, surname.text)
  
    location = person.find('location')
    if location is not None:
        properties.add_location_as_string(added_item, location.text)
         
    coat_of_arms = person.find('coat_of_arms')
    if coat_of_arms is not None and 'nieznany' not in coat_of_arms.text:    
        properties.add_coat_of_arms(added_item, coat_of_arms.text)
                
    date_of_birth = person.find('date_of_birth') 
    if date_of_birth is not None:
        properties.add_date_of_birth(added_item, date_of_birth.text)       
        
    date_of_death = person.find('date_of_death')
    if date_of_death is not None:
        properties.add_date_of_death(added_item, date_of_death.text) 
        
    floruit = person.find('floruit')
    if floruit is not None:
        properties.add_floruit(added_item, floruit.text)  
    
    place_of_birth = person.find('place_of_birth')
    if place_of_birth is not None:
        place_of_birth_name = place_of_birth.find('place').text
        place_of_birth_prng = place_of_birth.find('prng').text
        properties.add_birth_place(added_item, place_of_birth_name, place_of_birth_prng)

    stated_as_list = person.findall('stated_as')
    for stated_as in stated_as_list:
        language = stated_as.attrib['lang']
        properties.add_stated_as(added_item, stated_as.text, language)
        properties.add_alias(added_item, stated_as.text, language)
    
    # TODO urzędy dodają się ponownie chociaż już istnieją
    positions_list = person.find('positions')
    for position in positions_list:
        office, start_date, end_date, date = xml_parser.get_office_details(position)
        properties.add_position_held(added_item, office, start_date, end_date, date)
    
    bibliography = person.find('bibliography')   
    if bibliography is not None:
        title, pages = xml_parser.get_source_title_and_pages(bibliography)
        properties.add_described_by_source(added_item, title, pages)
    
    # added_item.write()
    
    