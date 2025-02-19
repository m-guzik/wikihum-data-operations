from typing import Tuple
import xml.etree.ElementTree as ET


def get_label_and_description(person: ET.Element) -> Tuple[str, str]:
    """
    Constructs label (name, surname, location) and description (years of life, offices held) 

    Parameters
    ----------
    person : ET.Element
        object from xml with all data about one person 
    Returns
    -------
    Tuple[str, str, str]
        complete label and description
    """
    name = person.find('name').text + " " if person.find('name') is not None else ""
    surname = person.find('surname').text + " " if person.find('surname') is not None else ""
    location = person.find('location').text if person.find('location') is not None else ""
            
    label = name + surname + location
    
    birth_date = person.find('date_of_birth').text if person.find('date_of_birth') is not None else ""
    death_date = person.find('date_of_death').text if person.find('date_of_death') is not None else ""
    floruit = person.find('floruit').text if person.find('floruit') is not None else ""
    
    if "-" in birth_date:
        birth_date = birth_date[-4:]
        
    if "-" in death_date:
        death_date = death_date[-4:]
    
    if birth_date != "" and death_date != "":
        description = "(" + birth_date + "-" + death_date + ") "
    elif birth_date != "":
        description = "(ur. " + birth_date + ") "
    elif death_date != "":
        description = "(zm. " + death_date + ") "
    elif floruit != "":
        description = "(" + floruit + ") "
    else:
        description = ""
    
    offices_elements = person.findall('./positions/position/office')
    offices_list = [x.text for x in offices_elements]
    offices_string = ', '.join(offices_list)
    description = description + offices_string
    
    label = label.strip()
    description = description.strip()

    return label, description


def get_office_details(position: ET.Element) -> Tuple[str, str, str, str]:
    """
    Checks which details about a position are given and returns texts from them 

    Parameters
    ----------
    position : ET.Element
        object from xml with all data about one position 
    Returns
    -------
    Tuple[str, str, str, str]
        texts from existing fields (office, start date, end date, date) or empty strings 
    """
    office = position.find('office')
    start_date = position.find('start_date')
    end_date = position.find('end_date')
    date = position.find('date')
    
    office_text = office.text
    start_date_text = ''
    end_date_text = ''
    date_text = ''
        
    if start_date is not None:
        start_date_text = start_date.text
        
    if end_date is not None:
        end_date_text = end_date.text
        
    if date is not None:
        date_text = date.text
    
    return office_text, start_date_text, end_date_text, date_text


def get_source_title_and_pages(source: ET.Element) -> Tuple[str, str]:
    """
    Retrieves data about the source title and related pages from given xml element 

    Parameters
    ----------
    source : ET.Element
        object from xml with bibliography item data

    Returns
    -------
    Tuple[str, str]
        source title (perhaps with volume) and specific pages 
    """
    title = ''
    pages = ''
    bibliography = source.find('biblio')
    title = bibliography.text.split(', s.')[0]
    pages = bibliography.text.split('s. ')[1]
    return title, pages

