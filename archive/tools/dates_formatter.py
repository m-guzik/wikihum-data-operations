from typing import Tuple
from wikibaseintegrator.wbi_enums import WikibaseTimePrecision



def get_numeric_value_precision_and_qualifier(date: str) -> Tuple[str, str, str]:
    """
    Retrieves information about exact date, its precision and optional qualifier

    Parameters
    ----------
    date : str
        date with optional additional information (about, before, etc.)

    Returns
    -------
    Tuple[str, str, str]
        date without additional information, date precision (century, year, date)
        and optional qualifier (applicable WikiHum property name) 
    """
    roman_numbers = { 'X' :'10', 'XI': '11', 'XII': '10', 'XIII': '13', 'XIV': '14', 
                    'XV': '15', 'XVI': '16', 'XVII': '17', 'XVIII': '18', 'XIX': '19' }
    numeric_value = ''
    precision = ''
    qualifier = ''
    
    if len(date.split()) > 1:
        if 'w.' in date:
            roman_value = date[:-3]
            numeric_value = roman_numbers[roman_value]
            precision = WikibaseTimePrecision.CENTURY
        elif 'ok.' in date:
            numeric_value = date[4:]
            precision = WikibaseTimePrecision.YEAR
            qualifier = 'P189'
        elif 'po' in date:
            numeric_value = date[3:]
            precision = WikibaseTimePrecision.YEAR
            qualifier = 'P38'
        elif 'przed' in date:
            numeric_value = date[6:]
            precision = WikibaseTimePrecision.YEAR
            qualifier = 'P39'
        else:
            print("Date was not recognized.")
    else:
        if len(date) == 4:
            numeric_value = date
            precision = WikibaseTimePrecision.YEAR
        elif len(date) == 10:
            day = date[0:2]
            month = date[3:5]
            year = date[6:]
            numeric_value = year + '-' + month + '-' + day
            precision = WikibaseTimePrecision.DAY
        else:
            print("Date was not recognized.")
    return numeric_value, precision, qualifier


def get_wbi_time(numeric_value: str) -> str:
    """
    Converts simple date into Wikibase date/time format
    Heavily inspired by the function 'format_date' from 
    https://github.com/pjaskulski/wikihub_skrypty/blob/main/src/wikidariahtools.py

    Parameters
    ----------
    numeric_value : str
        date (century, year, day-month-year)

    Returns
    -------
    str
        date in Wikibase format '+yyyy-mm-ddT00:00:00Z' (months and days cannot be set to '00')
    """
    wbi_time = ''
    if len(numeric_value) == 4:
        wbi_time = f"+{numeric_value}-01-01T00:00:00Z"
    elif len(numeric_value) == 10:
        wbi_time = f"+{numeric_value}T00:00:00Z"
    elif len(numeric_value) == 2:
        wbi_time = f"+{str(int(numeric_value)-1)}01-01-01T00:00:00Z"
    return wbi_time

