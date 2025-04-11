# Libraries

import pandas as pd
import requests
from bs4 import BeautifulSoup
from IPython.display import display
from io import StringIO
import csv
import gzip
from unidecode import unidecode
import html5lib

def ratings_weekly_scrape(url):

    # Fetch the url content
    response = requests.get(url)

    # Parse the url content with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html5lib')
    def remove_accents(text):
         return unidecode(text) if isinstance(text, str) else text

    # Iterate through all text elements in the HTML and replace accents
    for element in soup.find_all(string=True):
         element.replace_with(remove_accents(element))

    # Find the h2
    h2_header = soup.find('h2', {'id': 'Audiência'})
    desired_table = None
    next_div = None

    if h2_header:
        parent_div = h2_header.find_parent('div')
        next_div = parent_div.find_next_sibling()

        while next_div:
         if next_div.name == "table":
            desired_table = next_div
            break
         else:
            next_div = next_div.find_next_sibling()
            if next_div.find('h2'):
                break 

    if desired_table:

        # Replacing commas for periods in decimals
        for element in desired_table.find_all(string=True):
            if ',' in element:
                updated_text = element.replace(',', '.')
                element.replace_with(updated_text)

        # Parsing html table to DataFrame
        html_to_table = pd.read_html(StringIO(str(desired_table)))
        Ratings_weekly = html_to_table[0]

        # Adding the year of the current file
        Ratings_weekly['Edicao'] = url.rsplit('_', 1)[-1]

        # Remove index column
        Ratings_weekly.reset_index(drop=True, inplace=True)

        # Remove Media de Edicao row (last row)
        Ratings_weekly = Ratings_weekly.iloc[:-1]

        # Remove notes number from SEG to DOM
        Ratings_weekly.loc[:, 'SEG'] = Ratings_weekly['SEG'].str.replace(r'\[.*', '', regex=True)
        Ratings_weekly.loc[:, 'TER'] = Ratings_weekly['TER'].str.replace(r'\[.*', '', regex=True)
        Ratings_weekly.loc[:, 'QUA'] = Ratings_weekly['QUA'].str.replace(r'\[.*', '', regex=True)
        Ratings_weekly.loc[:, 'QUI'] = Ratings_weekly['QUI'].str.replace(r'\[.*', '', regex=True)
        Ratings_weekly.loc[:, 'SEX'] = Ratings_weekly['SEX'].str.replace(r'\[.*', '', regex=True)
        Ratings_weekly.loc[:, 'SAB'] = Ratings_weekly['SAB'].str.replace(r'\[.*', '', regex=True)
        Ratings_weekly.loc[:, 'DOM'] = Ratings_weekly['DOM'].str.replace(r'\[.*', '', regex=True)

        # Using commas as decimals, changing it to points
        Ratings_weekly.loc[:, 'SEG'] = Ratings_weekly['SEG'].str.replace(',', '.', regex=True)
        Ratings_weekly.loc[:, 'TER'] = Ratings_weekly['TER'].str.replace(',', '.', regex=True)
        Ratings_weekly.loc[:, 'QUA'] = Ratings_weekly['QUA'].str.replace(',', '.', regex=True)
        Ratings_weekly.loc[:, 'QUI'] = Ratings_weekly['QUI'].str.replace(',', '.', regex=True)
        Ratings_weekly.loc[:, 'SEX'] = Ratings_weekly['SEX'].str.replace(',', '.', regex=True)
        Ratings_weekly.loc[:, 'SAB'] = Ratings_weekly['SAB'].str.replace(',', '.', regex=True)
        Ratings_weekly.loc[:, 'DOM'] = Ratings_weekly['DOM'].str.replace(',', '.', regex=True)
    
        # Add week number
        Ratings_weekly = Ratings_weekly.copy()
        Ratings_weekly['Semana'] = ['Semana {}'.format(i + 1) for i in range(len(Ratings_weekly))]

        # Replace any -- with null
        Ratings_weekly.loc[:, 'SEG'] = Ratings_weekly.apply(lambda row: row['SEG'].replace('--', 'NaN') if isinstance(row['SEG'], str) and '--' in row['SEG'] else row['SEG'],axis=1)
        Ratings_weekly.loc[:, 'TER'] = Ratings_weekly.apply(lambda row: row['TER'].replace('--', 'NaN') if isinstance(row['TER'], str) and '--' in row['TER'] else row['TER'],axis=1)
        Ratings_weekly.loc[:, 'QUA'] = Ratings_weekly.apply(lambda row: row['QUA'].replace('--', 'NaN') if isinstance(row['QUA'], str) and '--' in row['QUA'] else row['QUA'],axis=1)
        Ratings_weekly.loc[:, 'QUI'] = Ratings_weekly.apply(lambda row: row['QUI'].replace('--', 'NaN') if isinstance(row['QUI'], str) and '--' in row['QUI'] else row['QUI'],axis=1)
        Ratings_weekly.loc[:, 'SEX'] = Ratings_weekly.apply(lambda row: row['SEX'].replace('--', 'NaN') if isinstance(row['SEX'], str) and '--' in row['SEX'] else row['SEX'],axis=1)
        Ratings_weekly.loc[:, 'SAB'] = Ratings_weekly.apply(lambda row: row['SAB'].replace('--', 'NaN') if isinstance(row['SAB'], str) and '--' in row['SAB'] else row['SAB'],axis=1)
        Ratings_weekly.loc[:, 'DOM'] = Ratings_weekly.apply(lambda row: row['DOM'].replace('--', 'NaN') if isinstance(row['DOM'], str) and '--' in row['DOM'] else row['DOM'],axis=1)    

    else: print("Ratings table not available")
    
    # Save to csv
    year = url.rsplit('_', 1)[-1]

    Ratings_weekly.to_csv(f'ratingsweekly{year}')
    
    return print("CSV files successfully saved")

# List of URLs to process
base_url = "https://pt.wikipedia.org/wiki/Big_Brother_Brasil_"
number_of_shows = 25

urls = [f"{base_url}{i}" for i in range(1, number_of_shows + 1)]

for url in urls:
    try:
        ratings_weekly_scrape(url)
        print(f"Processed and saved CSV files for: {url}")
    except Exception as e:
        print(f"Error processing {url}: {e}")
    
