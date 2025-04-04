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

def ranking_scrape(url):

    # Fetch the url content
    response = requests.get(url)

    # Parse the url content with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html5lib')
    def remove_accents(text):
        return unidecode(text) if isinstance(text, str) else text

    # Iterate through all text elements in the HTML and replace accents
    for element in soup.find_all(string=True):
        element.replace_with(remove_accents(element))

    # Find the h2 with id="Histórico"
    h2_header = soup.find('h2', {'id': 'Classificação_geral'})
    desired_table = None
    next_div = None

    if h2_header:
        parent_div = h2_header.find_parent('div')
        next_div = parent_div.find_next_sibling()

    if next_div:
        if next_div.name == "table":
            desired_table = next_div
        else:
            desired_table = next_div.find("table", recursive=False)
    
    if desired_table:

        #Parsing html table to DataFrame
        html_to_table = pd.read_html(StringIO(str(desired_table)))
        Ranking = html_to_table[0]

        # Normalising column name
        if 'Porcentagem/ Pontos' in Ranking.columns: 
            Ranking = Ranking.rename(columns={'Porcentagem/ Pontos':'% dos votos'})
        if 'Porcentagem/ Votos' in Ranking.columns: 
            Ranking = Ranking.rename(columns={'Porcentagem/ Votos':'% dos votos'})

        # Adding the year of the current file
        Ranking['Edicao'] = url.rsplit('_', 1)[-1]

        # Ensuring the Pos. column is always a string
        Ranking['Pos.'] = Ranking['Pos.'].astype(str)

        # Remove double ranking - take only first number
        Ranking['Pos.'] = Ranking['Pos.'].apply(lambda x: x.split('-')[0] if '-' in x else x)

        # Remove Notes number from % Votes and Meio de indicacao
        Ranking['% dos votos'] = Ranking['% dos votos'].str.replace(r'\[.*', '', regex=True)
        Ranking['Meio de indicacao'] = Ranking['Meio de indicacao'].str.replace(r'\[.*', '', regex=True)

        # Breakdown Meio de Indicacao into two columns: Meio and Nominated by
        Ranking['Indicado por'] = Ranking['Meio de indicacao'].str.extract(r'\((.*?)\)')
        Ranking['Indicado por'] = Ranking['Indicado por'].fillna(Ranking['Meio de indicacao'])
        Ranking['Meio de indicacao'] = Ranking['Meio de indicacao'].str.replace(r'\((.*?)\)', '', regex=True)

        # Some % Votes isn't %, it says "disqualified" or "withdrawn" in a merged cell.
        Ranking['% dos votos'] = Ranking.apply(lambda row: row['% dos votos'].replace(row['% dos votos'], row['Meio de indicacao']) if '%' not in row['% dos votos'] else row['% dos votos'], axis=1)
        
        # Replace -- in Eliminado em by Finalista
        Ranking['Eliminado em'] = Ranking.apply(lambda row: row['Eliminado em'].replace('--', row['Meio de indicacao']) if '--' in row['Eliminado em'] else row['Eliminado em'], axis=1)

    return Ranking

# Appending the Rankings to one single dataframe

base_url = "https://pt.wikipedia.org/wiki/Big_Brother_Brasil_"
number_of_shows = 25

urls = [f"{base_url}{i}" for i in range(1, number_of_shows + 1)]

Combined_ranking = []

for url in urls:
    try:
        ranking_new = ranking_scrape(url)
        Combined_ranking.append(ranking_new)
        print(f"Ranking information for {url} appended")
    except Exception as e:
        print(f"Error processing {url}: {e}")

# Save to csv
Ranking = pd.concat(Combined_ranking, ignore_index=True)
Ranking.to_csv(f'ranking')
