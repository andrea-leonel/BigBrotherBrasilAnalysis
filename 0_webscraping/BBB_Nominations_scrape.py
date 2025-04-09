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
import itertools

def nominations_scrape(url):

    # Fetch the url content
    response = requests.get(url)

    # Parse the url content with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html5lib')
    def remove_accents(text):
        return unidecode(text) if isinstance(text, str) else text

    # Iterate through all text elements in the HTML and replace accents
    for element in soup.find_all(string=True):
        element.replace_with(remove_accents(element))  # Replace text with de

    # Find the h2 with id="Histórico"
    h2_header = soup.find('h2', {'id': 'Histórico'})
    desired_table = None

    if h2_header:
        parent_div = h2_header.find_parent('div')
        next_div = parent_div.find_next_sibling()

    if next_div:
        if next_div.name == "table":
            desired_table = next_div
        else:
            desired_table = next_div.find("table", recursive=False)
    
    # Normalising headers
    if desired_table:

        #Parsing html table to DataFrame
        html_to_table = pd.read_html(StringIO(str(desired_table)))
        Nominations_raw = html_to_table[0]

        # Dynamically extract all column header levels into separate lists
        all_levels = [Nominations_raw.columns.get_level_values(level).tolist() for level in range(Nominations_raw.columns.nlevels)]

        # Merging these lists into one, creating column names that contain the information from the following levels, if any
        headers = []

        for items in zip(*all_levels): # Creates tuples for each column with the different levels.
            merged_items = []
            for i in range(len(items)): # For each item in each tuple
                if i == 0 or items[i] != items[i - 1]: # If it's the first item or if the item is different from the previous item.
                    merged_items.append(items[i]) # Include the item
                else:
                    merged_items.append("")  # Otherwise, add an empty string for duplicates
            headers.append(" - ".join(filter(None, merged_items)))  # Combine non-empty values

        #Set new headers
        headers = ["" if "Unnamed" in item else item for item in headers] # Removing the "Unnamed" levels in the header (column 0)
        Nominations_raw.columns = headers

    # Separate the 3 tables.

    # Replace empty cells with nulls
    Nominations_raw.replace(r'^\s*$', None, regex=True, inplace=True)
    Nominations_raw.replace(r'(nenhum)', None, regex=True, inplace=True)

    # Identify rows where all columns are null - the dividers
    divider_index = Nominations_raw[Nominations_raw.isnull().all(axis=1)].index

    # Split the DataFrame into three parts based on the dividing row indices and add the headers created above
    df_part1 = Nominations_raw.iloc[:divider_index[0]]  # From start to first blank row
    df_part1.columns = headers
    df_part2 = Nominations_raw.iloc[divider_index[0]+1:divider_index[1]]  # Between the blank rows
    df_part2.columns = headers
    df_part3 = Nominations_raw.iloc[divider_index[1]+1:]
    df_part3.columns = headers

    # Storing the individual tables into dataframes
    Nominations = pd.DataFrame(df_part1)
    Individual_nominations = pd.DataFrame(df_part2)
    Eviction_results = pd.DataFrame(df_part3)

    # Manipulating the Nominations table

    # Remove Na columns
    Nominations = Nominations.dropna(axis=1, how='all')

    # Drop any duplicate columns to the index
    Nominations = Nominations.loc[:, ~Nominations.T.duplicated()]

    #Transpose the table
    Nominations = Nominations.T
    Nominations.columns = Nominations.iloc[0]
    Nominations = Nominations[1:]

    # Adding the year of the current file
    Nominations['Edicao'] = url.rsplit('_', 1)[-1]

    # Reset the index to make 'Semana' a column
    Nominations = Nominations.reset_index()

    # Create a new column 'Dinamica' based on what's after the delimiter
    Nominations['Dinamica'] = Nominations['index'].apply(lambda x: x.split(' - ')[1] if ' - ' in x else None)

    # Update 'Semana' to keep only the part before the delimiter
    Nominations['index'] = Nominations['index'].apply(lambda x: x.split(' - ')[0] if ' - ' in x else x)
    Nominations.rename(columns={'index': 'Semana'}, inplace=True)

    # Removing spaces from column names
    Nominations.columns = [col.replace(' ', '_') for col in Nominations.columns]
    Nominations.columns = [col.replace('(', '') for col in Nominations.columns]
    Nominations.columns = [col.replace(')', '') for col in Nominations.columns]


    # Manipulating the Individual_nominations table

    # Remove Na columns
    Individual_nominations = Individual_nominations.dropna(axis=1, how='all')

    #Transpose the table
    Individual_nominations = Individual_nominations.T
    Individual_nominations.columns = Individual_nominations.iloc[0]
    Individual_nominations = Individual_nominations[1:]

    # Adding the year of the current file
    Individual_nominations['Edicao'] = url.rsplit('_', 1)[-1]

    # Reset the index to make 'Semana' a column
    Individual_nominations = Individual_nominations.reset_index()

    # Create a new column 'Dinamica' based on what's after the delimiter
    Individual_nominations['Dinamica'] = Individual_nominations['index'].apply(lambda x: x.split(' - ')[1] if ' - ' in x else None)

    # Update 'Semana' to keep only the part before the delimiter
    Individual_nominations['index'] = Individual_nominations['index'].apply(lambda x: x.split(' - ')[0] if ' - ' in x else x)
    Individual_nominations.rename(columns={'index': 'Semana'}, inplace=True)

    # Removing spaces from column names
    Individual_nominations.columns = [col.replace(' ', '_') for col in Individual_nominations.columns]
    Individual_nominations.columns = [col.replace('(', '') for col in Individual_nominations.columns]
    Individual_nominations.columns = [col.replace(')', '') for col in Individual_nominations.columns]

    # Manipulating the Eviction_results table

    # Remove Na columns
    Eviction_results = Eviction_results.dropna(axis=1, how='all')
    
    # Drop any duplicate columns to the index
    Eviction_results = Eviction_results.loc[:, ~Eviction_results.T.duplicated()]

    #Transpose the table
    Eviction_results_t = Eviction_results.T
    Eviction_results_t.columns = Eviction_results_t.iloc[0]
    Eviction_results = Eviction_results_t[1:]

    # Drop any duplicate columns to the index
    Eviction_results = Eviction_results.loc[:, ~Eviction_results.T.duplicated()]

    # Adding the year of the current file
    Eviction_results['Edicao'] = url.rsplit('_', 1)[-1]

    # Reset the index to make 'Semana' a column
    Eviction_results = Eviction_results.reset_index()

    # Create a new column 'Dinamica' based on what's after the delimiter
    Eviction_results['Dinamica'] = Eviction_results['index'].apply(lambda x: x.split(' - ')[1] if ' - ' in x else None)

    # Update 'Semana' to keep only the part before the delimiter
    Eviction_results['index'] = Eviction_results['index'].apply(lambda x: x.split(' - ')[0] if ' - ' in x else x)
    Eviction_results.rename(columns={'index': 'Semana'}, inplace=True)

    # Removing spaces from column names
    Eviction_results.columns = [col.replace(' ', '_') for col in Eviction_results.columns]
    Eviction_results.columns = [col.replace('(', '') for col in Eviction_results.columns]
    Eviction_results.columns = [col.replace(')', '') for col in Eviction_results.columns]
    Eviction_results.columns = [col.replace('%', 'Porcent') for col in Eviction_results.columns]  

    # Normalising column names
    for col in Eviction_results.columns:
        if col.startswith(('Outras_Porcent', 'Outras_Porcent_ou_Pontos', 'Outras_Porcent_ou_Votos','Outras_Porcent_ou_Votos')):
            Eviction_results.rename(columns={col: 'porcent_outros_'}, inplace=True)

    # Make duplicate column names unique
    excluded = Eviction_results.columns[~Eviction_results.columns.duplicated(keep=False)]
    counters = {}

    def ren(name):
        if name in excluded:
            return name
        if name not in counters:
            counters[name] = itertools.count()  # Create a new counter
        return f"{name}{next(counters[name])}"

    Eviction_results.columns = [ren(name) for name in Eviction_results.columns]

    # Handling duplicated values in Eliminado
    prefixes = ('Eliminado','porcent_outros_','Expulso','Paredao')

    for prefix in prefixes:
        columns_to_check = [col for col in Eviction_results.columns if col.startswith(prefix)]
        if columns_to_check:
            for index, row in Eviction_results.iterrows():
                unique_values = list(dict.fromkeys(row[columns_to_check]))
                for i, col in enumerate(columns_to_check):
                    Eviction_results.loc[index, col] = unique_values[i] if i < len(unique_values) else None

    Eviction_results = Eviction_results.dropna(axis=1, how='all')

     # Save to csv
    year = url.rsplit('_', 1)[-1]

    Nominations.to_csv(f'nominations{year}')
    Individual_nominations.to_csv(f'individualnominations{year}')
    Eviction_results.to_csv(f'evictionresults{year}')

    return print("CSV files successfully saved")
    
# List of URLs to process
base_url = "https://pt.wikipedia.org/wiki/Big_Brother_Brasil_"
number_of_shows = 25

urls = [f"{base_url}{i}" for i in range(1, number_of_shows + 1)]

for url in urls:
    try:
        nominations_scrape(url)
        print(f"Processed and saved CSV files for: {url}")
    except Exception as e:
        print(f"Error processing {url}: {e}")

