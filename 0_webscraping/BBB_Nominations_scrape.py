#!/usr/bin/env python
# coding: utf-8

# In[25]:


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

    # Set index for rows
    index_column = Nominations.columns[0]
    Nominations.set_index(index_column)

    # Drop any duplicate columns to the index
    Nominations = Nominations.loc[:, ~Nominations.T.duplicated()]

    #Transpose the table
    Nominations = Nominations.T
    Nominations.columns = Nominations.iloc[0]
    Nominations = Nominations[1:]

    # Adding the year of the current file
    Nominations['Edicao'] = url.rsplit('_', 1)[-1]

    # Renaming index column
    Nominations.index.name = 'Semana'

    # Manipulating the Individual_nominations table

    # Remove Na columns
    Individual_nominations = Individual_nominations.dropna(axis=1, how='all')

    # Set index for rows
    Individual_nominations.set_index(Individual_nominations.columns[0])

    #Transpose the table
    Individual_nominations = Individual_nominations.T
    Individual_nominations.columns = Individual_nominations.iloc[0]
    Individual_nominations = Individual_nominations[1:]

    # Adding the year of the current file
    Individual_nominations['Edicao'] = url.rsplit('_', 1)[-1]

    # Renaming index column
    Individual_nominations.index.name = 'Semana'

    # Manipulating the Eviction_results table

    # Remove Na columns
    Eviction_results = Eviction_results.dropna(axis=1, how='all')

    # Set index for rows
    Eviction_results.set_index(Eviction_results.columns[0])

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

    # Renaming index column
    Eviction_results.index.name = 'Semana'        

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



# In[ ]:




