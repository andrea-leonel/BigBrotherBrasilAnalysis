#!/usr/bin/env python
# coding: utf-8

# In[207]:


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

def ratings_scrape(url):

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
            
    else: print("Header Ratings not available")

    if desired_table:

        # Replacing commas for periods in decimals
        for element in desired_table.find_all(string=True):
            if ',' in element:
                updated_text = element.replace(',', '.')
                element.replace_with(updated_text)
        
        # Parsing html table to DataFrame
        html_to_table = pd.read_html(StringIO(str(desired_table)))
        Ratings = html_to_table[0]

        # Keep only the bottom row with the overall ratings for the season
        Ratings = Ratings[Ratings["SEG"] == "Media da edicao"]

        # Adding the year of the current file
        Ratings = Ratings.copy()
        Ratings['Edicao'] = int(url.rsplit('_', 1)[-1])

        # Keeping only relevant columns
        relevant_columns = Ratings[['Edicao','Media semanal']]
        Ratings = relevant_columns

        # Rename column
        Ratings.rename(columns={"Media semanal": "Media da edicao"}, inplace=True)

    else: print("Ratings table not found")

    return Ratings

# Appending the Ratings to one single dataframe

base_url = "https://pt.wikipedia.org/wiki/Big_Brother_Brasil_"
number_of_shows = 25

urls = [f"{base_url}{i}" for i in range(1, number_of_shows + 1)]

Combined_ratings = []

for url in urls:
    try:
        ratings_new = ratings_scrape(url)
        Combined_ratings.append(ratings_new)
        print(f"Ratings information for {url} appended")
    except Exception as e:
        print(f"Error processing {url}: {e}")
    
# Save to csv
Ratings_average = pd.concat(Combined_ratings, ignore_index=True)
Ratings_average.to_csv(f'ratingaverage.csv', drop_index=True)
    


# In[ ]:




