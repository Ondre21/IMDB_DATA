# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np
from datetime import datetime
tstart = datetime.now()

# pip install fuzzywuzzy
#install python levenshtein, conda install -c conda-forge python-levenshtein 

#### loading data
#o_data = pd.read_csv(r'C:\Users\mitta\Documents\Giulia\Giulia Thesis\query (15).csv')
anthology_list = pd.read_csv(r'C:\Users\mitta\Documents\Giulia\Giulia Thesis\Inputs\anthology_series_database.csv', sep = ';')
anthology_list['Title'] = anthology_list['TITLE']
#anthology_list.Title = anthology_list.Title.str.replace(r" \(.*\)","")  #fix this
anthology_list.loc[:, 'Title'] = anthology_list.Title.astype(str) #converting non-strings
anthology_list = anthology_list.set_index('Title')
print('Loaded Wiki Anthology Data')
print(datetime.now() - tstart)
print('...')

#getting imdb data
imdb_data = pd.read_table(r'C:\Users\mitta\Documents\Giulia\Giulia Thesis\Inputs\titlebasics.txt', low_memory = False)
#                          dtype={"tconst": str, "titleType": str,
#                                 "primaryTitle": str, "OriginalTitle": str,
#                                 "isAdult": int, "startYear": int,
#                                 "endYear": int , "runtimeMinutes": float,
#                                 "genres": list})

imdb_data['IMDB_TITLE'] = imdb_data.primaryTitle
imdb_data.loc[:, 'IMDB_TITLE'] = imdb_data.IMDB_TITLE.astype(str) #converting non-strings

imdb_data.IMDB_TITLE.fillna(imdb_data.originalTitle, inplace = True) #### filling in nan with other data
imdb_data.IMDB_TITLE.fillna('TITLE NOT AVAILBLE', inplace = True) #### filling in nan with other data
imdb_data = imdb_data.set_index('IMDB_TITLE')

#imdb_data = imdb_data.head(500000)

print('Loaded IMDB Data')
print(datetime.now() - tstart)
print('Starting Fuzzy Wuzzy')
print('...')
from fuzzywuzzy import fuzz

def match_name(name, list_names, min_score=0):
    # -1 score incase we don't get any matches
    max_score = -1
    # Returning empty name for no match as well
    max_name = ""
    # Iternating over all names in the other
    for name2 in list_names:
        #Finding fuzzy match score
        score = fuzz.ratio(name, name2)
        # Checking if we are above our threshold and have a better score
        if (score > min_score) & (score > max_score):
            max_name = name2
            max_score = score
    return (max_name, max_score)

# List for dicts for easy dataframe creation
dict_list = []
# iterating over our players without salaries found above
for name in anthology_list.index:
    # Use our method to find best match, we can set a threshold here
    match = match_name(name, imdb_data.index, 75)
    
    # New dict for storing data
    dict_ = {}
    dict_.update({"Wiki_title" : name})
    dict_.update({"IMDB_TITLE" : match[0]})
    dict_.update({"score" : match[1]})
    dict_list.append(dict_)
  

print('Completed Fuzzy Matching')
print(datetime.now() - tstart)
print('...')
##### join tables together
merge_table = pd.DataFrame(dict_list) # list to dataframe
merge_table = merge_table.set_index('Wiki_title')

smart_merge_df = anthology_list.join(merge_table, how = 'left') # merge smart connection to wiki

#replaceing these values back to nohting, since might create many to many merger
smart_merge_df['IMDB_TITLE'].replace(to_replace = 'TITLE NOT AVAILBLE', value = np.NaN)
print('Merging Items')
print(datetime.now() - tstart)
print(...)


# merge two back together
imdb_wiki_df = smart_merge_df.join(imdb_data, how = 'left', lsuffix='_wiki', rsuffix='_imdb') # left join imdb to wiki , left_on = 'imdb_title', right_on = 'IMDB_TITLE'
print('Saving to CSV')
print(datetime.now() - tstart)
print('...')


#save as csv
imdb_wiki_df.to_csv(r'C:\Users\mitta\Documents\Giulia\Giulia Thesis\Inputs\IMDB_WIKI_MERGE_OUTPUT.csv', sep = ',')

print('Completed ALl Tasks')
print(datetime.now() - tstart)

# set(imdb_data.genres.values) #gives set of values 

