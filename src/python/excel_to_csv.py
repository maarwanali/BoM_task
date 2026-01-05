#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
from pathlib import Path


# In[4]:


def read_file(file_path:str)->pd.Dataframe:
    file = Path(file_path)
    if not file.is_file():
        raise FileNotFoundError(f'{file_path} does not found')
    try:
        df = pd.read_excel(file_path)

        return df
    except Exception as e:
        raise ValueError(f"Failed to read Excel file '{file_path}': {e}")
    


# In[5]:


df = read_file('../../data/task_2_data_ex.xlsx')


# In[9]:


df


# In[10]:


df.to_csv('../import/production_data.csv',index=False)

