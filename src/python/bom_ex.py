#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from pathlib import Path


# In[2]:


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


# In[28]:


def replace_NaN(df)->pd.Dataframe:
    df["component_material_production_type"] = (
    df["component_material_production_type"]
    .fillna("_")
    )
    return df
    


# In[29]:


clean_df = replace_NaN(df)


# In[30]:


clean_df.info()


# In[31]:


def filter_FIN(df)->pd.Dataframe:
    return df[df['produced_material_release_type'] == 'FIN']


# In[32]:


fin_df = filter_FIN(df)


# In[33]:


fin_df.head()


# In[17]:


fin_materials = fin_df["produced_material"].unique()
fin_materials


# In[44]:


def agg_df(df)->pd.Dataframe:
    return ( 
        df.groupby(
        [
            "plant_id",
            "year",
            "produced_material",
            "produced_material_release_type",
            "produced_material_production_type",
            "component_material",
            "component_material_release_type",
            "component_material_production_type",
        ],
        as_index=False
        ).agg(
            produced_material_quantity=("produced_material_quantity", "sum"),
            component_material_quantity=("component_material_quantity", "sum"),
        )
        )


# In[45]:


fact_df= agg_df(clean_df)


# In[46]:


fact_df


# In[53]:


def traversal_materials(fact_df:pd.Dateframe,fin_df:pd.Dateframe,fin_materials:list)->pd.Dateframe:
    result_rows = []
    for fin_material in fin_materials:
    
        fin_rows = fin_df[fin_df["produced_material"] == fin_material]
        fin_info = fin_rows.iloc[0]
        
        queue = [fin_material]
        visited = set()
    
        while queue:
    
            current_material = queue.pop(0)
    
            if current_material in visited:
                continue
    
            visited.add(current_material)
    
            child = fact_df[fact_df['produced_material'] == current_material]
    
            if child.empty:
                continue
            for _,row in child.iterrows():
                result_rows.append({
                "plant": row["plant_id"],
                "year": row["year"],
            
                "fin_material_id": fin_material,
                "fin_material_release_type": fin_info["produced_material_release_type"],
                "fin_material_production_type": fin_info["produced_material_production_type"],
                "fin_production_quantity": fin_info["produced_material_quantity"],
            
                "prod_material_id": current_material,
                "prod_material_release_type": row["produced_material_release_type"],
                "prod_material_production_type": row["produced_material_production_type"],
                "prod_material_production_quantity": row["produced_material_quantity"],
            
                "component_id": row["component_material"],
                "component_material_release_type": row["component_material_release_type"],
                "component_material_production_type": row["component_material_production_type"],
                "component_consumption_quantity": row["component_material_quantity"]
            })
    
                component = row["component_material"]
                component_type = row["component_material_release_type"]
    
                if component_type in("FIN", "PROD") and component not in visited :
                    queue.append(component)

    return pd.DataFrame(result_rows)


# In[54]:


final_df = traversal_materials(fact_df,fin_df,fin_materials)


# In[57]:


result_df


# In[ ]:




