#!/bin/env python

import sqlite3
import pandas as pd

# Patients needed:
# 18+ years
# Indication: Cerebral edema
# On HTS (3%)

database = sqlite3.connect('MIMIC.db')

# Create initial dataframe and convert DOB and ADMITTIME to datetime objects
age_query = 'SELECT adm.HADM_ID, pat.DOB, adm.ADMITTIME FROM PATIENTS pat INNER JOIN ADMISSIONS adm WHERE pat.SUBJECT_ID = adm.SUBJECT_ID'
df_age = pd.read_sql_query(age_query, database)
# df_age['ADMITTIME'] = pd.to_datetime(df_age['ADMITTIME'])
# df_age['DOB'] = pd.to_datetime(df_age['DOB'])

# Get age as the difference between the ADMITTIME and the DOB
# AND remove those columns
# df_age['AGE'] = df_age.apply(lambda x: df_age['ADMITTIME'] - df_age['DOB'], axis=1)
# df_age.drop(['DOB', 'ADMITTIME'], axis=1, inplace=True)

# Convert age to a float
# Get ages 18 and above
# Remove the AGE column
# df_age['AGE'] = df_age.apply(lambda x: df_age['AGE'].total_seconds() / 86400 / 365)
# df_age['AGE'] = df_age.query('AGE > 17')
# df_age.drop('AGE', axis=1, inplace=True)

# All patient with the ICD9 code for cerebral edema
icd_query = f'SELECT HADM_ID FROM DIAGNOSES_ICD WHERE ICD9_CODE = 3485'
df_icd = pd.read_sql_query(icd_query, database)

# This dataframe now contains the HADM_IDs for all admissions with Cerebral Edema
df_age_diagnosis = df_age.merge(df_icd, how='inner')

# These values have been derived from either the column's own table, or the D_ITEMS table
prescription_query = f'SELECT HADM_ID FROM PRESCRIPTIONS WHERE (DRUG = "Sodium Chloride 3% (Hypertonic)") '
metavision_query = 'UNION SELECT HADM_ID FROM INPUTEVENTS_MV WHERE (ITEMID = 225161) '
carevue_query = 'UNION SELECT HADM_ID FROM INPUTEVENTS_CV WHERE (ITEMID = 30143)'

final_query = prescription_query + metavision_query + carevue_query
df_hts = pd.read_sql_query(final_query, database)
df_hts = df_hts.dropna()
df_hts['HADM_ID'] = df_hts.apply(lambda x: int(x), axis=1)

# Get the final dataframe from merging the age
# The HADM_IDs in this dataframe are the ones which need to have their LABEVENTS checked
df_final_ids = df_hts.merge(df_age_diagnosis, how='inner')
hadm_ids = df_final_ids['HADM_ID'].to_list()
hadm_ids_string = ','.join([str(float(i)) for i in hadm_ids])  # float != int

# The following dataframes must be reindex as per the following:
reindex = ['HADM_ID', 'CHARTTIME']

# Get data from the LABEVENTS table
# Blood osmolarity: 50964, Blood sodium (different from whole blood sodium): 50983
sodium_query = f'SELECT HADM_ID, CHARTTIME, VALUE FROM LABEVENTS WHERE HADM_ID IN ({hadm_ids_string}) AND ITEMID = 50983'
df_sodium = pd.read_sql_query(sodium_query, database)
df_sodium = df_sodium.set_index(reindex)

osmolarity_query = f'SELECT HADM_ID, CHARTTIME, VALUE FROM LABEVENTS WHERE HADM_ID IN ({hadm_ids_string}) AND ITEMID = 50964'
df_osmolarity = pd.read_sql_query(osmolarity_query, database)
df_osmolarity = df_osmolarity.set_index(reindex)

# Get the final datapairs
df_final = df_sodium.join(df_osmolarity, how='inner', rsuffix='_')
df_final = df_final.rename({'VALUE': 'SODIUM', 'VALUE_': 'OSMOLARITY'}, axis=1)
df_final = df_final.reset_index(drop=True)

df_final.to_csv('pairs.csv', index=False)