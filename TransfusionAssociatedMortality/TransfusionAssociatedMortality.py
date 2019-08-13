#!/bin/python

import os
import sqlite3
import statsmodels.api as sm

import pandas as pd
import numpy as np
from scipy.stats import ttest_ind, chi2_contingency
# import seaborn as sns; sns.set()

def get_data():
    # Tables
    transfusion_tables = [
        'TRANS_BLOOD_4HOURS', 'TRANS_BLOOD_24HOURS',
        'TRANS_PLASMA_4HOURS', 'TRANS_PLASMA_24HOURS',
        'TRANS_PLATELETS_4HOURS', 'TRANS_PLATELETS_24HOURS',
        'TRANS_CRYO_4HOURS', 'TRANS_CRYO_24HOURS']
    tables_prepend = ['transf.' + i for i in transfusion_tables]
    tables_string = ','.join(tables_prepend)

    # These databases have already been whittled down to cases with TBI (NOT isolated)
    database_files = ('2013.db', '2014.db', '2015.db', '2016.db')
    ais_tables_per_year = {
        '2016.db': 'RDS_AISPCODE'}

    print('Gathering data')
    final_df = pd.DataFrame()
    for this_database in database_files:

        try:
            ais_table = ais_tables_per_year[this_database]
        except KeyError:
            ais_table = 'RDS_AISCCODE'

        db = sqlite3.connect(this_database)

        only_tbi_query = f'SELECT PREDOT FROM {ais_table} GROUP BY INC_KEY HAVING COUNT(PREDOT) = 1'
        only_tbi_predots = filter(None, set([i[0] for i in db.execute(only_tbi_query).fetchall()]))
        only_tbi_predots = [i for i in only_tbi_predots if i < 200000]

        # query = f'SELECT ais.INC_KEY, ais.PREDOT, ais.SEVERITY, {tables_string}, disch.HOSPDISP, demo.GENDER FROM {ais_table} ais INNER JOIN RDS_PM transf ON transf.INC_KEY = ais.INC_KEY INNER JOIN RDS_DEMO demo ON transf.INC_KEY = demo.INC_KEY INNER JOIN RDS_DISCHARGE disch ON transf.INC_KEY = disch.INC_KEY'
        # query = f'SELECT demo.GENDER, ais.PREDOT, ais.SEVERITY, {tables_string}, disch.HOSPDISP FROM RDS_DEMO demo INNER JOIN {ais_table} ais ON demo.INC_KEY = ais.INC_KEY INNER JOIN RDS_PM transf ON transf.INC_KEY = demo.INC_KEY INNER JOIN RDS_DISCHARGE disch ON demo.INC_KEY = disch.INC_KEY'
        query = (
            f'SELECT ais.INC_KEY, ais.PREDOT, ais.SEVERITY, {tables_string}, disch.HOSPDISP, demo.GENDER '
            f'FROM {ais_table} ais '
            'INNER JOIN RDS_DEMO demo ON ais.INC_KEY = demo.INC_KEY '
            'INNER JOIN RDS_PM transf ON transf.INC_KEY = ais.INC_KEY '
            'INNER JOIN RDS_DISCHARGE disch ON disch.INC_KEY = ais.INC_KEY')

        this_df = pd.read_sql_query(query, db)

        # Remove all patients with non TBI injuries with a SEVERITY == 6
        lethal_non_tbi = this_df.query('PREDOT not in @only_tbi_predots and SEVERITY == 6').INC_KEY.to_list()
        this_df = this_df.query('INC_KEY not in @lethal_non_tbi')

        # Restrict to only TBI predots
        this_df = this_df.query('PREDOT in @only_tbi_predots')

        final_df = final_df.append(this_df, ignore_index=True)

    return final_df


def format_as_22(this_df, label):
    # The dataframe is expected to be restricted to a certain SEVERITY

                    # EXPIRED          NOT EXPIRED
    # Platelets yes         a                    b
    # Platelets no          c                    d

    comp_variable = 'TRANS_PLATELETS_4HOURS'
    comp_variable_label = 'PLATELETS'

    a = len(this_df.query('TRANS_PLATELETS_4HOURS == 1 and EXPIRED == 1'))
    b = len(this_df.query('TRANS_PLATELETS_4HOURS == 1 and EXPIRED == 0'))
    c = len(this_df.query('TRANS_PLATELETS_4HOURS == 0 and EXPIRED == 1'))
    d = len(this_df.query('TRANS_PLATELETS_4HOURS == 0 and EXPIRED == 0'))

    new_df = pd.DataFrame(
        {'EXPIRED': [a, c],
         'SURVIVED': [b, d],
         comp_variable_label: ['TRANSFUSED', 'NOT TRANSFUSED']})
    new_df = new_df.set_index(comp_variable_label)
    # new_df.to_html(label, classes='table table-striped')

    print(new_df)
    
    new_new_df = this_df[['TRANS_PLATELETS_4HOURS', 'EXPIRED']].assign(placeholder=1).pivot_table(
        index="TRANS_PLATELETS_4HOURS", columns="EXPIRED", values="placeholder", aggfunc="sum")

    print(new_new_df)

    # Perform chi2 test
    stat, p, dof, expected = chi2_contingency(new_df)

    new_df['PERCMORT'] = new_df.apply(lambda x: x['EXPIRED'] / (x['EXPIRED'] + x['SURVIVED']), axis=1)

    print(new_df)
    print(label, stat, p, dof, expected)
    print()

def process_data(df):
    # Process this dataframe
    print('Processing')
    df = df.dropna()
    df.to_csv('temporary_final.csv', index=False)
    df = pd.read_csv('temporary_final.csv')
    os.remove('temporary_final.csv')

    df = df.replace(-1, np.nan)
    df = df.dropna()

    # Get requisite values
    print('Reducing')
    df = df.query('SEVERITY > 2 and SEVERITY < 6')
    df = df.query('PREDOT < 200000')
    df['GENDER'] = df.apply(lambda x: 1 if x['GENDER'] == 'Male' else 0, axis=1)
    df['EXPIRED'] = df.apply(lambda x: 1 if 'xpi' in x['HOSPDISP'] else 0, axis=1)

    df['TRANS_BLOOD_4HOURS'] = df.apply(lambda x: 1 if x['TRANS_BLOOD_4HOURS'] > 0 else 0, axis=1)
    df['TRANS_PLATELETS_4HOURS'] = df.apply(lambda x: 1 if x['TRANS_PLATELETS_4HOURS'] > 0 else 0, axis=1)
    df['TRANS_PLASMA_4HOURS'] = df.apply(lambda x: 1 if x['TRANS_PLASMA_4HOURS'] > 0 else 0, axis=1)
    df['TRANS_CRYO_4HOURS'] = df.apply(lambda x: 1 if x['TRANS_CRYO_4HOURS'] > 0 else 0, axis=1)
    # df['TRANS_BLOOD_24HOURS'] = df.apply(lambda x: 1 if x['TRANS_BLOOD_24HOURS'] > 0 else 0, axis=1)
    # df['TRANS_PLATELETS_24HOURS'] = df.apply(lambda x: 1 if x['TRANS_PLATELETS_24HOURS'] > 0 else 0, axis=1)
    # df['TRANS_PLASMA_24HOURS'] = df.apply(lambda x: 1 if x['TRANS_PLASMA_24HOURS'] > 0 else 0, axis=1)
    # df['TRANS_CRYO_24HOURS'] = df.apply(lambda x: 1 if x['TRANS_CRYO_24HOURS'] > 0 else 0, axis=1)

    df = df.join(pd.get_dummies(df.SEVERITY, prefix='SEVERITY'))

    # Patients not transfused with other substances
    # df = df.query('TRANS_BLOOD_4HOURS == 1 and TRANS_PLASMA_4HOURS == 1')
    df = df.query('TRANS_BLOOD_4HOURS == 0 and TRANS_PLASMA_4HOURS == 0 and TRANS_CRYO_4HOURS == 0')

    # Drop unneeded columns
    df = df.drop(['INC_KEY', 'TRANS_BLOOD_24HOURS', 'TRANS_PLATELETS_24HOURS', 'TRANS_PLASMA_24HOURS', 'TRANS_CRYO_24HOURS'], axis=1)
    df = df.drop(['TRANS_BLOOD_4HOURS', 'TRANS_PLASMA_4HOURS', 'TRANS_CRYO_4HOURS'], axis=1)

    # chi2 test per severity score
    df_3 = df.query('SEVERITY == 3')
    df_4 = df.query('SEVERITY == 4')
    df_5 = df.query('SEVERITY == 5')
    format_as_22(df_3, '3')
    format_as_22(df_4, '4')
    format_as_22(df_5, '5')
    format_as_22(df, 'total')

    # Drop more columns
    df = df.drop(['PREDOT', 'SEVERITY', 'HOSPDISP'], axis=1)

    # Logistic regression
    print('Logistic regression')
    logit = sm.Logit(df[['EXPIRED']], df.drop('EXPIRED', axis=1))
    result = logit.fit()

    conf = result.conf_int()
    conf['OR'] = result.params
    conf.columns = ['2.5%', '97.5%', 'OR']

    print(result.summary2())
    print('Odds ratios (One unit increase in independent to dependent):')
    print(np.exp(conf))

    return df


dataframe = get_data()
dataframe = process_data(dataframe)

# df.to_csv('TransfusionData.csv', index=False)
