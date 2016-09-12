#!/usr/bin/python

import pandas as pd
from pandas import DataFrame,Series
import numpy as np
import datetime as dt
from optparse import OptionParser

import mysql.connector
from sqlalchemy import create_engine

import netrc


def clean_up_characteristics(Year):
    '''Return the cleaned up dataframe of the characteristics
    of acccidents.

    This is a simple clean up:
      1) Convert the 4 columns of date and time into one
        datetime column
      2) Remove unreliable information
      3) Rename the columns
      4) Correct the Area ID to make it usable
    
    Args:
        Year: the year of the CSV file.
    Returns:
        The Pandas dataframe of the characteristics of accidents.
    '''

    print('           - ',Year)

    caract_df = pd.read_csv(("data/%s_France_caracteristiques.csv" % Year),
                            sep=',', encoding='latin-1')

    # 1) Create one datetime column
    def columns_to_datetime(sr):
        '''Convert the 4 columns of year, months, days, and time
        into one datetime column.
        Args:
            Pandas Series: Corresponds to one row of the dataframe
                            being modified.
        Returns:
            Pandas Series: The input series added with the new
                            datetime entry.
        '''
        stringall = "%d-%d-%d %04d" % (sr['an'], sr['mois'], sr['jour'], sr['hrmn'])
        sr = sr.append(pd.Series(dt.datetime.strptime(stringall, "%y-%m-%d %H%M"),
                                 index=["datetime"]))
        return sr

    clean_df = caract_df.apply(columns_to_datetime,axis=1)

    # We can remove the original time and date columns
    clean_df.drop(['an','mois','jour','hrmn'],axis=1, inplace=True)


    # 2) Remove unreliable information
    clean_df.drop(['adr','gps','lat','long'],axis=1, inplace=True)


    # 3) Rename the columns
    clean_df.rename(columns={'Num_Acc':'accident id','lum':'luminosity',
                            'agg':'in city','int':'intersect type',
                            'atm':'weather','col':'collision type',
                            'com':'city id','dep':'area id'},
                    inplace=True)

    # The accident id is unique so we can use it as index now
    # and primary key in the SQL database
    clean_df.set_index(['accident id'],inplace=True,verify_integrity=True)
    clean_df.index.names = ['accident id']

    # 4) Area ID correction
    # For some reason most area IDs have an added 0 except for the overseas
    # areas 971, 973, 974, and 976.
    def correct_area_id(sr):
        if sr['area id'] < 970:
            sr['area id'] = int(sr['area id'] /10)
        return sr
    clean_df = clean_df.apply(correct_area_id,axis=1)

    clean_df['area id'].unique()

    return clean_df


def clean_up_locations(Year):
    '''Return the cleaned up dataframe of vehicles
    involved in acccidents.

    This is a simple clean up:
      1) Remove columns we want use
      2) Rename the columns
    
    Args:
        Year: the year of the CSV file.
    Returns:
        The Pandas dataframe of the vehicles in accidents.
    '''

    print('           - ',Year)

    lieux_df = pd.read_csv(("data/%s_France_lieux.csv" % Year),
                            sep=',', encoding='latin-1')

    # 1) Remove useless information
    clean_df = lieux_df.drop(['voie','v1','v2','pr','pr1'],axis=1)
    # clean_df.head()


    # 2) Rename columns
    clean_df.rename(columns={'Num_Acc':'accident id','catr':'road type',
                            'circ':'traffic mode','nbv':'nb lanes',
                            'vosp':'reserved lane','prof':'road profil',
                            'plan':'road alignment','lartpc':'central reservation',
                            'larrout':'road width','surf':'road surface',
                            'infra':'installations','situ':'location',
                            'env1':'school distance'},
                    inplace=True)

    # The accident id is unique so we can use it as index now
    # and primary key in the SQL database
    clean_df.set_index(['accident id'],inplace=True,verify_integrity=True)
    clean_df.index.names = ['accident id']

    return clean_df


def clean_up_vehicles(Year):
    '''Return the cleaned up dataframe of vehicles
    involved in acccidents.

    This is a simple clean up:
      1) Remove columns we want use
      2) Rename the columns
    
    Args:
        Year: the year of the CSV file.
    Returns:
        The Pandas dataframe of the vehicles in accidents.
    '''

    print('           - ',Year)

    vehicules_df = pd.read_csv(("data/%s_France_vehicules.csv" % Year),
                                sep=',', encoding='latin-1')

    # 1) Remove columns
    clean_df = vehicules_df.drop('senc',axis=1)

    # 2) Reorganize vehicle type categories
    newMap = {}
    for i,I in enumerate(clean_df['catv'].value_counts().sort_index().index):
        newMap[I] = i+1
    clean_df['catv'] = clean_df['catv'].map(newMap)

    # 3) Rename columns
    clean_df.rename(columns={'Num_Acc':'accident id', 'catv':'vehicle type',
                            'obs':'fixed obj hit','obsm':'moving obj hit',
                            'choc':'impact location','manv':'maneuver',
                            'occutc':'nb occupants public transit',
                            'num_veh':'vehicle id'}, inplace=True)

    return clean_df


def clean_up_users(Year):
    '''Return the cleaned up dataframe of road users
    involved in acccidents.

    The clean up requires 3 steps:
      1) Reformat the safety gear information
      2) Convert the birth year to age of victims
      3) Rename the columns
    
    Args:
        Year: the year of the CSV file.
    Returns:
        The Pandas dataframe of the road users
    '''

    print('           - ',Year)

    usagers_df = pd.read_csv(("data/%s_France_usagers.csv" % Year),
                            sep=',', encoding='latin-1')

    # 1) Reformat 'secu' to split it into two more useful columns.
    # We should always have the first digit since the value 9
    # represent 'others' for non-defined types.

    def safety_to_twocolumns(sr):
        '''Break down the 'secu' entry into two columns
        about safety gears.
        Args:
            Pandas Series: Corresponds to one row of the dataframe
                            being modified.
        Returns:
            Pandas Series: The input series added with the two new
                            columns on safety gears.
        '''
        if not np.isnan(sr['secu']):
            safetychar = "%2d" % (sr['secu'])
            if safetychar[0] != ' ':
                sr = sr.append(pd.Series(int(safetychar[0]), index=["safety gear type"]))
            sr = sr.append(pd.Series(int(safetychar[1]), index=["safety gear worn"]))
        return sr

    clean_df = usagers_df.apply(safety_to_twocolumns,axis=1)
    clean_df.drop('secu',inplace=True,axis=1)

    # 2) Convert victim's birth year to their age

    clean_df['age'] = int(Year) - clean_df['an_nais']
    clean_df.drop('an_nais',inplace=True,axis=1)

    # 3) Rename the columns
    clean_df.rename(columns={'Num_Acc':'accident id', 'num_veh':'vehicle id',
                             'place':'location in vehicle','catu':'user type',
                             'grav':'severity','sexe':'sex','trajet':'journey type',
                             'locp':'pedestrian location','actp':'pedestrian action',
                             'etatp':'pedestrian company'},inplace=True)
    
    return clean_df


def load_sql_engine():
    '''Return the SQLAlchemy connectable.

    The login and password are retrieved from a netrc file
    and the connection happens through the mysql socket.
    
    Returns:
        A SQLAlchemy connectable
    '''
    login_info = netrc.netrc().authenticators('mysql')
    mysqllogin = '%s:%s@%s' % (login_info[0], login_info[2], login_info[1])
    engine = create_engine('mysql+mysqlconnector://'+mysqllogin+
            '/safer_roads?unix_socket=/run/mysqld/mysqld.sock', echo=False)
    return engine

if __name__ == '__main__':
    # Check the options
    parser = OptionParser()
    parser.add_option("-R", "--replace", dest="replace",
            help="Recreate the database tables from scratch.", action="store_true")
    (options, args) = parser.parse_args()

    if args[0].lower() == 'all':
        Years = ['2010','2011','2012','2013','2014']
        options.replace = True
    else:
        Years = [args[0]]

    # Setup SQL engine
    # Load the dataframes in the database only if there wasn't
    # any problem in the preparation of the dataframes
    sqlEngine = load_sql_engine()

    tables={'characteristics': [clean_up_characteristics, 'accident id'],
            'users': [clean_up_users, 'index'],
            'locations': [clean_up_locations, 'accident id'],
            'vehicles': [clean_up_vehicles, 'index']}

    for name, params in tables.items():
        new_df = DataFrame()
        print(' -> Clean up {} dataframe'.format(name))
        for year in Years:
            # clean up the table
            clean_func = params[0]
            tmp_df = clean_func(year)

            if new_df.empty:
                new_df = tmp_df
            else:
                new_df = new_df.append(tmp_df, (params[1] == 'index'))

        print(' -> Load {} dataframe to SQL database'.format(name))

        if options.replace:
            if_exists = 'replace'
        else:
            if_exists = 'append'

        sql_options = {'con':sqlEngine, 'if_exists':if_exists,
                'index':True, 'chunksize':100}

        new_df.to_sql(name=name, **sql_options)

        # We want the index to be a primary key to speed things up among other reasons.
        sqlEngine.execute('''ALTER TABLE {}
                ADD PRIMARY KEY (`{}`)'''.format(name,params[1]))

