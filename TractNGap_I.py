"""
Description: Calculate school-neighborhood gap using tract as neighborhood
Author: Mao Li
"""

# Load Python libraries
import pandas as pd
import os
import pyodbc
from shapely.geometry import Point
import numpy as np
import shapely
import geopandas as gpd
import fiona
import pycrs
from pyjarowinkler import distance

# Setting the working path
rootPath_for_tmpFiles = r"D:\ArcGIS_kernelDensity\Store_Density"
allUSA_CTracts3832nozero = rootPath_for_tmpFiles + r"\regardsParticipants_aft_allUSA_CTracts3832nozero.shp"
tractsAllFile = rootPath_for_tmpFiles + r"\allUSA_CTracts_3832.shp"
crossWalk = pd.read_csv(r"D:\School\int_CTRACTS_proportionSABs_table.csv")
pov = pd.read_csv(r"D:\School\inTOpovTract.csv")  # tract-level poverty information
race = pd.read_csv(r"D:\School\raceBYageTract.csv")  # tract-level race information
# ************* Define set of years to be used for computation of Measures
years = ['2015', '2016']
# list of year available: ['2000','2001','2002','2003','2004','2005','2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016']
schoolType = ['public']
ccd = pd.read_csv(r"D:\School\schools_ccd_directory.csv")  # Common core data: Free lunch program
tract = gpd.read_file(tractsAllFile)
#  Specially for year from 13 - 17
#  If needed, add condition in this
race = race[['GISJOIN', 'nwhite13_17', 'nblack13_17', 'namericanIndian13_17',
             'nasian13_17', 'nnativeHawaiian13_17', 'nother13_17',
             'ntwoOrMore13_17']]
race['youngTotal'] = race['nwhite13_17'] + race['nblack13_17'] + race['namericanIndian13_17'] + race[
    'nasian13_17'] + race['nnativeHawaiian13_17'] + race['nother13_17'] + race['ntwoOrMore13_17']
race['GEOID'] = race['GISJOIN'].str[1:3] + race['GISJOIN'].str[4:7] + race['GISJOIN'].str[8:]
pov['GEOID'] = pov['tract'].astype(str).str.zfill(11)
pov = pov[['GEOID', 'inTOpovL13_17', 'totpop1317']]
crossWalk['GEOID'] = crossWalk['GEOID10'].astype(str).str.zfill(11)
# Get the data per years
for curSchoolType in schoolType:
    Tablelist = pd.DataFrame()
    for curYear in years:
        print(str(curYear) + " " + str(curSchoolType))
        # Filter data that have actual valid geographic information
        data = ccd[ccd['year'] == int(curYear)]
        freeLunch = data[['ncessch_num', 'free_lunch', 'enrollment', 'school_name']]
        freeLunch.rename(columns={'ncessch_num': 'ncessch'}, inplace=True)
        freeLunch = freeLunch.loc[(freeLunch['free_lunch'] >= 0) & (
                freeLunch['enrollment'] >= 0),]  # The schools have actually free lunch enrollment
        raceinfo = pd.read_csv("D:\School\group_race\school_race_" + curYear + ".csv")
        freeLunch_race = freeLunch.merge(raceinfo, how='outer', on='ncessch')

        #  Merge tract-level information with Crosswalk
        disaggregate = crossWalk.merge(freeLunch_race, how='left', on='ncessch').merge(pov, how='left',
                                                                                       on='GEOID').merge(race,
                                                                                                         how='left',
                                                                                                         on='GEOID')
        #  Calculate the disaggregation
        disaggregate['whiteDA'] = disaggregate['nwhite13_17'] * disaggregate['perIntTrct']
        disaggregate['blackDA'] = disaggregate['nblack13_17'] * disaggregate['perIntTrct']
        disaggregate['otherDA'] = disaggregate['nother13_17'] * disaggregate['perIntTrct']
        disaggregate['asianDA'] = disaggregate['nasian13_17'] * disaggregate['perIntTrct']
        disaggregate['americanIndianDA'] = disaggregate['namericanIndian13_17'] * disaggregate['perIntTrct']
        disaggregate['hawaiianDA'] = disaggregate['nnativeHawaiian13_17'] * disaggregate[
            'perIntTrct']
        disaggregate['twoOrMoreDA'] = disaggregate['ntwoOrMore13_17'] * disaggregate['perIntTrct']
        disaggregate['totalDA'] = disaggregate['youngTotal'] * disaggregate['perIntTrct']
        disaggregate['povDA'] = disaggregate['inTOpovL13_17'] * disaggregate['perIntTrct']
        disaggregate['totpopDA'] = disaggregate['totpop1317'] * disaggregate['perIntTrct']
        after_join = disaggregate
        #  Calculate the Gap measure in tract level
        after_join['nwhite_gap'] = (after_join['White'] / after_join['race_total']) - (
                after_join['whiteDA'] / after_join['totalDA'])
        after_join['nblack_gap'] = (after_join['Black'] / after_join['race_total']) - (
                after_join['blackDA'] / after_join['totalDA'])
        after_join['namericanIndian_gap'] = (after_join['American Indian or Alaska Native'] / after_join[
            'race_total']) - (after_join['americanIndianDA'] / after_join['totalDA'])
        after_join['nasian_gap'] = (after_join['Asian'] / after_join['race_total']) - (
                after_join['asianDA'] / after_join['totalDA'])
        after_join['nnativeHawaiian_gap'] = (after_join['Native Hawaiian or other Pacific Islander'] / after_join[
            'race_total']) - (after_join['hawaiianDA'] / after_join[
            'totalDA'])
        after_join['ntwoOrMore_gap'] = (after_join['Two or more races'] / after_join['race_total']) - (
                after_join['twoOrMoreDA'] / after_join['totalDA'])
        after_join['povGap'] = (after_join['free_lunch'] / after_join['enrollment']) - (
                after_join['povDA'] / after_join['totpopDA'])
        #  Keep the primary school
        after_join['gslo'].replace({'PK': '-1', 'KG': '0', 'N': '9', 'UG': '0'}, inplace=True)
        after_join['gslo'] = after_join['gslo'].astype(int)
        after_join = after_join.loc[~(after_join['gslo'] > 4),]
        # Group data in the census tract level
        tract_g = after_join.groupby('GEOID10')[
            ['nwhite_gap', 'nblack_gap', 'namericanIndian_gap', 'nasian_gap', 'nnativeHawaiian_gap', 'ntwoOrMore_gap',
             'povGap']]
        # Using max - min to determine the gap measure if there is a tract has several school in there.
        tractF = tract_g.agg(np.ptp)
        index = tractF.loc[tract_g.count()['povGap'] == 1,].index
        tractF = tractF.drop(index)
        tractF = tractF.reset_index()
        summ = tract_g.sum().loc[index,]
        summ = summ.reset_index()
        tractF = tractF.append(summ)
        # drop the census tracts which have all na values
        tractF.dropna(thresh=7, inplace=True)
        # drop the census tract which do not have any valid information in race
        tractF = tractF.loc[~((tractF.iloc[:, 1:7] == 0).all(1) & (tractF.loc[:, 'povGap'] == np.nan)), ]
        tractF.to_csv(r'D:\School\Sample\tractASnb_' + curYear + '.csv', index=False)
