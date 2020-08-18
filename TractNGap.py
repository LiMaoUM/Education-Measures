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
rootPath_for_tmpFiles = r"D:\ArcGIS_kernelDensity\Store_Density"
allUSA_CTracts3832nozero = rootPath_for_tmpFiles + r"\regardsParticipants_aft_allUSA_CTracts3832nozero.shp"  # ** Regards shape file with participants census tract and a projection that allows us to compute distances in meters
tractsAllFile = rootPath_for_tmpFiles + r"\allUSA_CTracts_3832.shp"
crossWalk = pd.read_csv(r"D:\School\int_CTRACTS_proportionSABs_table.csv")
pov = pd.read_csv(r"D:\School\inTOpovTract.csv")
race = pd.read_csv(r"D:\School\raceBYageTract.csv")
# ************* Define set of years to be used for computation of Kernel Density
years = ['2015']
# list of year available: ['2000','2001','2002','2003','2004','2005','2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016']
schoolType = ['public']
ccd = pd.read_csv(r"D:\School\schools_ccd_directory.csv")
tract = gpd.read_file(tractsAllFile)
for curSchoolType in schoolType:
    Tablelist = pd.DataFrame()
    for curYear in years:
        print(str(curYear) + " " + str(curSchoolType))
        # ** set vars to process the two las digits of the years to match the names of the columns in NETS data: NAICS+YEAR
        data = ccd[(ccd['year'] == int(curYear)) & (ccd['latitude'] != -2) & (ccd['latitude'] < 90) & (
                ccd['longitude'] != -2) & (ccd['longitude'] > -180)]
        freeLunch = data[['ncessch_num', 'free_lunch', 'enrollment']]
        freeLunch.rename(columns={'ncessch_num': 'ncessch'}, inplace=True)
        freeLunch = freeLunch.loc[(freeLunch['free_lunch']>=0)&(freeLunch['enrollment']>=0),]
        raceinfo = pd.read_csv("D:\School\group_race\school_race_2015.csv")
        freeLunch_race = freeLunch.merge(raceinfo, how='left', on='ncessch')
        disaggregate = crossWalk.merge(freeLunch_race, how='left', on='ncessch')
        disaggregate['whiteDA'] = disaggregate['White'] * disaggregate['perIntTrct']
        disaggregate['blackDA'] = disaggregate['Black'] * disaggregate['perIntTrct']
        disaggregate['hispanicDA'] = disaggregate['Hispanic'] * disaggregate['perIntTrct']
        disaggregate['asianDA'] = disaggregate['Asian'] * disaggregate['perIntTrct']
        disaggregate['americanIndianDA'] = disaggregate['American Indian or Alaska Native'] * disaggregate['perIntTrct']
        disaggregate['hawaiianDA'] = disaggregate['Native Hawaiian or other Pacific Islander'] * disaggregate[
            'perIntTrct']
        disaggregate['twoOrMoreDA'] = disaggregate['Two or more races'] * disaggregate['perIntTrct']
        disaggregate['totalDA'] = disaggregate['race_total'] * disaggregate['perIntTrct']
        disaggregate['freeLunchDA'] = disaggregate['free_lunch'] * disaggregate['perIntTrct']
        disaggregate['enrollmentDA'] = disaggregate['enrollment'] * disaggregate['perIntTrct']
        disaggregate1 = disaggregate.groupby('GEOID10').sum()[
            ['whiteDA', 'blackDA', 'hispanicDA', 'asianDA', 'americanIndianDA', 'hawaiianDA', 'twoOrMoreDA', 'totalDA',
             'freeLunchDA', 'enrollmentDA']].reset_index()
        disaggregate1['GEOID'] = disaggregate1['GEOID10'].astype(str).str.zfill(11)
        race = race[['GISJOIN', 'nwhite13_17', 'nblack13_17', 'namericanIndian13_17',
                     'nasian13_17', 'nnativeHawaiian13_17', 'nother13_17',
                     'ntwoOrMore13_17']]
        race['youngTotal'] = race['nwhite13_17'] + race['nblack13_17'] + race['namericanIndian13_17'] + race[
            'nasian13_17'] + race['nnativeHawaiian13_17'] + race['nother13_17'] + race['ntwoOrMore13_17']
        race['GEOID'] = race['GISJOIN'].str[1:3] + race['GISJOIN'].str[4:7] + race['GISJOIN'].str[8:]
        pov['GEOID'] = pov['tract'].astype(str).str.zfill(11)
        pov = pov[['GEOID', 'inTOpovL13_17', 'totpop1317']]
        after_join = disaggregate1.merge(pov, how='left', on='GEOID').merge(race, how='left', on='GEOID')
        after_join['nwhite_gap'] = (after_join['nwhite13_17'] / after_join['youngTotal']) - (
                after_join['whiteDA'] / after_join['totalDA'])
        after_join['nblack_gap'] = (after_join['nblack13_17'] / after_join['youngTotal']) - (
                after_join['blackDA'] / after_join['totalDA'])
        after_join['namericanIndian_gap'] = (after_join['namericanIndian13_17'] / after_join[
            'youngTotal']) - (after_join['americanIndianDA'] / after_join['totalDA'])
        after_join['nasian_gap'] = (after_join['nasian13_17'] / after_join['youngTotal']) - (
                after_join['asianDA'] / after_join['totalDA'])
        after_join['nnativeHawaiian_gap'] = (after_join['nnativeHawaiian13_17'] / after_join[
            'youngTotal']) - (after_join['hawaiianDA'] / after_join[
            'totalDA'])
        after_join['ntwoOrMore_gap'] = (after_join['ntwoOrMore13_17'] / after_join['youngTotal']) - (
                after_join['twoOrMoreDA'] / after_join['totalDA'])
        after_join['povGap'] = (after_join['inTOpovL13_17'] / after_join['totpop1317']) - (
                    after_join['freeLunchDA'] / after_join['enrollmentDA'])
        after_join.dropna(inplace=True)
        after_join.to_csv(r'D:\School\Sample\tractASnb.csv', index=False)
