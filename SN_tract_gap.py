# Description: Sample for year 2015 specific
# Author: Mao Li
import pandas as pd
import os
import pyodbc
from shapely.geometry import Point
import numpy as np
import shapely
import geopandas as gpd
import fiona
import pycrs

rootPath_for_tmpFiles = r"D:\ArcGIS_kernelDensity\Store_Density"
allUSA_CTracts3832nozero = rootPath_for_tmpFiles + r"\regardsParticipants_aft_allUSA_CTracts3832nozero.shp"  # ** Regards shape file with participants census tract and a projection that allows us to compute distances in meters
tractsAllFile = rootPath_for_tmpFiles + r"\allUSA_CTracts_3832.shp"
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
        school = data[['ncessch_num', 'free_lunch', 'latitude', 'longitude','enrollment']]
        school.rename(columns={'ncessch_num':'ncessch'},inplace=True)
        # ************************ end processing and updating NETS data for the moves and sales information
        schoolGeo = gpd.GeoDataFrame(school, geometry=gpd.points_from_xy(school['longitude'],
                                                                         school['latitude']))
        schoolGeo.rename(columns={'geometry': 'coord'}, inplace=True)
        school = schoolGeo
        school1 = gpd.GeoDataFrame(school, geometry='coord')
        # **********************************************************************************
        # ********** Update the CRS  (coordinate reference system) for the establishment file, use an existent crs=4326 from a file, and then convert it to  crs=3832  (distance units as meters)
        # ******** Also, create temporary files for processing
        crs = pycrs.load.from_file(
            r"C:\Users\maolee\Documents\ArcGIS_kernelDensity\Store_Density\allUSA_CTracts_4326.prj")
        crs_info = crs.to_proj4()
        school1.crs = crs_info
        school1.to_crs(tract.crs, inplace=True)
        after_join = gpd.sjoin(school1, tract, how='left', op="intersects")
        after_join = pd.DataFrame(after_join)
        after_join = after_join[['ncessch','GEOID','free_lunch','enrollment']]
        raceinfo = pd.read_csv("D:\School\group_race\school_race_2015.csv")
        after_join = after_join.merge(raceinfo, how='left',on='ncessch')
        pov = pd.read_csv(r"D:\School\inTOpov.csv")
        race = pd.read_csv(r"D:\School\raceBYage.csv")
        race = race[['GISJOIN', 'nwhite13_17', 'nblack13_17', 'namericanIndian13_17',
                     'nasian13_17', 'nnativeHawaiian13_17', 'nother13_17',
                     'ntwoOrMore13_17']]
        race['youngTotal'] = race['nwhite13_17'] + race['nblack13_17'] + race['namericanIndian13_17'] + race[
            'nasian13_17'] + race['nnativeHawaiian13_17'] + race['nother13_17'] + race['ntwoOrMore13_17']
        race['GEOID'] = race['GISJOIN'].str[1:3] + race['GISJOIN'].str[4:7] + race['GISJOIN'].str[8:]
        pov['GEOID'] = pov['tract'].astype(str).str.zfill(11)
        pov = pov[['GEOID','inTOpovL13_17','totpop1317']]
        after_join = after_join.merge(pov,how='left',on='GEOID').merge(race,how='left',on='GEOID')
        after_join['nwhite_gap'] = (after_join['nwhite13_17'] / after_join['youngTotal']) - (
                    after_join['White'] / after_join['race_total'])
        after_join['nblack_gap'] = (after_join['nblack13_17'] / after_join['youngTotal']) - (
                    after_join['Black'] / after_join['race_total'])
        after_join['namericanIndian_gap'] = (after_join['namericanIndian13_17'] / after_join[
            'youngTotal']) - (after_join['American Indian or Alaska Native'] / after_join['race_total'])
        after_join['nasian_gap'] = (after_join['nasian13_17'] / after_join['youngTotal']) - (
                    after_join['Asian'] / after_join['race_total'])
        after_join['nnativeHawaiian_gap'] = (after_join['nnativeHawaiian13_17'] / after_join[
            'youngTotal']) - (after_join['Native Hawaiian or other Pacific Islander'] / after_join[
            'race_total'])
        after_join['ntwoOrMore_gap'] = (after_join['ntwoOrMore13_17'] / after_join['youngTotal']) - (
                    after_join['Two or more races'] / after_join['race_total'])
        after_join['povGap'] = (after_join['inTOpovL13_17']/after_join['totpop1317']) - (after_join['free_lunch']/after_join['enrollment'])
