# -*- coding: utf-8 -*-
#Description: Count the frequency of certain category company and based on cetrain populaiton to calculate the density of such company
# Author: Mao Li
# *************************************************************************************************
# *** Load Python libraries
import pandas as pd
import os
import pyodbc
from shapely.geometry import Point
import numpy as np
import shapely
import geopandas as gpd
import fiona
import pycrs
# ****************************** SET up variables, needed files and paths to use throughout the execution of this script
pop = pd.read_excel(r"C:\Users\maolee\Documents\ArcGIS_kernelDensity\nanda_ses2000-2010_02P.xlsx",
                    usecols=['tract_fips10','totpop00','totpop01','totpop02','totpop03','totpop04','totpop05', 'totpop06', 'totpop07', 'totpop08', 'totpop09', 'totpop10'])
pop1 = pd.read_stata(
    r"O:\NaNDA\Data\ses_demographics\sesdem_tract_2008-2017\datasets\workfiles_received\nanda_ses_tract_2008-2017_03.dta",
    columns=['tract_fips10', 'aland10', 'totpop13_17'])
densityPath = r'C:\Users\maolee\Documents\ArcGIS_kernelDensity\Store_Density\new_excel_density'
rootPath_for_tmpFiles = r"D:\ArcGIS_kernelDensity\Store_Density"
allUSA_CTracts3832nozero = rootPath_for_tmpFiles + r"\regardsParticipants_aft_allUSA_CTracts3832nozero.shp"
tractsAllFile = rootPath_for_tmpFiles + r"\allUSA_CTracts_3832.shp"
# ************* Define set of years to be used for computation
years = ['2000','2001','2002','2003','2004','2005','2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016']
schoolType = ['public']
ccd = pd.read_csv(r"C:\Users\maolee\Downloads\schools_ccd_directory (1).csv")

# *****************************************************************************************************************************************
# ****************************** main loop for computation
# *** set the main loops to compute for all NAICS codes and all years
tract = gpd.read_file(tractsAllFile)
for curSchoolType in schoolType:
    Tablelist = pd.DataFrame()
    for curYear in years:
        print(str(curYear) + " " + str(curSchoolType))
        # filter the data that have actual lat/lon
        data = ccd[(ccd['year']==int(curYear))&(ccd['latitude']!=-2)&(ccd['latitude']<90)&(ccd['longitude']!=-2)&(ccd['longitude']>-180)]
        school = data[['ncessch_num','latitude','longitude']]
        # Transform school data to geodataframe
        schoolGeo = gpd.GeoDataFrame(school, geometry=gpd.points_from_xy(school['longitude'],
                                                                         school['latitude']))
        schoolGeo.rename(columns={'geometry': 'coord'}, inplace=True)
        school = schoolGeo
        school1 = gpd.GeoDataFrame(school, geometry='coord')

        # Update the CRS  (coordinate reference system) for the establishment file, use an existent crs=4326 from a file, and then convert it to  crs=3832  (distance units as meters)
        # Also, create temporary files for processing
        crs = pycrs.load.from_file(r"C:\Users\maolee\Documents\ArcGIS_kernelDensity\Store_Density\allUSA_CTracts_4326.prj")
        crs_info = crs.to_proj4()
        school1.crs = crs_info
        school1.to_crs(tract.crs,inplace=True)
        after_join = gpd.sjoin(school1, tract, how='left', op="intersects")
        count = pd.DataFrame(after_join)
        count = count.groupby('GEOID').count()
        count = count.reset_index()
        count['year'] = int(curYear)
        count = count[['GEOID','ncessch_num', 'year']]
        Tablelist = Tablelist.append(count)
        if Tablelist.GEOID.count() == 0:
            print('error')
        else:
            print('Good Job!')
    tract1 = pd.DataFrame(tract)
    tract1 = tract1[['GEOID']]
    cleaned_NetData = Tablelist.pivot(index='GEOID', columns='year', values='ncessch_num')
    cleaned_NetData.index = cleaned_NetData.index.astype(str).str.zfill(11)
    pop.tract_fips10 = pop.tract_fips10.astype(str).str.zfill(11)
    tract_join = tract1.join(cleaned_NetData, on='GEOID')
    tract_join.rename(columns={"GEOID":"tract_fips10"}, inplace=True)
    pop_clean = tract_join.merge(pop,how='left', on='tract_fips10').merge(pop1, how='left',on='tract_fips10')
    pop_clean.fillna(0, inplace=True)
    pop_clean['Denstiy_00'] = pop_clean[2000] / pop_clean['totpop00']
    pop_clean['Denstiy_01'] = pop_clean[2001] / pop_clean['totpop01']
    pop_clean['Denstiy_02'] = pop_clean[2002] / pop_clean['totpop02']
    pop_clean['Denstiy_03'] = pop_clean[2003] / pop_clean['totpop03']
    pop_clean['Denstiy_04'] = pop_clean[2004] / pop_clean['totpop04']
    pop_clean['Denstiy_05'] = pop_clean[2005] / pop_clean['totpop05']
    pop_clean['Denstiy_06'] = pop_clean[2006] / pop_clean['totpop06']
    pop_clean['Denstiy_07'] = pop_clean[2007] / pop_clean['totpop07']
    pop_clean['Denstiy_08'] = pop_clean[2008] / pop_clean['totpop08']
    pop_clean['Denstiy_09'] = pop_clean[2009] / pop_clean['totpop09']
    pop_clean['Denstiy_10'] = pop_clean[2010] / pop_clean['totpop10']
    pop_clean['Denstiy_11'] = pop_clean[2011] / pop_clean['totpop10']
    pop_clean['Denstiy_12'] = pop_clean[2012] / pop_clean['totpop10']
    pop_clean['Denstiy_13'] = pop_clean[2013] / pop_clean['totpop13_17']
    pop_clean['Denstiy_14'] = pop_clean[2014] / pop_clean['totpop13_17']
    pop_clean['Denstiy_15'] = pop_clean[2015] / pop_clean['totpop13_17']
    pop_clean['Denstiy_16'] = pop_clean[2016] / pop_clean['totpop13_17']
    pop_clean[str(curSchoolType)+'_area_density_00'] = pop_clean[2000] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_01'] = pop_clean[2001] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_02'] = pop_clean[2002] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_03'] = pop_clean[2003] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_04'] = pop_clean[2004] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_05'] = pop_clean[2005] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_06'] = pop_clean[2006] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_07'] = pop_clean[2007] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_08'] = pop_clean[2008] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_09'] = pop_clean[2009] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_10'] = pop_clean[2010] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_11'] = pop_clean[2011] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_12'] = pop_clean[2012] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_13'] = pop_clean[2013] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_14'] = pop_clean[2014] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_15'] = pop_clean[2015] / pop_clean['aland10']
    pop_clean[str(curSchoolType)+'_area_density_16'] = pop_clean[2016] / pop_clean['aland10']
    pop_clean = pop_clean.round(
        {'totpop00': 3,'totpop01': 3,'totpop02': 3,'totpop03': 3,'totpop04': 3,'totpop05': 3,'totpop06': 3, 'totpop07': 3, 'totpop08': 3, 'totpop09': 3, 'totpop10': 3,'totpop13_17':3,
         'Denstiy_00': 6, 'Denstiy_01': 6, 'Denstiy_02': 6,'Denstiy_03': 6,'Denstiy_04': 6,'Denstiy_05': 6,'Denstiy_06': 6, 'Denstiy_07': 6,
         'Denstiy_08': 6, 'Denstiy_09': 6, 'Denstiy_10': 6, 'Denstiy_11': 6, 'Denstiy_12': 6, 'Denstiy_13': 6,
         'Denstiy_14': 6, 'Denstiy_15': 6,'Density_16':6})
    pop_clean = pop_clean.rename(
        columns={"Unnamed: 0": "Index", "GEOID": "tract_fips10", 2000: str(curSchoolType) + "_Count_00", 2001: str(curSchoolType) + "_Count_01", 2002: str(curSchoolType) + "_Count_02", 2003: str(curSchoolType) + "_Count_03", 2004: str(curSchoolType) + "_Count_04", 2005: str(curSchoolType) + "_Count_05", 2006: str(curSchoolType) + "_Count_06",
                 2007: str(curSchoolType) + "_Count_07", 2008: str(curSchoolType) + "_Count_08",
                 2009: str(curSchoolType) + "_Count_09", 2010: str(curSchoolType) + "_Count_10",
                 2011: str(curSchoolType) + "_Count_11", 2012: str(curSchoolType) + "_Count_12",
                 2013: str(curSchoolType) + "_Count_13", 2014: str(curSchoolType) + "_Count_14",
                 2015: str(curSchoolType) + "_Count_15", 2016: str(curSchoolType) + "_Count_16",
                 'Denstiy_00': str(curSchoolType) + "_density_00", 'Denstiy_01': str(curSchoolType) + "_density_01",
                 'Denstiy_02': str(curSchoolType) + "_density_02", 'Denstiy_03': str(curSchoolType) + "_density_03",
                 'Denstiy_04': str(curSchoolType) + "_density_04", 'Denstiy_05': str(curSchoolType) + "_density_05",
                 'Denstiy_06': str(curSchoolType) + "_density_06",
                 'Denstiy_07': str(curSchoolType) + "_density_07", 'Denstiy_08': str(curSchoolType) + "_density_08",
                 'Denstiy_09': str(curSchoolType) + "_density_09", 'Denstiy_10': str(curSchoolType) + "_density_10",
                 'Denstiy_11': str(curSchoolType) + "_density_11", 'Denstiy_12': str(curSchoolType) + "_density_12",
                 'Denstiy_13': str(curSchoolType) + "_density_13", 'Denstiy_14': str(curSchoolType) + "_density_14",
                 'Denstiy_15': str(curSchoolType) + "_density_15",'Denstiy_16':str(curSchoolType) + "_density_16"})
    pop_clean.replace(np.inf, np.nan, inplace=True)
    pop_clean["tract_fips10"] = pop_clean["tract_fips10"].astype(str).str.zfill(11)
    pop_clean.to_excel(densityPath + '\\' + curSchoolType + '_density.xlsx', header=True, index=False)
