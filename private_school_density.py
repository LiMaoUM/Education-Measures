# Description: Calculation the raw count of private school per census tract.
# Data Source: NCES private school survey(PSS)
# Methodology: Geopandas spatial join
# @author: maolee
# ******************************************************
import pandas as pd
import geopandas as gpd
import pycrs
import numpy as np

# import data: population
pop = pd.read_excel(r"C:\Users\maolee\Documents\ArcGIS_kernelDensity\nanda_ses2000-2010_02P.xlsx",
                    usecols=['tract_fips10', 'totpop05', 'totpop06', 'totpop07', 'totpop08', 'totpop09', 'totpop10'])
pop1 = pd.read_stata(
    r"O:\NaNDA\Data\ses_demographics\sesdem_tract_2008-2017\datasets\workfiles_received\nanda_ses_tract_2008-2017_03.dta",
    columns=['tract_fips10', 'aland10', 'totpop13_17'])
# Set up the working directory and files
densityPath = r'C:\Users\maolee\Documents\ArcGIS_kernelDensity\Store_Density\new_excel_density'
rootPath_for_tmpFiles = r"D:\ArcGIS_kernelDensity\Store_Density"
tractsAllFile = rootPath_for_tmpFiles + r"\allUSA_CTracts_3832.shp"

# Survey years available
years = ['0506', '0708', '0910', '1112', '1314', '1516', '1718']
schoolType = ['private_school']
tract = gpd.read_file(tractsAllFile)
for curSchoolType in schoolType:
    Tablelist = pd.DataFrame()
    for curYear in years:
        # Set up the condition. The reason is for different year we have different data type and encoding format.
        if curYear in ['0506', '0708']:
            data = pd.read_csv(
                r"D:\School\private_school\Data Source\TXT_PSS" + curYear + "\\PSS" + curYear + "_PU.txt", sep='\t',
                usecols=['latitude', 'longitude', 'ppin'])
        elif curYear == '0910':
            data = pd.read_csv(
                r"D:\School\private_school\Data Source\TXT_PSS" + curYear + "\\PSS" + curYear + "_PU.txt", sep='\t',
                usecols=['latitude10', 'longitude10', 'ppin'], encoding='latin-1')
            data.rename(columns={'latitude10': 'latitude', "longitude10": 'longitude'}, inplace=True)
        elif curYear == '1112':
            data = pd.read_csv(r"D:\School\private_school\Data Source\pss1112_pu_txt\pss1112_pu.txt",
                               sep='\t', usecols=['latitude12', 'longitude12', 'ppin'])
            data.rename(columns={'latitude12': 'latitude', "longitude12": 'longitude'}, inplace=True)
        elif curYear == '1314':
            data = pd.read_csv(r"D:\School\private_school\Data Source\pss" + curYear + "_pu_csv\\pss" + curYear + "_pu.csv",
                               usecols=['LATITUDE14', 'LONGITUDE14', 'PPIN'])
            data.rename(columns={'LATITUDE14': 'latitude', "LONGITUDE14": 'longitude','PPIN':'ppin'}, inplace=True)
        elif curYear == '1516':
            data = pd.read_csv(r"D:\School\private_school\Data Source\pss" + curYear + "_pu_csv\\pss" + curYear + "_pu.csv",
                               usecols=['latitude16', 'longitude16', 'ppin'])
            data.rename(columns={'latitude16': 'latitude', "longitude16": 'longitude'}, inplace=True)
        else:
            data = pd.read_csv(r"D:\School\private_school\Data Source\pss" + curYear + "_pu_csv\\pss" + curYear + "_pu.csv",
                               usecols=['LATITUDE18', 'LONGITUDE18', 'PPIN'])
            data.rename(columns={'LATITUDE18': 'latitude', "LONGITUDE18": 'longitude','PPIN':'ppin'}, inplace=True)

        data = data[(data['latitude'] != "N") & (data['longitude'] != "N")]  # Make sure the school actually have the geoinformation.
        data['latitude'] = data['latitude'].astype(float)
        data['longitude'] = data['longitude'].astype(float)
        # ** set vars to process the two las digits of the years to match the names of the columns in NETS data:
        # NAICS+YEAR
        schoolGeo = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data['longitude'],
                                                                       data['latitude']))
        schoolGeo.rename(columns={'geometry': 'coord'}, inplace=True)
        school = schoolGeo
        school1 = gpd.GeoDataFrame(school, geometry='coord')
        # ********************************************************************************** ********** Update the
        # CRS  (coordinate reference system) for the establishment file, use an existent crs=4326 from a file,
        # and then convert it to  crs=3832  (distance units as meters) ******** Also, create temporary files for
        # processing
        crs = pycrs.load.from_file(
            r"C:\Users\maolee\Documents\ArcGIS_kernelDensity\Store_Density\allUSA_CTracts_4326.prj")
        crs_info = crs.to_proj4()
        school1.crs = crs_info
        school1.to_crs(tract.crs, inplace=True)
        after_join = gpd.sjoin(school1, tract, how='left', op="intersects")
        count = pd.DataFrame(after_join)
        count = count.groupby('GEOID').count()
        count = count.reset_index()
        count['year'] = int(curYear)
        count = count[['GEOID', 'ppin', 'year']]
        Tablelist = Tablelist.append(count)
        if Tablelist.GEOID.count() == 0:
            print('error')
        else:
            print('Good Job!')
    tract1 = pd.DataFrame(tract) # Transform the geodataframe to dataframe for merge data later.
    tract1 = tract1[['GEOID']]
    # Transform the format from long to wide
    cleaned_NetData = Tablelist.pivot(index='GEOID', columns='year', values='ppin')
    cleaned_NetData.index = cleaned_NetData.index.astype(str).str.zfill(11)
    pop.tract_fips10 = pop.tract_fips10.astype(str).str.zfill(11)
    tract_join = tract1.join(cleaned_NetData, on='GEOID')
    tract_join.rename(columns={"GEOID": "tract_fips10"}, inplace=True)
    pop_clean = tract_join.merge(pop, how='left', on='tract_fips10').merge(pop1, how='left', on='tract_fips10') # Merge the data together.
    pop_clean.fillna(0, inplace=True)
    # Calculating all the measures we would like to have.
    pop_clean['private_school_density_0506'] = pop_clean[506] / pop_clean['totpop06']
    pop_clean['private_school_density_0708'] = pop_clean[708] / pop_clean['totpop08']
    pop_clean['private_school_density_0910'] = pop_clean[910] / pop_clean['totpop10']
    pop_clean['private_school_density_1112'] = pop_clean[1112] / pop_clean['totpop10']
    pop_clean['private_school_density_1314'] = pop_clean[1314] / pop_clean['totpop13_17']
    pop_clean['private_school_density_1516'] = pop_clean[1516] / pop_clean['totpop13_17']
    pop_clean['private_school_density_1718'] = pop_clean[1718] / pop_clean['totpop13_17']
    pop_clean[str(curSchoolType) + '_area_density_0506'] = pop_clean[506] / pop_clean['aland10']
    pop_clean[str(curSchoolType) + '_area_density_0708'] = pop_clean[708] / pop_clean['aland10']
    pop_clean[str(curSchoolType) + '_area_density_0910'] = pop_clean[910] / pop_clean['aland10']
    pop_clean[str(curSchoolType) + '_area_density_1112'] = pop_clean[1112] / pop_clean['aland10']
    pop_clean[str(curSchoolType) + '_area_density_1314'] = pop_clean[1314] / pop_clean['aland10']
    pop_clean[str(curSchoolType) + '_area_density_1516'] = pop_clean[1516] / pop_clean['aland10']
    pop_clean[str(curSchoolType) + '_area_density_1718'] = pop_clean[1718] / pop_clean['aland10']
    pop_clean = pop_clean.rename(
        columns={"Unnamed: 0": "Index", "GEOID": "tract_fips10", 506: str(curSchoolType) + "_Count_0506",
                 708: str(curSchoolType) + "_Count_0708", 910: str(curSchoolType) + "_Count_0910",
                 1112: str(curSchoolType) + "_Count_1112", 1314: str(curSchoolType) + "_Count_1314",
                 1516: str(curSchoolType) + "_Count_1516", 1718: str(curSchoolType) + "_Count_1718"})
    pop_clean.replace(np.inf, np.nan, inplace=True)
    pop_clean["tract_fips10"] = pop_clean["tract_fips10"].astype(str).str.zfill(11)
    pop_clean.to_excel(densityPath + '\\' + curSchoolType + '_density.xlsx', header=True, index=False)
