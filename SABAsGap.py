# Description: Calculation of School neighborhood gap at the school level
# Author: Mao Li


# Load python library
import pandas as pd
import numpy as mp
import geopandas as gpd
# Read data from enrollment and race aggregation
SAB = pd.read_csv(r"C:\Users\maolee\Downloads\SAB_agg_15.csv")
school = pd.read_csv(r"D:\School\group_race\school_race_2015.csv")
school_16 = pd.read_csv(r"D:\School\group_race\school_race_2015.csv")
SAB_pov_agg = pd.read_csv(r"C:\Users\maolee\Downloads\SAB_pov_agg.csv")
ccd = pd.read_csv(r"D:\School\schools_ccd_directory.csv")
SABshapefile = gpd.read_file(r"D:\School\SABS_15_16_3832\SABS_1516_3832.shp")
#  Keep the primary school
SABshapefile['gslo'].replace({'PK': '-1', 'KG': '0','N':'9','UG':'0'}, inplace=True)

SABwithSchool = SAB.merge(school, how='left')
SABwithSchool.dropna(inplace=True)
# Calculating race school-neighborhood gap
SABwithSchool['nwhite_gap'] = (SABwithSchool['nwhite_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['White'] / SABwithSchool['race_total'])
SABwithSchool['nblack_gap'] = (SABwithSchool['nblack_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Black'] / SABwithSchool['race_total'])
SABwithSchool['namericanIndian_gap'] = (SABwithSchool['namericanIndian_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['American Indian or Alaska Native'] / SABwithSchool['race_total'])
SABwithSchool['nasian_gap'] = (SABwithSchool['nasian_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Asian'] / SABwithSchool['race_total'])
SABwithSchool['nnativeHawaiian_gap'] = (SABwithSchool['nnativeHawaiian_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Native Hawaiian or other Pacific Islander'] / SABwithSchool['race_total'])
SABwithSchool['ntwoOrMore_gap'] = (SABwithSchool['ntwoOrMore_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Two or more races'] / SABwithSchool['race_total'])

# Calculating poverty ratio school-neighborhood gap

ccd = ccd.loc[ccd['year'] == 2015,]
ccd = ccd[['ncessch', 'free_lunch', 'enrollment']]
ccd = ccd.loc[(ccd['free_lunch'] >= 0) & (ccd['enrollment'] >= 0),]
SAB_pov_agg = SAB_pov_agg.merge(ccd, how='left')
SAB_pov_agg.dropna(inplace=True)
SAB_pov_agg['povGap'] = (SAB_pov_agg['inTOpovL15_agg'] / SAB_pov_agg['totpop_agg']) - (
            SAB_pov_agg['free_lunch'] / SAB_pov_agg['enrollment'])
SAB = SAB_pov_agg.merge(SABwithSchool,how='outer')
SABshapefile = SABshapefile[['ncessch','gslo']]
SABshapefile['ncessch'] = SABshapefile['ncessch'].astype('int64')
SAB = SAB.merge(SABshapefile,how='left')
SAB['gslo'] = SAB['gslo'].astype(int)
SAB = SAB.loc[~(SAB['gslo'] > 4),]
SAB.drop(columns=['gslo','Unnamed: 0'],inplace=True)
SAB.to_csv(r'D:\School\Sample\SABSAsGap_2015.csv', index=False)


SAB = pd.read_csv(r"C:\Users\maolee\Downloads\SAB_agg_15.csv")
school = pd.read_csv(r"D:\School\group_race\school_race_2015.csv")
school_16 = pd.read_csv(r"D:\School\group_race\school_race_2015.csv")
SAB_pov_agg = pd.read_csv(r"C:\Users\maolee\Downloads\SAB_pov_agg.csv")
ccd = pd.read_csv(r"D:\School\schools_ccd_directory.csv")
SABshapefile = gpd.read_file(r"D:\School\SABS_15_16_3832\SABS_1516_3832.shp")
#  Keep the primary school
SABshapefile['gslo'].replace({'PK': '-1', 'KG': '0','N':'9','UG':'0'}, inplace=True)

SABwithSchool = SAB.merge(school, how='left')
SABwithSchool.dropna(inplace=True)

SABwithSchool = SAB.merge(school_16,  how='left')

SABwithSchool.dropna(inplace=True)

# Calculating race school-neighborhood gap
SABwithSchool['nwhite_gap'] = (SABwithSchool['nwhite_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['White'] / SABwithSchool['race_total'])
SABwithSchool['nblack_gap'] = (SABwithSchool['nblack_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Black'] / SABwithSchool['race_total'])
SABwithSchool['namericanIndian_gap'] = (SABwithSchool['namericanIndian_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['American Indian or Alaska Native'] / SABwithSchool['race_total'])
SABwithSchool['nasian_gap'] = (SABwithSchool['nasian_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Asian'] / SABwithSchool['race_total'])
SABwithSchool['nnativeHawaiian_gap'] = (SABwithSchool['nnativeHawaiian_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Native Hawaiian or other Pacific Islander'] / SABwithSchool['race_total'])
SABwithSchool['ntwoOrMore_gap'] = (SABwithSchool['ntwoOrMore_agg'] / SABwithSchool['youngTotal_agg']) - (
        SABwithSchool['Two or more races'] / SABwithSchool['race_total'])

# Calculating poverty ratio school-neighborhood gap
ccd = pd.read_csv(r"D:\School\schools_ccd_directory.csv")
ccd = ccd.loc[ccd['year'] == 2016,]
ccd = ccd[['ncessch', 'free_lunch', 'enrollment']]
ccd = ccd.loc[(ccd['free_lunch'] >= 0) & (ccd['enrollment'] >= 0),]
SAB_pov_agg = SAB_pov_agg.merge(ccd, how='left')
SAB_pov_agg.dropna(inplace=True)
SAB_pov_agg['povGap'] = (SAB_pov_agg['inTOpovL15_agg'] / SAB_pov_agg['totpop_agg']) - (
            SAB_pov_agg['free_lunch'] / SAB_pov_agg['enrollment'])
SAB = SAB_pov_agg.merge(SABwithSchool,how='outer')
SABshapefile = SABshapefile[['ncessch','gslo']]
SABshapefile['ncessch'] = SABshapefile['ncessch'].astype('int64')
SAB = SAB.merge(SABshapefile,how='left')
SAB['gslo'] = SAB['gslo'].astype(int)
SAB = SAB.loc[~(SAB['gslo'] > 4),]
SAB.drop(columns=['gslo','Unnamed: 0'],inplace=True)
SAB.to_csv(r'D:\School\Sample\SABSAsGap_2016.csv', index=False)
