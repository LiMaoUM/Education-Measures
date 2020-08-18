# -*- coding: utf-8 -*-
"""
Created on Wed May  6 14:03:45 2020
Harmonizing the ACS data with census data for Race and Poverty ratio.
The data will use for calculating gap measures
@author: maolee
"""
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
# Read data from NHGIS
crosswalk = pd.read_csv(r"D:\School\ACS_poverty_raceBYage_censusTracts_datasets\nhgis0032_shapefile_tl2010_block_ALL_2010.csv")
acs_block = pd.read_csv(r"D:\School\ACS_poverty_raceBYage_censusTracts_datasets\incomeTOpovertyRatio_BY_tract_ACS\original\cBlockGroup_data_forCrosswalk\nhgis0049_ds152_2000_blck_grp.csv")
acs2000 = pd.read_csv(r"D:\School\ACS_poverty_raceBYage_censusTracts_datasets\incomeTOpovertyRatio_BY_tract_ACS\original\nhgis0028_ds151_2000_tract.csv",encoding='latin-1')
acs2010 = pd.read_csv(r"D:\School\ACS_poverty_raceBYage_censusTracts_datasets\incomeTOpovertyRatio_BY_tract_ACS\original\nhgis0027_ds191_2008_2012_tract.csv",encoding='latin-1')
acs2013_17 = pd.read_csv(r"D:\School\ACS_poverty_raceBYage_censusTracts_datasets\incomeTOpovertyRatio_BY_tract_ACS\original\nhgis0027_ds233_2013_2017_tract.csv",encoding='latin-1')
# Pick the variables that we are interested in
acs2000 = acs2000[['GISJOIN','GN8001','GN8002','GN8003','GN8004','GN8005','GN8006','GN8007','GN8008','GN8009']]
acs2013_17 = acs2013_17[['GISJOIN','AH1JE001','AH1JE002','AH1JE003','AH1JE004','AH1JE005','AH1JE006','AH1JE007','AH1JE008']]
acs2010 = acs2010[['GISJOIN','QUVE001','QUVE002','QUVE003','QUVE004','QUVE005','QUVE006','QUVE007','QUVE008']]
# ratio lower than 1.25 regards as low_income
acs2000['low_income_00'] = acs2000['GN8001'] + acs2000['GN8002'] + acs2000['GN8003'] + acs2000['GN8004'] + acs2000['GN8005']
acs2010['low_income_0812'] =  acs2010['QUVE002'] + acs2010['QUVE003'] + acs2010['QUVE004'] + acs2010['QUVE005']
acs2013_17['low_income_1317'] = acs2013_17['AH1JE002'] + acs2013_17['AH1JE003'] + acs2013_17['AH1JE004'] + acs2013_17['AH1JE005']

acs2000['totpop00'] = acs2000['GN8001'] + acs2000['GN8002'] + acs2000['GN8003'] + acs2000['GN8004'] + acs2000['GN8005'] + acs2000['GN8006']+ acs2000['GN8007']+ acs2000['GN8008']+ acs2000['GN8009']
acs2010['totpop0812'] = acs2010['QUVE002'] + acs2010['QUVE003'] + acs2010['QUVE004'] + acs2010['QUVE005'] + acs2010['QUVE006']+ acs2010['QUVE007']+ acs2010['QUVE008']
acs2013_17['totpop1317'] = acs2013_17['AH1JE002'] + acs2013_17['AH1JE003'] + acs2013_17['AH1JE004'] + acs2013_17['AH1JE005'] + acs2013_17['AH1JE006']+ acs2013_17['AH1JE007']+ acs2013_17['AH1JE008']

acs2000_join = acs2000[['GISJOIN','low_income_00','totpop00']]
acs2010_join = acs2010[['GISJOIN','low_income_0812','totpop0812']]
acs2013_17_join = acs2013_17[['GISJOIN','low_income_1317','totpop1317']]
