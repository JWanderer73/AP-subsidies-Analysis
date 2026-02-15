#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from pathlib import Path
from crushing_migration_data import crushMigrationData

# In[52]:


# goal: take state college data, multiply it by outcome measures of college, sum or average by column, and we have a measure for the state
# btw, realized that you can reindex using unitid and maybe have a way easier time. It could be worth doing, though it wouldn't significantly impact performance.


# In[ ]:


ROOT = Path.cwd()
DATA_DIR = ROOT / "datasets"
IPEDS_DIR = DATA_DIR / "ipeds"
OUTCOMES_DIR = IPEDS_DIR / "outcomes"
# MIGRATION_DIR = IPEDS_DIR / "migration"
PROCESSED_DIR = IPEDS_DIR / "processed"


def findOrCrushMigration(year: int) -> bool:
    """
    This function takes the year of the incoming class, then looks for the crushed migration data in processed. If it does not exist, it tries to crush it. 
    Parameters:
    year: an integer year
    Returns:
    boolean representing whether the file exists. 
    """

    try:
        crushedMigration = pd.read_csv(PROCESSED_DIR / f"{year}_crushed_migration.csv") 
    except FileNotFoundError:
        print(f"crushed data for {year} not found; attempting to create")
        try: 
            crushMigrationData(year)
        except FileNotFoundError:
            return False

    return True


            


# outcomes data should be the 4 YEAR GRADUATION RATE in the database EIGHT YEARS AFTER the class entered, due to how the data is structured. Ex. 2018 om 4-year describes the incoming class of 2010, graduating in 2014. 
# 

def scaleOutcomeMeasureGrad(year: int) -> pd.Series:
    """
    This function takes in the year of the incoming class and derives a Series containing the average 4-year graduation rates of the students in that class by state. It also converts this Series to a csv.
    Parameters:
    year: an integer year
    Returns:
    Series containing graduation rates of 50 states + DC + US student average + US school average
    """

    findOrCrushMigration(year)

    try: 
        outcomes = pd.read_csv(OUTCOMES_DIR / f"om{year+8}_rv.csv")
    except FileNotFoundError:
        print(f"om{year+8}_rv.csv not found; trying om{year+8}.csv")
        try: 
            outcomes = pd.read_csv(OUTCOMES_DIR / f"om{year+8}.csv") 
        except FileNotFoundError:
            raise FileNotFoundError(f"Missing outcomes CSV: om{year+8}.csv")

    # first time, full time 

    firstFullOutcomes = outcomes[outcomes["OMCHRT"] == 10].reset_index(drop=True)

    unitIds = firstFullOutcomes["UNITID"].reset_index(drop=True)

    # grad rate sanity check
    # print(firstFullOutcomes["OMAWDP4"].mean()) 

    gradPercent = pd.concat([firstFullOutcomes["UNITID"], firstFullOutcomes["OMAWDP4"]], axis = 1)


    # In[5]:


    # grab state makeups data
    migration_csv = PROCESSED_DIR / f"{year}_crushed_migration.csv"
    stateMakeup = pd.read_csv(migration_csv)
    stateMakeup


    # In[57]:


    # reduce data to fewest rows, so we only evaluate schools with both migration and outcome data
    reducedMakeup = stateMakeup[stateMakeup["UNITID"].isin(gradPercent["UNITID"])]
    reducedGradRate = gradPercent[gradPercent["UNITID"].isin(stateMakeup["UNITID"])]

    # row nums should be equal
    # print(reducedMakeup.shape)
    # print(reducedGradRate.shape)


    # In[59]:


    # scaling school population uniformly by school grad rate
    scaled = pd.DataFrame(reducedMakeup.iloc[:, 1:].apply(pd.to_numeric).values * (reducedGradRate.iloc[: , 1:].apply(pd.to_numeric, axis = 1).values / 100))
    scaled = scaled.reset_index(drop=True)
    print(scaled.shape)

    COL_LABELS = reducedMakeup.columns.tolist()[1:]
    scaled.columns = COL_LABELS

    scaled


    # In[73]:


    # divide the total graduating from a given state by the total incoming class size (4 years earlier) to get the average graduation rate from the state. 
    graduatingMakeup = scaled.sum()

    totalMakeup = reducedMakeup.drop(columns = "UNITID").sum()

    gradPercentByState = graduatingMakeup / totalMakeup



    # In[77]:

    print(f"writing to class {year} 4yr grad rate by state.csv")
    gradPercentByState.to_csv(PROCESSED_DIR / f"class {year} 4yr grad rate by state.csv", header=False)

    return gradPercentByState

for year in range(2010, 2017):
    if (year % 2 == 0):
        print(year)
        scaleOutcomeMeasureGrad(year)