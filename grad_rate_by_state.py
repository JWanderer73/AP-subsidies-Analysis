#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from pathlib import Path
from crushing_migration_data import crushMigrationData

# In[ ]:


# copy of outcome_measures_by_state but it uses the specific graduation rates data on IPEDS instead of the outcome measures data


# In[3]:


ROOT = Path.cwd()
DATA_DIR = ROOT / "datasets"
IPEDS_DIR = DATA_DIR / "ipeds"
GRAD_DIR = IPEDS_DIR / "graduation"
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


# grad rate data should be the 4 YEAR GRADUATION RATE in the database SIX YEARS AFTER the class entered, due to how the data is structured. Ex. 2022 grad rate data describes the incoming class of 2016 (for 4-year), graduating in 2020. 
def scaleGradRates(year: int) -> pd.Series:

    findOrCrushMigration(year)

    try: 
        grad = pd.read_csv(GRAD_DIR / f"gr{year+6}_rv.csv")
    except FileNotFoundError:
        print(f"gr{year+6}_rv.csv not found; trying gr{year+6}.csv")
        try: 
            grad = pd.read_csv(GRAD_DIR / f"gr{year+6}.csv") 
        except FileNotFoundError:
            raise FileNotFoundError(f"Missing outcomes CSV: gr{year+6}.csv")

    

    # bachelors or equivalent seeking cohort size (denominator)
    totalCohort = grad[grad["GRTYPE"] == 8].reset_index(drop=True)
    totalCohort = pd.concat([totalCohort["UNITID"], totalCohort["GRTOTLT"]], axis = 1)

    # Completers of bachelors in <= 4 yrs (numerator)
    graduatedCohort = grad[grad["GRTYPE"] == 13].reset_index(drop = True)
    graduatedCohort = pd.concat([graduatedCohort["UNITID"], graduatedCohort["GRTOTLT"]], axis = 1)

    # merging data and computing graduation rate
    mergedCohort = pd.merge(graduatedCohort, totalCohort, on="UNITID", how="inner")
    mergedCohort["GRTOTLT_x"] = mergedCohort["GRTOTLT_x"] / mergedCohort["GRTOTLT_y"]
    mergedCohort = mergedCohort.iloc[:, 0:2]
    mergedCohort.columns = ["UNITID", "GRADRATE"]


    # In[26]:


    # grab state makeups data
    migration_csv = PROCESSED_DIR / f"{year}_crushed_migration.csv"
    stateMakeup = pd.read_csv(migration_csv)


    # In[28]:


    # reduce data to fewest rows, so we only evaluate schools with both migration and outcome data
    stateAndGradRate = pd.merge(stateMakeup, mergedCohort, on="UNITID", how = "inner")


    # In[52]:


    # scaling school population uniformly by school grad rate
    scaled = pd.DataFrame(stateAndGradRate.iloc[:, 1:-1].apply(pd.to_numeric).values * (stateAndGradRate.iloc[: , -1:].apply(pd.to_numeric, axis = 1).values))
    scaled.columns = stateAndGradRate.columns[1:-1]


    # In[56]:


    # divide the total graduating from a given state by the total incoming class size (4 years earlier) to get the average graduation rate from the state. 
    graduatingMakeup = scaled.sum()

    totalMakeup = stateMakeup.drop(columns = "UNITID").sum()

    gradPercentByState = graduatingMakeup / totalMakeup


    # In[58]:

    print(f"writing to class_{year}_4yr_gradrate_by_state.csv")
    gradPercentByState.to_csv(PROCESSED_DIR / f"class_{year}_4yr_gradrate_by_state.csv", header=False)
    return

for year in range(2004, 2017):
    print(year)
    if (year % 2 ==0):
        scaleGradRates(year)
