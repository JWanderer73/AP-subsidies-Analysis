from pathlib import Path
import pandas as pd

code_to_state = {     #dictionary of state codes to state, yes there are gaps
    "01": "Alabama",
    "02": "Alaska",
    "04": "Arizona",
    "05": "Arkansas",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "13": "Georgia",
    "15": "Hawaii",
    "16": "Idaho",
    "17": "Illinois",
    "18": "Indiana",
    "19": "Iowa",
    "20": "Kansas",
    "21": "Kentucky",
    "22": "Louisiana",
    "23": "Maine",
    "24": "Maryland",
    "25": "Massachusetts",
    "26": "Michigan",
    "27": "Minnesota",
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "37": "North Carolina",
    "38": "North Dakota",
    "39": "Ohio",
    "40": "Oklahoma",
    "41": "Oregon",
    "42": "Pennsylvania",
    "44": "Rhode Island",
    "45": "South Carolina",
    "46": "South Dakota",
    "47": "Tennessee",
    "48": "Texas",
    "49": "Utah",
    "50": "Vermont",
    "51": "Virginia",
    "53": "Washington",
    "54": "West Virginia",
    "55": "Wisconsin",
    "56": "Wyoming",
    "57": "Unknown",
    "58": "US Total"
}


# migration data/state demographics, REMEMBER TO USE EVEN YEARS, AS ODD YEARS HAVE INCOMPLETE DATA
# also note that data is a sample fromm the school and must be scaled to the school's population (ratios should be the same, though)

def crushMigrationData(year: int):
    """
    This function takes in a yeasr, then looks for the CSV from the extracted folder in datasets/ipeds/migration. The file should be have the name ef{year}c.csv, and is within the downloaded folder from IPEDS website.
    Parameters:
    year: an integer year
    """
    
    ROOT = Path.cwd()
    DATA_DIR = ROOT / "datasets"
    IPEDS_DIR = DATA_DIR / "ipeds"
    MIGRATION_DIR = IPEDS_DIR / "migration"

    # years prior to 2004 have a different organization scheme
    if (year < 2004):
        print(f"year: {2004} unsupported")
    else: 
        try: 
            migration = pd.read_csv(MIGRATION_DIR / f"ef{year}c.csv") 
        except FileNotFoundError:
            raise FileNotFoundError(f"Missing migration CSV: ef{year}c.csv")
    

    # making folder to put formed csvs in if necessary

    OUTPUT_DIR = IPEDS_DIR / "processed"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    migration.columns = migration.columns.str.upper()

    # getting unique IDs of schools in migration data
    unique_schools = migration["UNITID"].unique()


    migrationIDs = migration["UNITID"] # school ID
    migrationState = migration["EFCSTATE"] # state code
    migationStudents = migration["EFRES01"] # num first-time undergrad from that state

    labels = ["UNITID"] + [code_to_state.get(str(i).zfill(2), "DEFAULT") for i in range(1, 59)] + ["GRAND TOTAL"]
    labels


    # loop through each row in migration to assemble an array containing state makeups for each unique school in migration data, then appending it to a dataframe (OPTIMIZE IF TOO SLOW)

    merged = []   # holds rows 

    currIdx = 0         # current row in migration data

    TOTAL_LABEL_COUNT = 60              # state codes go from 1-58 (with gaps) + 1 for all US studenmts + 1 for total students
    LAST_IDX = TOTAL_LABEL_COUNT - 1    # last idx of row
    GRAND_TOTAL_STATE_CODE = 99         # state code for grand total

    for schoolID in unique_schools:
        row = migration.iloc[currIdx]
        outputRow = [float("nan")] * TOTAL_LABEL_COUNT   
        outputRow[0] = schoolID

        while (currIdx < len(migrationIDs) and schoolID == migrationIDs[currIdx]):
            if (migrationState[currIdx] < LAST_IDX):                                         
                outputRow[migrationState[currIdx]] = migationStudents[currIdx]
            elif (migrationState[currIdx] == GRAND_TOTAL_STATE_CODE):
                outputRow[LAST_IDX] = migationStudents[currIdx]

            currIdx += 1

        merged.append(outputRow)

        continue

    # create dataframe from newly created rows, drop columns without values, replace null w/ 0

    school_states = pd.DataFrame(merged, columns=labels)
    school_states = school_states.dropna(axis=1, how="all")
    school_states = school_states.fillna(0)

    # put files in it
    out_file = OUTPUT_DIR / f"{year}_crushed_migration.csv"
    school_states.to_csv(out_file, index=False)

    print(f"crushed migration data saved to {out_file}")


for year in range(2004, 2025):
    if (year % 2 == 0):
        print(year)
        crushMigrationData(year)
    