import pandas as pd

# Define the List of all columns in the file (GDELT has 61, we list the ones we want)
# Note: Since there is no header, we must provide these names
COLUMN_NAMES = [
    "GlobalEventID",
    "Day",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1Type1Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "DATEADDED",
    "SOURCEURL",
]

# Define the Dtypes (The Data Contract)
# We use 'string' for EventCodes to keep leading zeros as warned in the manual [cite: 82, 88, 93]
GDELT_DTYPES = {
    "GlobalEventID": "Int64",
    "Day": "Int64",
    "FractionDate": "float64",
    "Actor1Code": "string",
    "Actor1Name": "string",
    "EventCode": "string",
    "GoldsteinScale": "float64",
    "AvgTone": "float64",
    "DATEADDED": "Int64",
    "SOURCEURL": "string",
}


def ingest_data():
    # Placeholder for the logic we will write next
    pass
