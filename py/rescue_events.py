import pandas as pd
from datetime import datetime


def load(original_path, cache_path):
    try:
        print("Loading Data")
        events = pd.read_csv(cache_path, index_col=0, parse_dates=["Aeg "])
    except OSError:
        events = None
    else:
        setCategorical(events)

    if events is None:
        print("No cached data found")
        print("Loading from xlsx - slow...")
        events = load_from_xls(original_path)
        events.to_csv(cache_path, encoding='utf-8')
    clean(events)
    print("Data Loaded!")
    print("{} events loaded".format(len(events.index)))


    missing_datetime=events.Aeg.isnull()
    missing_datetime_count = missing_datetime.sum()
    if(missing_datetime_count>0):
        print("{} rows have missing datetime - removing".format(missing_datetime_count))
        events = events[~events.Aeg.isnull()]
    print("")
    return events


def load_from_xls(path):
    df5 = pd.read_excel(
        io=path,
        sheetname="2015",
        index_col=0,
        parse_cols="A:E",
        verbose=True
    )

    df4 = pd.read_excel(
        io=path,
        sheetname="2014",
        index_col=0,
        parse_cols="A:E",
        names = df5.columns,
        verbose=True
    )
    pairs =[
        ("Väljakutse liik SOS", "Sündmus"),
        ("Linn,vald","Linna osa või vald")
    ]
    for new,old in pairs:
        df4[new] = df4[old]
        df4.drop(old, axis=1, inplace=True)

    df3 = pd.read_excel(
        io=path,
        sheetname="2013",
        index_col=0,
        parse_cols="A,C,G,J,K",
        verbose=True
    )

    df2 = pd.read_excel(
        io=path,
        sheetname="2012",
        index_col=0,
        parse_cols="A,B,F,I,J",
        verbose=True
    )

    df1 = pd.read_excel(
        io=path,
        sheetname="2011",
        index_col=0,
        parse_cols="A,B,F,I,J",
        verbose=True
    )
    df321 = df3.append(df2).append(df1)

    pairs =[("Aeg ", "VK aeg")]
    for new,old in pairs:
        df321[new] = df321[old]
        df321.drop(old, axis=1, inplace=True)

    df = df5.append(df4).append(df321)
    return df



def clean(df):
    #Merge renamed categories:
    replacements = [
        ("INFRA-ELEKTRIVÕRKUDE AVARII","INFRA - ELEKTRIVÕRKUDE AVARII"),
        ("INFRA-GAASIAVARII","INFRA - GAASIAVARII"),
        ("INFRA-KOMMUNAALAVARII","INFRA - KOMMUNAALAVARII"),
        ("DEM-LÕHKEKEHA","DEM - LÕHKEKEHA")
    ]
    for rep in replacements:
        df["Väljakutse liik SOS"] = df["Väljakutse liik SOS"].str.replace(rep[0],rep[1])
    
    # Clean data by removing leading and trailing whitespaces
    df["Linn,vald"] = df["Linn,vald"].str.strip()
    df["Linn,vald"] = df["Linn,vald"].str.replace("Vändra vald \(alev\)", "Vändra vald")
    df["Linn,vald"] = df["Linn,vald"].str.replace("Vändra alev", "Vändra vald")

    datetime_format = '%d.%m.%Y %H:%M:%S'

    setCategorical(df)

    date_as_datetime = (df["Aeg "].apply(type) == datetime)
    if not df[date_as_datetime].empty:
        df.ix[date_as_datetime, 'Aeg '] = df["Aeg "][date_as_datetime].dt.strftime(datetime_format)

    df["Aeg"] = pd.to_datetime(
        df["Aeg "],
        format='%d.%m.%Y %H:%M:%S',
        unit='d'
    )
    df.drop("Aeg ", axis=1, inplace =True)


def setCategorical(df):
    df["Maakond"] = df["Maakond"].astype('category')
    df["Linn,vald"] = df["Linn,vald"].astype('category')
    df["Väljakutse liik SOS"] = df["Väljakutse liik SOS"].astype('category')
