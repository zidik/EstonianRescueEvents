import pandas as pd
import os.path
import urllib.request

def load_stations(path="stations_estonia.csv"):
    # Took all relevant stations from ghcnd-stations.txt and converted the format to csv semi-manually
    # Saved the result to filtered_stations.csv
    stations = pd.read_csv(
        path,
        header=None,
        names=["ID","lat","lng","elev","name","WMO ID"],
        usecols=["ID","lat","lng","elev","name"]
    )
    return stations


def load_daily_weather(path, original=True, **kwargs):
    try:
        return pd.read_csv(
            path,
            header=None,
            names=["station","date","type","value","","","",""],
            usecols=["station","date","type","value"],
            **kwargs
        )
    except pd.parser.CParserError:
        return pd.read_csv(
            path,
            header=None,
            names=["station","date","type","value"],
            parse_dates=["date"],
            **kwargs
        )


def save_daily_weather(df, path):
    df.to_csv(
        path,
        header=None,
        compression="gzip",
        index=False
    )


def download_weather(cache_path,year):
    print("* Downloading year: {} *".format(year))
    src_url = "http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{}.csv.gz".format(year)
    dst_path = "{}{}.csv.gz".format(cache_path,year)

    urllib.request.urlretrieve(src_url, dst_path)
    print("* Download complete! *")

def filter_weather(cache_path, stations, year):
    src_path = "{}{}.csv.gz".format(cache_path,year)
    dst_path = "{}{}_estonia.csv.gz".format(cache_path,year)

    #Check if needed file is there, if not -> download
    if(os.path.isfile(src_path)):
        print("Cache '{}' exists, skipping downloading...".format(src_path))
    else:
        download_weather(cache_path, year)

    print("* Filtering year: {} *".format(year))

    print("Loading '{}'...".format(src_path), end="")
    daily = load_daily_weather(src_path)
    print("DONE!")

    print("Converting...",end="")
    daily_estonia = daily[daily.station.isin(stations.ID)]
    print("DONE!")

    print("Saving to '{}'...".format(dst_path), end="")
    save_daily_weather(daily_estonia, dst_path)
    print("DONE!")
    print("* Filtering complete! *")


def load_estonian_weather(cache_path, stations, years=range(2011,2015)):
    print("*** Loading weather ***")
    df_frames = []
    for year in years:
        src_path = "{}{}_estonia.csv.gz".format(cache_path, year)

        #Check if needed file is there, if not -> filter
        if(os.path.isfile(src_path)):
            print("Cache '{}' exists, skipping filtering...".format(src_path))
        else:
            filter_weather(cache_path, stations, year)
        print("* Loading year: {} *".format(year))
        df = load_daily_weather(src_path)
        df_frames.append(df)
        print("* Loading year complete! *")
    print("*** Loading weather complete ***")
    return pd.concat(df_frames, ignore_index=True).sort_values(by="date")


