import urllib.request
import urllib.parse
import json
from rate_limit_decorator import rate_limited


@rate_limited(10)
def get_location(address):
    parsed_address = urllib.parse.quote(address)
    url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(parsed_address)
    response = urllib.request.urlopen(url).read().decode('utf-8')
    payload = json.loads(response)
    try:
        location = payload["results"][0]["geometry"]["location"]
    except IndexError:
        raise LookupError("Could not find coordinates for {}".format(address))

    return location
