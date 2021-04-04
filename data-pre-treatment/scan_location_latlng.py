import json
from concurrent.futures.thread import ThreadPoolExecutor

import googlemaps


def scan_location_latlng():
    api_key = "your google api key"
    locations = []

    with open("resource/taxi_zones.csv", "r") as f:
        for i, line in enumerate(f.readlines()):
            line = [x.replace('"', "").strip() for x in line.split(",")]
            if i == 0:
                header = line
            else:
                location = {header[j]: k for j, k in enumerate(line)}
                if location["Borough"] != "Unknown":
                    locations.append(location)

    def get_lat_lng(loc):
        info = gmaps.geocode(f"newyork,{loc['Borough']},{loc['Zone']}")
        loc.update(info[0]["geometry"]["location"])
        return loc

    gmaps = googlemaps.Client(key=api_key)
    with ThreadPoolExecutor(max_workers=32) as executor:
        locations = list(executor.map(get_lat_lng, locations))

    with open("resource/zones_latlng.txt", "w") as f:
        locations = [json.dumps(x) for x in locations]
        f.writelines(f"{x}\n" for x in locations)


if __name__ == '__main__':
    scan_location_latlng()
