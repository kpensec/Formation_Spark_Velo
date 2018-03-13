# coding: utf-8
import os
import sys
import math
from docopt import docopt
from pyspark import SparkContext


"""ParkaBike
Usage:
    main.py <inputPath> [-d=<dist>]

Restrictions:
    <dist> : float
"""
#if __name__ == "__main__":
#    arguments = docopt(__doc__, version='ParkaBike 0.1')
#    print(arguments)

#    0 -> id
#    10 -> capa
#    15 -> location

radius = 0.4
if __name__ == "__main__" and len(sys.argv[2]) > 2:
    radius = float(sys.argv[2])/1000

(LAT,LON) = (47.2063698, -1.5643358)

earthRadius = 6371
R = radius / earthRadius
latProj = math.degrees(R/math.cos(math.radians(LAT)))

maxLon = LON + latProj
minLon = LON - latProj
maxLat = LAT + math.degrees(R)
minLat = LAT - math.degrees(R)

def extractData(line):
    elements = line.split(',')
    key = elements[0][1:-1]        # removing double quote
    title = elements[1][1:-1]
    capa = int(elements[10][1:-1])
    lat = float(elements[-2][2:])
    lon = float(elements[-1][:-2])
    value = {
        'title': title,
        'capa': capa,
        'lat': lat,
        'lon': lon
    }
    return (key,value)

"""
    check if distance is in a given radius from a given point 
"""
def distCheck(element):
    lat = element[1]['lat']
    lon = element[1]['lon']
    return (minLat <= lat <= maxLat) and (minLon <= lon <= maxLon)

"""
    dist in meter
"""
def distCalc(element):
    lon1, lon2, lat1, lat2 = map(math.radians,[LON, element[1]['lon'], LAT, element[1]['lat']])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat/2)**2
    a += math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return 6371 * 2 * math.asin(math.sqrt(a)) * 1000


if __name__ == "__main__":
    # arg parse
    if len(sys.argv) < 3:
        sys.exit(-1)

    sc = SparkContext(appName='Mon Velo', master='local[2]')
    inputDirname = sys.argv[1]
    outputDirname = sys.argv[2]

    result = sc.textFile(
        inputDirname
    ).map(
        extractData
    ).filter(
        lambda e: e[1]['capa'] >= 5
    ).filter(
        distCheck
    ).map(
        lambda e : (e[1]['title'], distCalc(e))
    )
    for e in result.collect():
        print(e)
