#!/usr/bin/env python

# Author: Jachym Cepicky
# Purpose: Track cars of Seznam Panorama project

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from lxml import objectify
import urllib
import os
import json
import sqlite3
import time
import ConfigParser

URL = "http://vyfotpanorama.seznam.cz/cars.xml"
INTERVAL = 2
OUTFILE_GEOJSON = os.path.join(
    os.path.dirname(__file__),
    os.path.join("..", "data", "sznpanorama.geojson")
)
OUTFILE_SQLITE = os.path.join(
    os.path.dirname(__file__),
    os.path.join("..", "data", "sznpanorama.sqlite")
)


class Car:
    id = None
    utc = None
    local = None
    position = None
    speed = None

    class Position:
        lon = None
        lat = None

        def __init__(self, lon, lat):
            self.lon = lon
            self.lat = lat

        def __str__(self):
            return "%f %f" % (self.lon, self.lat)

    def __init__(self):
        pass

    def parse(self, elem):
        self.id = int(elem.id)
        self.utc = time.strptime(elem.lastSeen.text,
                                 "%H:%M:%S %d.%m.%Y")
        self.local = time.strptime(elem.lastSeenLocaltime.text,
                                   "%H:%M:%S %d.%m.%Y")
        (lon, lat) = elem.lastPosition.text.split(",")
        self.position = self.Position(float(lon), float(lat))
        self.speed = float(elem.lastSpeed)

    def to_sql(self):

        return "(%d, '%s', %f, %f, %f)" %\
            (self.id, self.utc, self.position.lon,
             self.position.lat, self.speed)

    def to_geojson(self):
        point_obj = {
            'type': 'Feature',
            'properties': {
                'id': self.id,
                'last': self.utc,
                'speed': self.speed
            },
            'geometry': {
                'type': 'Point',
                'coordinates': '[%s]' % (str(self.position)),
            }
        }

        return point_obj

    def __str__(self):

        return "Car %d: %s|%s|%.1f km/h" % (self.id, self.utc,
                                            self.position, self.speed)


def get_color(nr):

    interval = 1
    r = g = b = 0
    start = 0
    while start <= nr:
        r += int(255 / interval)
        if r > 255:
            r = 0
            g += int(255 / interval)
        if g > 255:
            g = 0
            b += int(255 / interval)
        if b > 255:
            b = 0
        start += 1

    return "%.2X%.2X%.2X" % (r, g, b)


def write_geojson(out_file):

    json_file = open(out_file, "w")

    json_obj = {'type': 'FeatureCollection', 'features': []}
    conn = sqlite3.connect(OUTFILE_SQLITE)
    c = conn.cursor()

    c.execute("select car,speed,time,lat,lon from positions")
    for row in c.fetchall():
        json_obj['features'].append({
            'type': 'Feature',
            'properties': {
                'car': row[0],
                'speed': row[1],
                'time': time.ctime(int(row[2])),
                'marker-symbol': 'marker',
                'marker-color': get_color(row[0])
            },
            'geometry': {
                'type': 'Point',
                'coordinates': [row[4], row[3]]
            }
        })

    json_file.write(json.dumps(json_obj))
    json_file.close()

    config = ConfigParser.ConfigParser()
    if len(config.read([os.path.join(os.path.dirname(__file__),
                        "sniffer.cfg")])):
        if config.getboolean('Github', 'write'):
            import subprocess
            subprocess.Popen(["git", "commit", "-m",
                              "'Automatic GeoJson update'",
                             OUTFILE_GEOJSON],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             stdin=subprocess.PIPE
                             )

            time.sleep(1)
            subprocess.Popen(["git", "push"],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             stdin=subprocess.PIPE
                             )


def _write_sqlite(cars):

    conn = sqlite3.connect(OUTFILE_SQLITE)
    c = conn.cursor()
    c.execute("select car, max(time) from positions  group by car")

    last_positions = c.fetchall()
    for car in cars:
        for row in last_positions:
            if row[0] == car.id:
                position_time = int(time.mktime(car.utc))
                if position_time > int(row[1]):
                    c.execute("INSERT INTO positions"
                              "(car, time, lat, lon) VALUES"
                              "(%d,%i, %f, %f)" %
                              (car.id, position_time,
                               car.position.lat, car.position.lon))
    conn.commit()

    #s = time.mktime(t)


def write(cars, geojson=False):
    _write_sqlite(cars)
    if geojson:
        write_geojson(OUTFILE_GEOJSON)


def read():

    fileobj = urllib.urlopen(URL)
    tree = objectify.parse(fileobj)

    cars = tree.getroot()

    new_cars = []

    for i in range(len(cars.car)):
        car_elem = cars.car[i]
        car = Car()
        car.parse(car_elem)
        new_cars.append(car)

    return new_cars


def spawn():

    pid = os.fork()
    if pid:
        return pid


def loop():

    i = 0
    while True:
        # every 120 cycles write geojson too
        if i == 120:
            write(read(), True)
            i = 0
        else:
            write(read(), False)
            i += 1
        time.sleep(INTERVAL)


def main(daemon):

    if daemon:
        pid = spawn()
        if pid > 0:
            print "Daemon set, pid %d" % pid
            return
        else:
            loop()
    else:
        loop()

    print "Loop end"


if __name__ == "__main__":

    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-d", "--daemon",
                      action="store_false", dest="daemon", default=True,
                      help="run in daemon mode (default)")
    parser.add_option("-j", "--geojson",
                      dest="geojson", metavar="FILE",
                      help="Generate just the geojson file")

    (options, args) = parser.parse_args()

    if options.geojson:
        write_geojson(options.geojson)
    else:
        main(options.daemon)
