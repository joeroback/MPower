#!/usr/bin/env python3

import argparse
import math
import pytz
import struct
import sys
import zipfile

import logging as lg
import numpy as np
import pandas as pd

from datetime import datetime, timezone
from pyproj import Geod
from signal import signal, SIGPIPE, SIG_DFL

#
# TODO
#
# interpolate GPS using haversine formula
# debug print
# csv print
#

#
# all far files have 8 bytes at the beginning that indicate the record size
# then <n> of records, all which start with a 8 byte float timestamp.
# if recorded on iOS, timestamp is in local timezone and epoch starts at
# jan 1st, 2001, because tim apple and iOS 6+... ugh
#

FAR_FILE_FORMAT = {
    'AccelerationLateral.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'AccelerationLongitudinal.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'AcceleratorPedal.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'BrakeContact.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'CurrentConsumption.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'Distance.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'Gear.far': {
        'record_size': 16,
        'record_format': '<dQ',
    },
    'Gearbox.far': {
        'record_size': 24,
        'record_format': '<dQQ',
    },
    'Heading.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'Location.far': {
        'record_size': 24,
        'record_format': '<ddd',
    },
    'RPM.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'Speed.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
    'Steering.far': {
        'record_size': 16,
        'record_format': '<dd',
    },
}

class FarFile:
    def __init__(self, file, name):
        self.name = name
        self.record_size = FAR_FILE_FORMAT[name]['record_size']
        self.record_format = FAR_FILE_FORMAT[name]['record_format']
        self.records = []

        lg.info(f'Loading {name}')

        if self.record_size != struct.unpack('<Q', file.read(8))[0]:
            raise RuntimeError('Invalid record size detected!')

        # read all records out
        while True:
            record_bytes = file.read(self.record_size)
            if not record_bytes or len(record_bytes) != self.record_size:
                break
            record = struct.unpack(self.record_format, record_bytes)
            self.records.append(record)

        lg.info(f'  -> Found {len(self.records)} records for {name}')

class MPowerFile:
    IOS_EPOCH_HACK = 978307200

    def __init__(self, filename):
        self.time_index = []
        self.far_files = {}

        with zipfile.ZipFile(filename) as mpower_file:
            for far_file in sorted(list(filter(lambda x: (x in FAR_FILE_FORMAT), mpower_file.namelist()))):
                with mpower_file.open(far_file) as file:
                    self.far_files[far_file] = FarFile(file, far_file)

        # build time index of unique sorted time records from all the far files
        for far_file in self.far_files.values():
            for record in far_file.records:
                assert len(record) > 0
                self.time_index.append(record[0])
        self.time_index = sorted(set(self.time_index))

        lg.info(f'Found {len(self.time_index)} events')
        assert len(self.time_index) > 0

    def to_csv(self):
        def format_laptime(lt):
            minutes = math.trunc(lt / 60)
            seconds = math.trunc(lt) - (minutes * 60)
            microseconds = round((lt - (minutes * 60) - seconds) * 100)
            return f'{minutes:02d}:{seconds:02d}.{microseconds:02d}'

        start_lap_time = self.time_index[0]
        index = 0
        lap_index = 1

        series_index = pd.Series(index=self.time_index, name='INDEX', dtype='UInt64')
        series_utc_time = pd.Series(index=self.time_index, name='UTC Time', dtype='float64')
        series_lap_index = pd.Series(index=self.time_index, name='LAPINDEX', dtype='UInt64')
        series_date = pd.Series(index=self.time_index, name='DATE', dtype='string')
        series_time = pd.Series(index=self.time_index, name='TIME', dtype='string')
        series_time_lap = pd.Series(index=self.time_index, name='TIME_LAP', dtype='string')
        series_height_m = pd.Series(index=self.time_index, name='HEIGHT_M', dtype='UInt64')
        series_height_ft = pd.Series(index=self.time_index, name='HEIGHT_FT', dtype='UInt64')
        series_accel_source = pd.Series(index=self.time_index, name='ACCELERATIONSOURCE[CALCULATED/MEASURED/UNDEFINED]', dtype='UInt64')

        for t in self.time_index:
            series_index.loc[t] = index
            index = index + 1

            series_lap_index.loc[t] = lap_index

            # TODO this assumes recorded timezone is timezone of this running script...
            # might be possible to figure out recorded timezone from JSON files included in mpower file?
            dt = datetime.fromtimestamp(t + self.IOS_EPOCH_HACK).astimezone(timezone.utc)
            series_utc_time[t] = dt.replace(tzinfo=timezone.utc).timestamp()
            series_date.loc[t] = dt.strftime("%d-%b-%y")
            series_time.loc[t] = dt.strftime("%H:%M:%S.%f")

            # TODO adjust this once we have mpower file with sessions / laps
            series_time_lap.loc[t] = format_laptime(t - start_lap_time)

            series_height_m.loc[t] = 0
            series_height_ft.loc[t] = 0
            series_accel_source.loc[t] = 1

        series_brake_pressure = pd.Series(index=self.time_index, name='BRAKEPRESSURE', dtype='float64')
        for record in self.far_files['BrakeContact.far'].records:
            assert len(record) == 2
            series_brake_pressure.loc[record[0]] = record[1]
        series_brake_pressure = series_brake_pressure.interpolate(method='ffill').fillna(0)

        series_current_consumption = pd.Series(index=self.time_index, name='CURRENTCONSUMPTION', dtype='float64')
        for record in self.far_files['CurrentConsumption.far'].records:
            assert len(record) == 2
            if record[1] <= 0:
                series_current_consumption.loc[record[0]] = 0
            else:
                series_current_consumption.loc[record[0]] = 235.214 / record[1]
        series_current_consumption = series_current_consumption.interpolate(method='ffill').fillna(0)

        series_gear = pd.Series(index=self.time_index, name='GEAR', dtype='UInt64')
        for record in self.far_files['Gearbox.far'].records:
            assert len(record) == 3
            series_gear.loc[record[0]] = record[2]
        series_gear = series_gear.interpolate(method='ffill').fillna(0)

        series_heading = pd.Series(index=self.time_index, name='HEADING_DEG', dtype='float64')
        for record in self.far_files['Heading.far'].records:
            assert len(record) == 2
            series_heading.loc[record[0]] = math.degrees(record[1])
        # TODO FILL OR LINEAR???
        series_heading = series_heading.interpolate(method='ffill').fillna(0)

        series_speed_kph = pd.Series(index=self.time_index, name='SPEED_KPH', dtype='float64')
        series_speed_mph = pd.Series(index=self.time_index, name='SPEED_MPH', dtype='float64')
        for record in self.far_files['Speed.far'].records:
            assert len(record) == 2
            series_speed_kph.loc[record[0]] = record[1] * 3.6
            series_speed_mph.loc[record[0]] = record[1] * 2.237
        series_speed_kph = series_speed_kph.interpolate(method='linear', limit_direction='forward').fillna(0)
        series_speed_mph = series_speed_mph.interpolate(method='linear', limit_direction='forward').fillna(0)

        series_rpm = pd.Series(index=self.time_index, name='RPM', dtype='float64')
        for record in self.far_files['RPM.far'].records:
            assert len(record) == 2
            series_rpm.loc[record[0]] = record[1]
        series_rpm = series_rpm.interpolate(method='linear', limit_direction='forward').fillna(0)

        series_steering = pd.Series(index=self.time_index, name='STEERINGANGLE', dtype='float64')
        for record in self.far_files['Steering.far'].records:
            assert len(record) == 2
            series_steering.loc[record[0]] = record[1]
        series_steering = series_steering.interpolate(method='linear', limit_direction='forward').fillna(0)

        series_distance_km = pd.Series(index=self.time_index, name='DISTANCE_KM', dtype='float64')
        series_distance_mi = pd.Series(index=self.time_index, name='DISTANCE_MILE', dtype='float64')
        for record in self.far_files['Distance.far'].records:
            assert len(record) == 2
            series_distance_km.loc[record[0]] = record[1] / 1000.0
            series_distance_mi.loc[record[0]] = record[1] / 1609.0
        series_distance_km = series_distance_km.interpolate(method='linear', limit_direction='forward').fillna(0)
        series_distance_mi = series_distance_mi.interpolate(method='linear', limit_direction='forward').fillna(0)

        series_linealg = pd.Series(index=self.time_index, name='LINEALG', dtype='float64')
        for record in self.far_files['AccelerationLongitudinal.far'].records:
            assert len(record) == 2
            series_linealg.loc[record[0]] = record[1]
        series_linealg = series_linealg.interpolate(method='nearest', limit_direction='forward').fillna(0)

        series_lateralg = pd.Series(index=self.time_index, name='LATERALG', dtype='float64')
        for record in self.far_files['AccelerationLateral.far'].records:
            assert len(record) == 2
            series_lateralg.loc[record[0]] = record[1]
        series_lateralg = series_lateralg.interpolate(method='nearest', limit_direction='forward').fillna(0)

        series_throttle = pd.Series(index=self.time_index, name='THROTTLE', dtype='float64')
        for record in self.far_files['AcceleratorPedal.far'].records:
            assert len(record) == 2
            series_throttle.loc[record[0]] = record[1] / 100.0
        series_throttle = series_throttle.interpolate(method='linear', limit_direction='forward')
        series_throttle = series_throttle.fillna(0)

        df_gps = pd.DataFrame(index=self.time_index, columns=['LATITUDE','LONGITUDE'], dtype='float64')
        for record in self.far_files['Location.far'].records:
            assert len(record) == 3
            df_gps.loc[record[0], 'LATITUDE'] = record[1]
            df_gps.loc[record[0], 'LONGITUDE'] = record[2]
        df_gps = df_gps.sort_index()

        # interpolate GPS coordinates using GPS method, can pandas do this??

        class GpsTracker:
            def __init__(self):
                self.lat = np.nan
                self.lng = np.nan
                self.n = 0

        gt = GpsTracker()
        geoid = Geod(ellps="WGS84")

        for i in range(0, len(df_gps)):
            lat = df_gps.iloc[i]['LATITUDE']
            lng = df_gps.iloc[i]['LONGITUDE']

            if pd.notnull(lat):
                assert pd.notnull(lng)
                if gt.n > 0:
                    lg.debug(f'Filling {gt.n} rows. ({gt.lat},{gt.lng}) -> ({lat},{lng}), rows {i-gt.n}:{i-1}')
                    try:
                        extra_points = geoid.npts(gt.lng, gt.lat, lng, lat, gt.n)
                        for ei, ep in enumerate(extra_points):
                            lg.debug(f'point: {ei} {i - gt.n + ei} {ep}')
                            df_gps.iloc[i - gt.n + ei]['LATITUDE'] = ep[1]
                            df_gps.iloc[i - gt.n + ei]['LONGITUDE'] = ep[0]
                    except Exception as e:
                        lg.warning(f'Unable to interpolate GPS points {gt.lng}, {gt.lat} -> {lng}, {lat} for {gt.n} points: {e}')

                    gt.n = 0
                gt.lat = lat
                gt.lng = lng
            elif pd.notnull(gt.lat):
                assert pd.notnull(gt.lng)
                gt.n = gt.n + 1

        df_gps = df_gps.fillna(0)

        # minimum CSV headers required by Telemetry Overlay when importing a Harry's Laptimer formatted CSV
        # headers = [
        #     'INDEX',
        #     'LAPINDEX',
        #     'DATE',
        #     'TIME',
        #     'TIME_LAP',
        #     'LATITUDE',
        #     'LONGITUDE',
        #     'SPEED_KPH',
        #     'SPEED_MPH',
        #     'HEIGHT_M',
        #     'HEIGHT_FT',
        #     'HEADING_DEG',
        #     'DISTANCE_KM',
        #     'DISTANCE_MILE',
        #     'ACCELERATIONSOURCE[CALCULATED/MEASURED/UNDEFINED]',
        #     'LINEALG',
        #     'LATERALG',
        #     'GEAR',
        #     'RPM',
        #     'THROTTLE',
        #     'BRAKEPRESSURE',
        #     'STEERINGANGLE',
        # ]

        df = pd.DataFrame(index=self.time_index)
        df = df.merge(series_index, left_index=True, right_index=True)
        df = df.merge(series_utc_time, left_index=True, right_index=True)
        df = df.merge(series_lap_index, left_index=True, right_index=True)
        df = df.merge(series_date, left_index=True, right_index=True)
        df = df.merge(series_time, left_index=True, right_index=True)
        df = df.merge(series_time_lap, left_index=True, right_index=True)
        df = df.merge(df_gps, left_index=True, right_index=True)
        df = df.merge(series_speed_kph, left_index=True, right_index=True)
        df = df.merge(series_speed_mph, left_index=True, right_index=True)
        df = df.merge(series_height_m, left_index=True, right_index=True)
        df = df.merge(series_height_ft, left_index=True, right_index=True)
        df = df.merge(series_heading, left_index=True, right_index=True)
        df = df.merge(series_distance_km, left_index=True, right_index=True)
        df = df.merge(series_distance_mi, left_index=True, right_index=True)
        df = df.merge(series_accel_source, left_index=True, right_index=True)
        df = df.merge(series_linealg, left_index=True, right_index=True)
        df = df.merge(series_lateralg, left_index=True, right_index=True)
        df = df.merge(series_gear, left_index=True, right_index=True)
        df = df.merge(series_rpm, left_index=True, right_index=True)
        df = df.merge(series_throttle, left_index=True, right_index=True)
        df = df.merge(series_brake_pressure, left_index=True, right_index=True)
        df = df.merge(series_current_consumption, left_index=True, right_index=True)
        df = df.merge(series_steering, left_index=True, right_index=True)

        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None, 'display.memory_usage', None, 'display.width', 20000):
            lg.debug(df)

        print('Harry\'s GPS LapTimer', file=sys.stdout)
        df.to_csv(path_or_buf=sys.stdout, sep=',', header=True, index=False)

if __name__ == '__main__':
    signal(SIGPIPE, SIG_DFL)

    FORMAT = '%(asctime)s %(message)s'
    lg.basicConfig(stream=sys.stderr, format=FORMAT, level=lg.INFO)

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('mpower_file', nargs='?', help='MPower filename')
    parser.add_argument('--debug', action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    if args.debug:
        lg.getLogger().setLevel(lg.DEBUG)

    mpower_file = MPowerFile(args.mpower_file)
    mpower_file.to_csv()
