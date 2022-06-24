#!/usr/bin/env python3

import argparse
import math
import pytz
import struct
import sys
import zipfile

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
# then <n> of records, all which start with a 8 byte float timestamp
#
# record_nil does not contain value for timestamp, only data values
#

FAR_FILE_FORMAT = {
    'AccelerationLateral.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
    'AccelerationLongitudinal.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
    'AcceleratorPedal.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
    'BrakeContact.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
    'Distance.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
    'Gearbox.far': {
        'record_size': 24,
        'record_format': '<dQQ',
        'record_nil': (0, 0,),
    },
    'Heading.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
    'Location.far': {
        'record_size': 24,
        'record_format': '<ddd',
        'record_nil': (float('NaN'), float('NaN'),),
    },
    'RPM.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (float('NaN'),),
    },
    'Speed.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
    'Steering.far': {
        'record_size': 16,
        'record_format': '<dd',
        'record_nil': (0.0,),
    },
}

class FarFile:
    def __init__(self, file, name):
        self.name = name
        self.record_size = FAR_FILE_FORMAT[name]['record_size']
        self.record_format = FAR_FILE_FORMAT[name]['record_format']
        self.record_nil = FAR_FILE_FORMAT[name]['record_nil']
        self.records = []
        self.current_record = 0

        print(f'Loading {name}', file=sys.stderr)

        if self.record_size != struct.unpack('<Q', file.read(8))[0]:
            raise RuntimeError('Invalid record size detected!')

        # read all records out
        while True:
            record_bytes = file.read(self.record_size)
            if not record_bytes or len(record_bytes) != self.record_size:
                break
            record = struct.unpack(self.record_format, record_bytes)
            self.records.append(record)

        print(f'  -> Found {len(self.records)} records for {name}', file=sys.stderr)

    def get_csv_record(self, t):
        """ get nearest record to time t but do not actually return the time part of the tuple """
        assert len(self.records) > 1

        ret = None

        record_time = self.records[self.current_record][0]

        # special case GPS coordinates, we will interpolate them later
        # using more sophisticated method, for now just leave them empty
        if self.name == 'Location.far' or self.name == 'RPM.far':
            if t == record_time:
                ret = self.records[self.current_record][1:]
                if self.current_record < len(self.records) - 1:
                    self.current_record = self.current_record + 1
            else:
                ret = self.record_nil
        else:
            if t >= record_time:
                ret = self.records[self.current_record][1:]
                if self.current_record < len(self.records) - 1:
                    self.current_record = self.current_record + 1
            elif self.current_record == 0:
                ret = self.record_nil
            else:
                ret = self.records[self.current_record - 1][1:]

        return ret

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

        print(f'Found {len(self.time_index)} events', file=sys.stderr)
        assert len(self.time_index) > 0

        def format_laptime(lt):
            minutes = math.trunc(lt / 60)
            seconds = math.trunc(lt) - (minutes * 60)
            microseconds = round((lt - (minutes * 60) - seconds) * 100)
            return f'{minutes:02d}:{seconds:02d}.{microseconds:02d}'

        start_lap_time = self.time_index[0]
        i = 0

        self.series_index = pd.Series(index=self.time_index, name='INDEX', dtype='UInt64')
        self.series_lap_index = pd.Series(index=self.time_index, name='LAPINDEX', dtype='UInt64')
        self.series_date = pd.Series(index=self.time_index, name='DATE', dtype='string')
        self.series_time = pd.Series(index=self.time_index, name='TIME', dtype='string')
        self.series_time_lap = pd.Series(index=self.time_index, name='TIME_LAP', dtype='string')
        self.series_height_m = pd.Series(index=self.time_index, name='HEIGHT_M', dtype='UInt64')
        self.series_height_ft = pd.Series(index=self.time_index, name='HEIGHT_FT', dtype='UInt64')
        self.series_accel_source = pd.Series(index=self.time_index, name='ACCELERATIONSOURCE[CALCULATED/MEASURED/UNDEFINED]', dtype='UInt64')

        for t in self.time_index:
            self.series_index.loc[t] = i
            i = i + 1

            self.series_lap_index.loc[t] = 1

            dt = datetime.fromtimestamp(t + self.IOS_EPOCH_HACK)
            self.series_date.loc[t] = dt.strftime("%d-%b-%y")
            self.series_time.loc[t] = dt.strftime("%H:%M:%S.%f")
            self.series_time_lap.loc[t] = format_laptime(t - start_lap_time)

            self.series_height_m.loc[t] = 0
            self.series_height_ft.loc[t] = 0
            self.series_accel_source.loc[t] = 1

        self.series_brake_pressure = pd.Series(index=self.time_index, name='BRAKEPRESSURE', dtype='float64')
        for record in self.far_files['BrakeContact.far'].records:
            assert len(record) == 2
            self.series_brake_pressure.loc[record[0]] = record[1]
        self.series_brake_pressure = self.series_brake_pressure.interpolate(method='ffill')
        self.series_brake_pressure = self.series_brake_pressure.interpolate(method='bfill')

        self.series_gear = pd.Series(index=self.time_index, name='GEAR', dtype='UInt64')
        for record in self.far_files['Gearbox.far'].records:
            assert len(record) == 3
            self.series_gear.loc[record[0]] = record[2]
        self.series_gear = self.series_gear.interpolate(method='ffill')
        self.series_gear = self.series_gear.interpolate(method='bfill')

        self.series_heading = pd.Series(index=self.time_index, name='HEADING_DEG', dtype='float64')
        for record in self.far_files['Heading.far'].records:
            assert len(record) == 2
            self.series_heading.loc[record[0]] = math.degrees(record[1])
        # TODO FILL OR LINEAR???
        self.series_heading = self.series_heading.interpolate(method='ffill')
        self.series_heading = self.series_heading.interpolate(method='bfill')

        self.series_speed_kph = pd.Series(index=self.time_index, name='SPEED_KPH', dtype='float64')
        self.series_speed_mph = pd.Series(index=self.time_index, name='SPEED_MPH', dtype='float64')
        for record in self.far_files['Speed.far'].records:
            assert len(record) == 2
            self.series_speed_kph.loc[record[0]] = record[1] * 3.6
            self.series_speed_mph.loc[record[0]] = record[1] * 2.237
        self.series_speed_kph = self.series_speed_kph.interpolate(method='linear', limit_direction='both')
        self.series_speed_mph = self.series_speed_mph.interpolate(method='linear', limit_direction='both')

        self.series_rpm = pd.Series(index=self.time_index, name='RPM', dtype='float64')
        for record in self.far_files['RPM.far'].records:
            assert len(record) == 2
            self.series_rpm.loc[record[0]] = record[1]
        self.series_rpm = self.series_rpm.interpolate(method='linear', limit_direction='both')

        self.series_steering = pd.Series(index=self.time_index, name='STEERINGANGLE', dtype='float64')
        for record in self.far_files['Steering.far'].records:
            assert len(record) == 2
            self.series_steering.loc[record[0]] = record[1]
        self.series_steering = self.series_steering.interpolate(method='linear', limit_direction='both')

        self.series_distance_km = pd.Series(index=self.time_index, name='DISTANCE_KM', dtype='float64')
        self.series_distance_mi = pd.Series(index=self.time_index, name='DISTANCE_MILE', dtype='float64')
        for record in self.far_files['Distance.far'].records:
            assert len(record) == 2
            self.series_distance_km.loc[record[0]] = record[1] / 1000.0
            self.series_distance_mi.loc[record[0]] = record[1] / 1609.0
        self.series_distance_km = self.series_distance_km.interpolate(method='linear', limit_direction='both')
        self.series_distance_mi = self.series_distance_mi.interpolate(method='linear', limit_direction='both')

        self.series_linealg = pd.Series(index=self.time_index, name='LINEALG', dtype='float64')
        for record in self.far_files['AccelerationLongitudinal.far'].records:
            assert len(record) == 2
            self.series_linealg.loc[record[0]] = record[1]
        self.series_linealg = self.series_linealg.interpolate(method='nearest', limit_direction='both')

        self.series_lateralg = pd.Series(index=self.time_index, name='LATERALG', dtype='float64')
        for record in self.far_files['AccelerationLateral.far'].records:
            assert len(record) == 2
            self.series_lateralg.loc[record[0]] = record[1]
        self.series_lateralg = self.series_lateralg.interpolate(method='nearest', limit_direction='both')

        self.series_throttle = pd.Series(index=self.time_index, name='THROTTLE', dtype='float64')
        for record in self.far_files['AcceleratorPedal.far'].records:
            assert len(record) == 2
            self.series_throttle.loc[record[0]] = record[1] / 100.0
        self.series_throttle = self.series_throttle.interpolate(method='linear', limit_direction='both')

        # lat0, lon0 = 40.07081111, -105.3095917
        # lat1, lon1 = 40.07079194, -105.3095875
        # n_extra_points = 8

        # geoid = Geod(ellps="WGS84")
        # extra_points = geoid.npts(lon0, lat0, lon1, lat1, n_extra_points)
        # print(extra_points, file=sys.stderr)

        # self.series_latitude = pd.Series(index=self.time_index, name='LATITUDE', dtype='float64')
        # self.series_longitude = pd.Series(index=self.time_index, name='LONGITUDE', dtype='float64')
        # for record in self.far_files['Location.far'].records:
        #     assert len(record) == 3
        #     self.series_latitude.loc[record[0]] = record[1]
        #     self.series_longitude.loc[record[0]] = record[2]










# class GpsTracker:
#     def __init__(self):
#         self.lat = None
#         self.lng = None
#         self.n = 0

# gt = GpsTracker()

# gps = [
#     [None, None],
#     [40.1, -105.0],
#     [None, None],
#     [None, None],
#     [None, None],
#     [40.2, -105.05],
#     [40.3, -105.08],
#     [None, None],
#     [40.6, -105.3],
#     [None, None],
#     [None, None],
#     [40.65, -105.4],
#     [None, None],
# ]

# for index, coords in enumerate(gps):
#     if coords[0] != None:
#         if gt.lat != None:
#             assert gt.lng
#             print(f'Filling {gt.n} rows. ({gt.lat},{gt.lng}) -> ({coords[0]}, {coords[1]}), {index-gt.n}:{index-1}')
#             gt.n = 0
#         assert coords[1]
#         gt.lat = coords[0]
#         gt.lng = coords[1]
#     else:
#         if gt.lat != None:
#             assert gt.lng
#             gt.n = gt.n + 1
















        self.df_gps = pd.DataFrame(index=self.time_index, columns=['LATITUDE','LONGITUDE'], dtype='float64')
        for record in self.far_files['Location.far'].records:
            assert len(record) == 3
            self.df_gps.loc[record[0], 'LATITUDE'] = record[1]
            self.df_gps.loc[record[0], 'LONGITUDE'] = record[2]

        for col, content in self.df_gps.iteritems():
            #print(row['LATITUDE'], row['LONGITUDE'], file=sys.stderr)
            print(col, content, file=sys.stderr)
            break

        # first_df = self.df_gps.apply(pd.Series.first_valid_index)
        # print('1', self.df_gps.loc[first_df['LATITUDE']]['LATITUDE'], self.df_gps.loc[first_df['LONGITUDE']]['LONGITUDE'], file=sys.stderr)

    def to_csv(self):
        # def format_laptime(lt):
        #     minutes = math.trunc(lt / 60)
        #     seconds = math.trunc(lt) - (minutes * 60)
        #     microseconds = round((lt - (minutes * 60) - seconds) * 100)
        #     return f'{minutes:02d}:{seconds:02d}.{microseconds:02d}'

        # # TODO adjust this once we have mpower file with sessions / laps
        # start_lap_time = self.time_index[0]

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

        # print('Harry\'s GPS LapTimer')
        # print(','.join(headers))

        # csv_data = []

        # lap_index = 1

        # for t in self.time_index:
        #     row = []

        #     # INDEX
        #     row.append(len(csv_data))

        #     # LAPINDEX
        #     row.append(lap_index)

        #     # TODO is this UTC?
        #     dt = datetime.fromtimestamp(t + self.IOS_EPOCH_HACK)

        #     # DATE
        #     row.append(dt.strftime("%d-%b-%y"))

        #     # TIME
        #     row.append(dt.strftime("%H:%M:%S.%f"))

        #     # TIME_LAP
        #     row.append(format_laptime(t - start_lap_time))

        #     # LATITUDE, LONGITUDE
        #     tmp = self.far_files['Location.far'].get_csv_record(t)
        #     row.append(tmp[0])
        #     row.append(tmp[1])

        #     # SPEED_KPH (km/h), SPEED_MPH (mph)
        #     tmp = self.far_files['Speed.far'].get_csv_record(t)
        #     row.append(tmp[0] * 3.6)
        #     row.append(tmp[0] * 2.237)

        #     # HEIGHT_M, HEIGHT_FT
        #     row.append(0)
        #     row.append(0)

        #     # HEADING_DEG
        #     tmp = self.far_files['Heading.far'].get_csv_record(t)
        #     row.append(math.degrees(tmp[0]))

        #     # DISTANCE_KM, DISTANCE_MILE
        #     tmp = self.far_files['Distance.far'].get_csv_record(t)
        #     row.append(tmp[0] / 1000.0)
        #     row.append(tmp[0] / 1609.0)

        #     # ACCELERATIONSOURCE[CALCULATED/MEASURED/UNDEFINED]
        #     row.append(1)

        #     # LINEALG
        #     tmp = self.far_files['AccelerationLongitudinal.far'].get_csv_record(t)
        #     row.append(tmp[0])

        #     # LATERALG
        #     tmp = self.far_files['AccelerationLateral.far'].get_csv_record(t)
        #     row.append(tmp[0])

        #     # GEAR
        #     tmp = self.far_files['Gearbox.far'].get_csv_record(t)
        #     row.append(tmp[1])

        #     # RPM
        #     tmp = self.far_files['RPM.far'].get_csv_record(t)
        #     row.append(tmp[0])

        #     # THROTTLE
        #     tmp = self.far_files['AcceleratorPedal.far'].get_csv_record(t)
        #     row.append(tmp[0] / 100.0)

        #     # BRAKEPRESSURE
        #     tmp = self.far_files['BrakeContact.far'].get_csv_record(t)
        #     row.append(tmp[0])

        #     # STEERINGANGLE
        #     tmp = self.far_files['Steering.far'].get_csv_record(t)
        #     row.append(-tmp[0])

        #     csv_data.append(row)

        # for row in csv_data:
        #     print(','.join([str(e) for e in row]))

        df = pd.DataFrame(index=self.time_index)
        df = df.merge(self.series_index, left_index=True, right_index=True)
        df = df.merge(self.series_lap_index, left_index=True, right_index=True)
        df = df.merge(self.series_date, left_index=True, right_index=True)
        df = df.merge(self.series_time, left_index=True, right_index=True)
        df = df.merge(self.series_time_lap, left_index=True, right_index=True)
        df = df.merge(self.df_gps, left_index=True, right_index=True)
        df = df.merge(self.series_speed_kph, left_index=True, right_index=True)
        df = df.merge(self.series_speed_mph, left_index=True, right_index=True)
        df = df.merge(self.series_height_m, left_index=True, right_index=True)
        df = df.merge(self.series_height_ft, left_index=True, right_index=True)
        df = df.merge(self.series_heading, left_index=True, right_index=True)
        df = df.merge(self.series_distance_km, left_index=True, right_index=True)
        df = df.merge(self.series_distance_mi, left_index=True, right_index=True)
        df = df.merge(self.series_accel_source, left_index=True, right_index=True)
        df = df.merge(self.series_linealg, left_index=True, right_index=True)
        df = df.merge(self.series_lateralg, left_index=True, right_index=True)
        df = df.merge(self.series_gear, left_index=True, right_index=True)
        df = df.merge(self.series_rpm, left_index=True, right_index=True)
        df = df.merge(self.series_throttle, left_index=True, right_index=True)
        df = df.merge(self.series_brake_pressure, left_index=True, right_index=True)
        df = df.merge(self.series_steering, left_index=True, right_index=True)

        print('Harry\'s GPS LapTimer', file=sys.stdout)
        df.to_csv(path_or_buf=sys.stdout, sep=',', header=True, index=False)

        # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None, 'display.memory_usage', None, 'display.width', 20000):
        #     print(df, file=sys.stderr)


if __name__ == '__main__':
    signal(SIGPIPE, SIG_DFL)
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('mpower_file', nargs='?', help='MPower filename')
    args = parser.parse_args()
    mpower_file = MPowerFile(args.mpower_file)
    mpower_file.to_csv()
