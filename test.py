import logging
import sys
import time

from dynamodb_timeseries import TimeSeries
from pprint import pprint

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if __name__ == '__main__':
    # end_ts = int(time.time() * 1000)
    # start_ts = end_ts - (86400000 * 30)
    # start_ts = 1575180000000 # 12/1/2019
    # end_ts = 1579845599999
    # tags = [
    #     # 'oil-tank-15-1h-3-alisa-ogden:oil',
    #     # 'oil-tank-15-1h-3-alisa-ogden:water',
    #     # 'oil-tank-15-1h-3-alisa-ogden:overall-level',
    #     # 'oil-tank-15-1h-3-alisa-ogden:oil-change-last-hour',
    #     # 'oil-tank-15-1h-4-alisa-ogden:oil',
    #     # 'oil-tank-15-1h-4-alisa-ogden:oil-barrel',
    #     # 'oil-tank-15-1h-4-alisa-ogden:water',
    #     # 'oil-tank-15-1h-4-alisa-ogden:water-barrel',
    #     # 'oil-tank-15-1h-4-alisa-ogden:overall-level',
    #     # 'oil-tank-15-1h-5-alisa-ogden:oil',
    #     # 'oil-tank-15-1h-5-alisa-ogden:oil-barrel',
    #     # 'oil-tank-15-1h-5-alisa-ogden:water',
    #     # 'oil-tank-15-1h-5-alisa-ogden:water-barrel',
    #     # 'oil-tank-15-1h-5-alisa-ogden:overall-level',
    #     # '1h-oil-tank-1-sioux:oil',
    #     # '1h-oil-tank-1-sioux:oil-barrel',
    #     # '1h-oil-tank-1-sioux:water',
    #     # '1h-oil-tank-1-sioux:water-barrel',
    #     # '1h-oil-tank-1-sioux:overall-level',
    #     # '1h-oil-tank-2-sioux:oil',
    #     # '1h-oil-tank-2-sioux:oil-barrel',
    #     # '1h-oil-tank-2-sioux:water',
    #     # '1h-oil-tank-2-sioux:water-barrel',
    #     # '1h-oil-tank-2-sioux:overall-level',
    #     # '1h-oil-tank-3-sioux:oil',
    #     # '1h-oil-tank-3-sioux:oil-barrel',
    #     # '1h-oil-tank-3-sioux:water',
    #     # '1h-oil-tank-3-sioux:water-barrel',
    #     # '1h-oil-tank-3-sioux:overall-level',
    #     # '1h-oil-tank-4-sioux:oil',
    #     # '1h-oil-tank-4-sioux:oil-barrel',
    #     # '1h-oil-tank-4-sioux:water',
    #     # '1h-oil-tank-4-sioux:water-barrel',
    #     # '1h-oil-tank-4-sioux:overall-level',
    # ]
    ts = TimeSeries('timeseries-caza')
    start_ts = 1516773600000
    end_ts = 1579902869660
    sort = 'desc'
    limit = 0
    # tags = [
    #     'well-5h-sioux:casing-pressure-transmitter',
    #     'well-5h-sioux:tubing-pressure-transmitter',
    #     'well-5h-separator-sioux:gas-meter-cumulative-total',
    #     'well-5h-separator-sioux:gas-meter-todays-total',
    #     'well-5h-separator-sioux:gas-meter-yesterdays-total',
    #     'well-5h-separator-sioux:oil-meter-cumulative-total',
    #     'well-5h-separator-sioux:oil-meter-last-hour',
    #     'well-5h-separator-sioux:oil-meter-todays-total',
    #     'well-5h-separator-sioux:oil-meter-yesterdays-total',
    #     'well-5h-separator-sioux:water-meter-cumulative-total',
    #     'well-5h-separator-sioux:water-meter-todays-total',
    #     'well-5h-separator-sioux:water-meter-yesterdays-total',
    # ]
    #     "well-5h-sioux": ["casing-pressure-transmitter", "tubing-pressure-transmitter"],
    #     "well-5h-separator-sioux": ["", "", "", "", "", "", "", "", "", ""]
    # }
    tags = [
        # 'well-15-1h-separator-alisa-ogden:oil-meter-yesterdays-total/avg:1d',
        # 'well-15-1h-separator-alisa-ogden:water-meter-yesterdays-total/avg:1d',
        'well-15-1h-separator-alisa-ogden:gas-meter-yesterdays-total/avg:1d',
    ]
    start = time.perf_counter()
    pprint(ts.query(tags, start_ts, end_ts, order='asc'))
    end = time.perf_counter()
    print(f'Time: {end-start}s')
