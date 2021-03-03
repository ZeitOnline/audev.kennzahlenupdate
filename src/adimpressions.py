"""
Created by: humberg
Date:       03.06.20

This module contains all functions to retrieve the ad impressions data from webtrekk (or ad manager
api)
"""
from src import api
import pandas as pd
import logging
from datetime import datetime
import googleads
import tempfile

# disable default logging of module
logging.getLogger("googleads").setLevel(logging.WARNING)

def get_data(date_from=api.get_datetime_yesterday(),
             date_to=api.get_datetime_yesterday()):
    """
    function to build analysisConfig and make api request
    :param date_from:
    :param date_to:
    :return: dataframe with relevant information [date, ai_stationaer, ai_mobile, ai_hp_stationaer,
                                                    ai_hp_mobile]
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisObjects": [{
            "title": "Tage"
        }],
        "metrics": [{
            "title": "AI stationaer gesamt"
        }, {
            "title": "AI mobile gesamt"
        }, {
            "title": "AI HP stationaer"
        }, {
            "title": "AI HP mobile"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["date", "ai_stationaer", "ai_mobile", "ai_hp_stationaer", "ai_hp_mobile"]
    df.columns = col_names
    df.date = pd.to_datetime(df.date, format="%d.%m.%Y")

    convert_cols = df.columns.drop('date')
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    logging.info('ad impressions imported from webtrekk for ' + date_from)

    return df


def get_data_admanager(date_from=api.get_datetime_yesterday(),
                       date_to=api.get_datetime_yesterday()):
    """
    function establishes connection to ad manager api and gets adimpressions
    :param date_from: date_from as string
    :param date_to: date_to as string
    :return: dataframe [date, ai_stationaer, ai_mobile, ai_hp_stationaer, ai_hp_mobile]
    """
    # set key file
    key_file = 'admanager-auth.json'
    application_name = 'AdManager API Export'
    network_code = 183

    # Initialize the GoogleRefreshTokenClient
    oauth2_client = \
        googleads.oauth2.GoogleServiceAccountClient(key_file,
                                                    googleads.oauth2.GetAPIScope('ad_manager'))

    # Initialize the Ad Manager client.
    ad_manager_client = \
        googleads.ad_manager.AdManagerClient(oauth2_client, application_name, network_code,
                                             cache=googleads.common.ZeepServiceProxy.NO_CACHE)

    # create dictionary with all report informations
    report_dict = create_admanager_dict()

    # initialize dict
    value_dict = {'date': date_from}

    # run report job and extract data for each adimpressions report
    for cur_report in [*report_dict]:
        cur_adimp = run_admanager_job(date_from=date_from, date_to=date_to,
                                      report_dict=report_dict[cur_report],
                                      client=ad_manager_client)
        value_dict[cur_report] = cur_adimp

    df = pd.DataFrame([value_dict])
    df.date = pd.to_datetime(df.date, format="%Y-%m-%d")

    return df


def create_admanager_dict():
    """
    creates a dictionary with relevant information to query admanager report
    :return: dict with information regarding filter, parent_flag and webtrekk_id
    """
    report_dict = {
        'ai_stationaer': {
            'filter': [39375618685, 4155085],
            'parent_flag': False,
            'webtrekk_id': 11
        },
        'ai_mobile': {
            'filter': [39375618805, 13739725],
            'parent_flag': False,
            'webtrekk_id': 12
        },
        'ai_hp_stationaer': {
            'filter': [39375618685, 13869325],
            'parent_flag': True,
            'webtrekk_id': 13
        },
        'ai_hp_mobile': {
            'filter': [39375618805, 13739845],
            'parent_flag': True,
            'webtrekk_id': 14
        },
    }

    return report_dict


def run_admanager_job(date_from=api.get_datetime_yesterday(),
                      date_to=api.get_datetime_yesterday(),
                      report_dict=None,
                      client=None):
    """
    create statement, runs report job and retrieves job information; also extracts only relevant
    data
    :param: date_from: date_from as string
    :param: date_to: date_to as string
    :param: dict: dictionary with specific information for getting adimpression data
    :param: client: google ad manager client
    :return: adimpressions, depending on input dict for specific date
    """
    # set variables from input dict
    custom_targeting_value_id = report_dict['filter'][0]
    ad_unit_id = report_dict['filter'][1]
    parent_flag = report_dict['parent_flag']

    # set where condition; if parent_flag=True use PARENT_AD_UNIT_ID variable name
    if parent_flag:
        where_condition = 'CUSTOM_TARGETING_VALUE_ID = :customTargetingValueId AND ' \
                          'PARENT_AD_UNIT_ID = :adUnitId'
    else:
        where_condition = 'CUSTOM_TARGETING_VALUE_ID = :customTargetingValueId AND ' \
                          'AD_UNIT_ID = :adUnitId'

    # convert string to datetime object; required for API
    date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
    date_to = datetime.strptime(date_to, '%Y-%m-%d').date()

    # Initialize a DataDownloader.
    report_downloader = client.GetDataDownloader(version='v202011')

    # Create statement object to filter
    statement = (googleads.ad_manager.StatementBuilder(version='v202011')
                 .Where(where_condition)
                 .WithBindVariable('customTargetingValueId', custom_targeting_value_id)
                 .WithBindVariable('adUnitId', ad_unit_id)
                 .Limit(None)
                 .Offset(None))

    # Create report job.
    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'AD_UNIT_ID', 'CUSTOM_CRITERIA'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS'],
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': date_from,
            'endDate': date_to,
            'statement': statement.ToStatement()
        }
    }

    # Run the report and wait for it to finish.
    report_job_id = report_downloader.WaitForReport(report_job)

    # download report job and save as CSV
    report_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
    report_downloader.DownloadReportToFile(report_job_id, export_format='CSV_DUMP',
                                           outfile=report_file, use_gzip_compression=False)
    report_file.close()

    # extract report data
    with open(report_file.name, 'rt') as report:
        df = pd.read_csv(report)

    return df['Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS'][0]

