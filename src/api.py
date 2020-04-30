"""
Created by: humberg
Date:       29.04.20

This module contains all functions related to api calls
"""

import requests
import json


def f3_call(body):
    """
    function to access IVW data from reshin
    :param body: body of api call
    :return: JSON of data
    """

    # get token
    token = requests.post(
        url='https://app.live2.reshin.de/api/auth/access-token',
        data={
            'grant_type': 'client_credentials',
            'client_id': 'zeit-api_user_ivwdaten',
            'client_secret': 'apikey-d8c0bc73-0355-44a0-825e-06f5eb63f2b4',
            'scope': 'org.reshin.app.zeit.dashboard'
        },
        headers={
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    )
    # convert dict to JSON
    data_json = json.dumps(body)
    # get data
    response = requests.post(
        url='https://app.live2.reshin.de/api/es/zeit-dashboard/idas/_search',
        headers={
            'Authorization': 'Bearer ' + json.loads(token.text)['access_token'],
            'Content-Type': 'application/json'
        },
        data=data_json
    )
    # parse data
    response_json = json.loads(response.text)
    return response_json


def f3_body(aggr, horizon):
    """
    wrapper function to call f3 api and get data
    :param aggr: aggr ("hour" or "day");
    :param horizon: horizon of days to be called
    :return: json with response of specific call
    """

    # define time intervall
    date_from = "now/d-"+str(horizon)+"d"
    date_to = "now/d-1d"
    # define body
    data = {
        "aggs": {
            "2": {
                "date_histogram": {
                    "field": "@timestamp",
                    "interval": "1d",
                    "time_zone": "Europe/Berlin",
                    "min_doc_count": 1
                },
                "aggs": {
                    "3": {
                        "filters": {
                            "filters": {
                                "stationaer": {
                                    "query_string": {
                                        "query": "+idasid:zeitonl",
                                        "analyze_wildcard": True,
                                        "default_field": "*"
                                    }
                                },
                                "mobile": {
                                    "query_string": {
                                        "query": "+idasid:mobzeit",
                                        "analyze_wildcard": True,
                                        "default_field": "*"
                                    }
                                },
                                "Android App": {
                                    "query_string": {
                                        "query": "+idasid:aadzeion",
                                        "analyze_wildcard": True,
                                        "default_field": "*"
                                    }
                                },
                                "iOS App": {
                                    "query_string": {
                                        "query": "+idasid:appzeion",
                                        "analyze_wildcard": True,
                                        "default_field": "*"
                                    }
                                },
                                "ZE.TT Android App": {
                                    "query_string": {
                                        "query": "+idasid:aadzett",
                                        "analyze_wildcard": True,
                                        "default_field": "*"
                                    }
                                },
                                "ZE.TT iOS App": {
                                    "query_string": {
                                        "query": "+idasid:appzett",
                                        "analyze_wildcard": True,
                                        "default_field": "*"
                                    }
                                },
                                "Gesamt": {
                                    "query_string": {
                                        "query": "*",
                                        "analyze_wildcard": True,
                                        "default_field": "*"
                                    }
                                }
                            }
                        },
                        "aggs": {
                            "1": {
                                "sum": {
                                    "field": "sessions"
                                }
                            }
                        }
                    }
                }
            }
        },
        "size": 0,
        "_source": {
            "excludes": []
        },
        "stored_fields": [
            "*"
        ],
        "script_fields": {},
        "docvalue_fields": [
            {
                "field": "@timestamp",
                "format": "date_time"
            }
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "match_all": {}
                    },
                    {
                        "query_string": {
                            "query": "+aggr:" + aggr + " +idastype:dim_sessions +idasid: mobzeit,aadzeion,appzeion,zeitonl,aadzett,appzett*",
                            "analyze_wildcard": True,
                            "default_field": "*"
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": date_from,
                                "lte": date_to,
                                "format": "epoch_millis"
                            }
                        }
                    }
                ],
                "filter": [],
                "should": [],
                "must_not": []
            }
        }
    }
    return data