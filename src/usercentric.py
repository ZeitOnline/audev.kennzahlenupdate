"""
Created by: humberg
Date:       07.05.20

This module contains all functions to import user centric relevant data from webtrekk/mapp
"""

from src import api
import pandas as pd


def get_data(date_from=api.get_datetime_yesterday(),
             date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request
    :param date_from:
    :param date_to:
    :return: dataframe with relevant information
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisObjects": [{
            "title": "Tage"
        }],
        "metrics": [
        {
            "title": "Visitors"
        }, {
            "title": "Visits",
            "metricFilter": {
                "filterRules": [{
                    "objectTitle": "Einstiegsseite",
                    "comparator": "=",
                    "filter": "*www.zeit.de/index",
                    "scope": "visit"
                }]
            }
        }, {
            "title": "Anzahl Bestellungen"
        }, {
            "title": "Anzahl Bestellungen Z+ gesamt"
        }, {
            "title": "CR Bestellungen Schranke"
        }, {
            "title": "Anzahl Registrierung SSO"
        }, {
            "title": "Anzahl Registrierungen Schranke"
        }, {
            "title": "CR Registrierungen Schranke"
        }, {
            "title": "PI pro Visit"
        }, {
            "title": "Loginquote - zeit.de"
        }, {
            "title": "Loginquote - ZON App"
        }, {
            "title": "Anteil Abonnenten (Paid Visitors)"
        }, {
            "title": "Anteil Abonnenten (angemeldet Visitors)"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["date", "visitors", "qualified_visits", "anzahl_bestellungen",
                 "anzahl_bestellungen_zplus_gesamt", "cr_bestellungen_schranke",
                 "anzahl_registrierung_sso", "anzahl_registrierung_schranke",
                 "cr_registrierung_schranke", "pi_pro_visit", "loginquote_zeitde",
                 "loginquote_zonapp", "anteil_abonnenten_paid", "anteil_abonnenten_angemeldet"]
    df.columns = col_names
    df.date = pd.to_datetime(df.date, format="%d.%m.%Y")

    convert_cols = df.columns.drop('date')
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    return df