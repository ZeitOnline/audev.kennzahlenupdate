"""
Created by: humberg
Date:       07.05.20

This module contains all functions to import user centric relevant data from webtrekk/mapp
"""
import logging

import pandas as pd

from src import api


def get_data(date_from=api.get_datetime_yesterday(),
             date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request
    :param date_from:
    :param date_to:
    :return: dataframe with relevant information
    """
    # build two analysisConfigs, since webtrekk api can only process 30 metrics at once
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisObjects": [{
            "title": "Tage"
        }],
        "metrics": [
        {
            "title": "Visitors *"
        }, {
            "title": "Visitors - angemeldet"
        }, {
            "title": "Abonnenten"
        }, {
            "title": "Abonnenten - angemeldet"
        }, {
            "title": "Browsers, Unique *"
        }, {
            "title": "Browsers, Unique - angemeldet"
        }, {
            "title": "Browsers, Unique - zeit.de"
        }, {
            "title": "Browsers, Unique - zeit.de - ang."
        }, {
            "title": "Browsers, Unique - ZON App"
        }, {
            "title": "Browsers, Unique - ZON App - ang."
        }, {
            "title": "Browsers, Unique - Abonnenten"
        }, {
            "title": "Browsers, Unique - Abonnenten - ang."
        }, {
            "title": "Einstiege *"
        }, {
            "title": "Einstiege - angemeldet"
        }, {
            "title": "Visits *"
        }, {
            "title": "Visits - angemeldet"
        }, {
            "title": "Qualified Visits"
        }, {
            "title": "Visits Stationaer"
        }, {
            "title": "Visits mobile"
        }, {
            "title": "Visits mit Paywall"
        }, {
            "title": "Visits auf Bestellstrecke"
        }, {
            "title": "Page Impressions"
        }, {
            "title": "PIs Schranke Register"
        }, {
            "title": "PIs Schranke Paid"
        }, {
            "title": "PIs Pur"
        }, {
            "title": "Anzahl Bestellungen"
        }, {
            "title": "Anzahl Best. Z  Abo-Schranke nur Red. Marketing"
        }, {
            "title": "Anzahl Bestellungen Z  nur Footerbar"
        }, {
            "title": "Anzahl Bestellungen Z+ gesamt"
        }
        ]}

    analysisConfig2 = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisObjects": [{
            "title": "Tage"
        }],
        "metrics": [
        {
            "title": "Anzahl Bestellungen Pur Only"
        }, {
            "title": "Anzahl Bestellungen Pur Upgrade"
        }, {
            "title": "Anzahl Bestellungen Pur Kombi"
        }, {
            "title": "Anzahl Registrierung SSO"
        }, {
            "title": "Anzahl Registrierungen Schranke"
        }, {
            "title": "Anzahl Login SSO"
        }, {
            "title": "Anzahl Digitalabonnenten"
        }, {
            "title": "Abonnenten - Paid Services - ang."
        }, {
            "title": "Browsers, Unique - Comments"
        }, {
            "title": "Anzahl Best. Z  Abo-Schranke nur Red. Marketing 2"
        }, {
            "title": "Anzahl Bestellungen Z  nur Footerbar 2"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)
    data2 = api.wt_get_data(analysisConfig2)

    # parse data
    data = data["result"]["analysisData"]
    data2 = data2["result"]["analysisData"]
    data_comb = [data[0] + data2[0][1:]]
    df = pd.DataFrame(data_comb)
    col_names = ["date", "visitors", "visitors_ang", "abonnenten", "abonnenten_ang",
                 "b_unique", "b_unique_ang", "b_unique_zeitde", "b_unique_zeitde_ang",
                 "b_unique_zonapp", "b_unique_zonapp_ang", "b_unique_abonnenten",
                 "b_unique_abonnenten_ang", "einstiege", "einstiege_ang", "visits", "visits_ang",
                 "qualified_visits", "visits_stationaer", "visits_mobile", "visits_mit_paywall",
                 "visits_bestellstrecke", "pis", "pis_schranke_register", "pis_schranke_paid",
                 "pis_pur", "best", "best_zplus_red_marketing", "best_zplus_footer",
                 "best_zplus_gesamt", "best_pur_only", "best_pur_upgrade", "best_pur_kombi",
                 "reg_sso", "reg_schranke", "login_sso", "sum_abonnenten",
                 "abonnenten_paid_serv_ang", "b_unique_comments", "best_zplus_red_marketing_2",
                 "best_zplus_footer_2"]
    df.columns = col_names
    df.date = pd.to_datetime(df.date, format="%d.%m.%Y")

    convert_cols = df.columns.drop('date')
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    logging.info('usercentric imported from webtrekk for ' + date_from)

    return df
