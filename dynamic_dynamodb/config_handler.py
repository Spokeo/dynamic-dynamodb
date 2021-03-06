# -*- coding: utf-8 -*-
""" Configuration handler """
import config
from dynamic_dynamodb.statistics.table_metric_buffer import TableMetricBuffer

CONFIGURATION = config.get_configuration()

def get_configured_tables():
    """ Returns a list of all configured tables

    :returns: list -- List of tables
    """
    try:
        return CONFIGURATION['tables'].keys()
    except KeyError:
        return []


def get_global_option(option):
    """ Returns the value of the option

    :returns: str or None
    """
    try:
        return CONFIGURATION['global'][option]
    except KeyError:
        return None

def get_table_metric_buffer(table_name, lookback_period):
    """ returns an instance of TableMetricBuffer
    """
    try:
        if (False == CONFIGURATION['table_metric_buffer_dict'].has_key(table_name)):
            CONFIGURATION['table_metric_buffer_dict'][table_name] = \
                TableMetricBuffer(int(lookback_period) * 60, table_name)
        return CONFIGURATION['table_metric_buffer_dict'][table_name]
    except KeyError:
        return None

def get_gsi_option(table_key, gsi_key, option):
    """ Returns the value of the option

    :type table_key: str
    :param table_key: Table key name
    :type gsi_key: str
    :param gsi_key: GSI key name
    :returns: str or None
    """
    try:
        return CONFIGURATION['tables'][table_key]['gsis'][gsi_key][option]
    except KeyError:
        return None


def get_logging_option(option):
    """ Returns the value of the option

    :returns: str or None
    """
    try:
        return CONFIGURATION['logging'][option]
    except KeyError:
        return None


def get_table_option(table_name, option):
    """ Returns the value of the option

    :type table_name: str
    :param table_name: Name of the DynamoDB table
    :returns: str or None
    """
    try:
        return CONFIGURATION['tables'][table_name][option]
    except KeyError:
        return None
