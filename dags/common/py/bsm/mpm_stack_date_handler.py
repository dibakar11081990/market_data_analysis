# Imports
from datetime import datetime, timedelta



#############################################################

def mpm_stack_date_handler(airflow_var_dict, delta=15):
    """
    This function manipulates the start/end dates for the mpm facts
    so that each dag run refers to eiether legacy stack or the new stack not both.

    :param airflow_var_dict:
    :param delta:
    :return:
    """

    # try getting the start_date and end_date f
    start_date = airflow_var_dict.get('start_date', None)
    end_date = airflow_var_dict.get('end_date', None)

    # Legacy stack cutoff date.
    legacy_stack_cutoff_date = datetime(2020, 1, 31, 23, 59, 59)

    # Handling condition where start and end time is passed via airflow variable.
    if start_date is not None and end_date is not None:
        # convert both to datetime objects.
        dt_start_date = datetime.strptime(str(start_date), '%Y%m%d')
        dt_end_date = datetime.strptime(str(end_date), '%Y%m%d')

        # check if any mismatches are present in the start date and end date.
        if dt_end_date > legacy_stack_cutoff_date >= dt_start_date:
            dt_start_date = datetime(2020, 2, 1, 0, 0, 0)
        elif dt_end_date < legacy_stack_cutoff_date < dt_start_date:
            dt_end_date = dt_start_date

    # Handling condition where its a scheduled job.
    else:
        dt_end_date = datetime.now()
        interim_start_date = dt_end_date - timedelta(days=delta)
        dt_start_date = legacy_stack_cutoff_date + timedelta(days=1) \
            if interim_start_date < legacy_stack_cutoff_date else interim_start_date

    return dt_start_date.strftime('%Y%m%d'), dt_end_date.strftime('%Y%m%d')
