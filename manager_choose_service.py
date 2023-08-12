# This service takes input data about a closed deal and a list of managers,
# and selects the most suitable manager to work with this deal

import sys
sys.path.append('../common')

import datetime
import logging
import numpy as np
import pandas as pd

import db_config
from pymongo import MongoClient
from bson.objectid import ObjectId
from lib import vertica

import mysql.connector
import json
import pandahouse_wrapper

log = logging.getLogger(__name__)

# Defining the class
class WelcomeCallImpl():
    def __init__(self):
        self.zmain = MongoClient(db_config.zmain_cluster)['zoon']
        self.collection = MongoClient(db_config.zds_uri)['log']['welcome_call_processed_deals']
        self.ch_logs = pandahouse_wrapper.pandahouse_wrapper(db_config.ch_logs_native)

    # Method for fetching data from MongoDB
    def get_transaction_ids_str(self, crm_deal_id):
        deal_data = self.zmain['crm_deal'].find_one(
            {'_id': ObjectId(f'{crm_deal_id}')},
            {'transaction_ids', 'owner', 'status'}
        )
        
        # Here we perform checks and raise errors if necessary
        if deal_data is None:
            raise Exception('crm_deal not found')
        if deal_data['status'] != 'won':
            raise Exception("crm_deal status not 'won'")
        
        try:
            transactions_str = str(deal_data['transaction_ids'])[1:-1]
        except:
            raise Exception('crm_deal transaction_ids do not exist')

        return transactions_str

    # Method returning a dataframe with managers and the count of organizations they have for renewal in the given month
    def get_transaction_info(self, transactions_str, start_date):
        # We determine which service has generated more money,
        # if it's based on reputation, we take the number of days from reputation,
        # if it's based on premium, we take the number of premium days


        cnx = mysql.connector.connect(**db_config.mysql_connection)
        cursor = cnx.cursor()
        query = f'SELECT data FROM zoon.BillingTransaction WHERE id in ({transactions_str})'
        cursor.execute(query)
        trs = [json.loads(data) for (data,) in cursor]
        cursor.close()
        cnx.close()

        df = pd.DataFrame(
            columns=['cnt', 'sum1','sum2','day1','day2'], 
            data = [(d.get('count',1),d.get('day_sum'),d.get('reputation_sum'),d['features'].get('premium'),d['features'].get('reputation')) for d in trs],
        ).fillna(0)
        df = df.sum()
        if df['sum1']>df['sum2']:
            main_service_days = df['day1']
        else:
            main_service_days = df['day2']
        
        if main_service_days < 30:
            log.error('product duration less than 30 days', extra = {'transactions': transactions_str})
            main_service_days = 30
        
        return df['cnt'], start_date + datetime.timedelta(days=int(main_service_days))

    
    #Method that uploads data on the total number of organizations assigned to the PM

    def manager_orgs_func(self, managers, start_date, end_date):
        
        start_month_str = start_date.strftime(format="%Y-%m-01")
        end_month_str = end_date.strftime(format="%Y-%m-01")
        
        manager_orgs = vertica("""
            select account_id, orgs total_orgs
            from public.orgs_per_account_manager
            """)
        
        manager_orgs = manager_orgs[manager_orgs['account_id'].isin(managers)]
        
        manager_orgs_month = vertica(f"""
            select 
                pm_id as account_id,
                count(distinct case when date_trunc('month', ts_from) = '{start_month_str}' then org_id end) as start_month_orgs,
                count(distinct case when date_trunc('month', ts_to) = '{end_month_str}' then org_id end) as end_month_orgs
            from public.new_autoplan 
            where (type in ('продление1', 'продление2')
                    and product IN ('premium_norm','premium_lite','reputation'))
                and (date_trunc('month', ts_from) = '{start_month_str}'
                    or date_trunc('month', ts_to) = '{end_month_str}')
            group by pm_id
            """)
        
        
        manager_orgs = manager_orgs.merge(manager_orgs_month, on='account_id', how='left')
  
        
        return manager_orgs


     # Method uploaded data to mongoDB
    def write_deals(self, crm_deal_id, manager, orgs_in_deal, unapplied_filters):
        ts = datetime.datetime.now()
        post = {
            'crm_deal_id': crm_deal_id,
            'ts': ts, 
            'manager_id': int(manager),
            'orgs_in_deal': int(orgs_in_deal),
            'unapplied_filters': unapplied_filters,
        }
        print('LOG: ', post)
        self.collection.insert_one(post)
        
        if len(unapplied_filters):
            del post['ts']
            log.debug('cannot determine manager', extra = post)

    # Method that selects a suitable manager and records the number of assigned organizations per manager into the database
    def main_choose_manager(self, manager_orgs, processed_deals, orgs_in_deal, start_date, end_date):

        filters_skipped = []

        empty_df = pd.DataFrame({'_id': [np.nan], 'manager_id': [1], 'orgs_in_deal': [1], 'ts': [pd.NaT]})
        processed_deals = pd.concat([processed_deals, empty_df])
        manager_logs = pd.DataFrame(processed_deals.groupby(['manager_id'])['orgs_in_deal'].sum()) \
            .reset_index().rename(columns={'manager_id': 'account_id', \
                                           'orgs_in_deal': 'today_orgs'})
        manager_orgs = manager_orgs.merge(manager_logs, on='account_id', how='left').fillna(0)

        manager_orgs['total_orgs_with_today'] = manager_orgs['total_orgs'] + manager_orgs['today_orgs']
        manager_orgs['end_month_orgs_with_today'] = manager_orgs['end_month_orgs'] + manager_orgs['today_orgs']
        manager_orgs['start_month_orgs_with_today'] = manager_orgs['start_month_orgs'] + manager_orgs['today_orgs']
        manager_orgs = manager_orgs[['account_id', 'total_orgs_with_today', 'end_month_orgs_with_today', \
                                     'start_month_orgs_with_today', 'today_orgs']]

        if len(manager_orgs[manager_orgs['start_month_orgs_with_today'] + orgs_in_deal <= 20]) > 0:
            manager_orgs = manager_orgs[manager_orgs['start_month_orgs_with_today'] + orgs_in_deal <= 20]
        else:
            filters_skipped.append('start_month_orgs_less_than_20')

        if len(manager_orgs[manager_orgs['end_month_orgs_with_today'] + orgs_in_deal <= 15]) > 0:
            manager_orgs = manager_orgs[manager_orgs['end_month_orgs_with_today'] + orgs_in_deal <= 15]
        else:
            filters_skipped.append('end_month_orgs_less_than_15')

        if len(manager_orgs[manager_orgs['total_orgs_with_today'] + orgs_in_deal <= 140]) > 0:
            manager_orgs = manager_orgs[manager_orgs['total_orgs_with_today'] + orgs_in_deal <= 140]
        else:
            filters_skipped.append('total_orgs_less_than_140')

        if len(manager_orgs[manager_orgs['today_orgs'] + orgs_in_deal <= 3]) > 0:
            manager_orgs = manager_orgs[manager_orgs['today_orgs'] + orgs_in_deal <= 3]
        else:
            filters_skipped.append('today_orgs_less_than_3')

        novices = self.ch_logs.read_clickhouse(f"""
            select object_id account_id, date_diff(day, min(ts), today()) as day_from_first_log
            from logs.admin_log
            where object_type in ('user', 'user_data')
                and object_id in (%(account_id)s)
            group by object_id
            having day_from_first_log < 183
        """,{'account_id': manager_orgs['account_id'].to_list()})

        if len(manager_orgs[manager_orgs['account_id'].isin(novices['account_id'])]) > 0:
            manager_orgs = manager_orgs[manager_orgs['account_id'].isin(novices['account_id'])]
        else:
            filters_skipped.append('novice')

        right_manager = manager_orgs['account_id'].sample(n=1).iloc[0]

        return right_manager, filters_skipped

    # Final method that checks if the deal has been processed before, and if not, it triggers all other functions in order
    def define_manager(self, crm_deal_id, managers, start_date=None):
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else datetime.date.today()

        today_first_second = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        processed_deals = pd.DataFrame(self.collection.find({'ts': {'$gte': today_first_second}}))
        if len(processed_deals[processed_deals['crm_deal_id'] == crm_deal_id]) > 0:
            self.collection.delete_many({'_id': {'$in': processed_deals[processed_deals['crm_deal_id'] == crm_deal_id]['_id'].tolist()}})
            processed_deals = processed_deals.loc[processed_deals['crm_deal_id'] != crm_deal_id]

        transactions_str = self.get_transaction_ids_str(crm_deal_id)

        orgs_in_deal, end_date = self.get_transaction_info(transactions_str, start_date)
        manager_orgs = self.manager_orgs_func(managers, start_date, end_date)
        manager, unapplied_filters = self.main_choose_manager(manager_orgs, processed_deals, orgs_in_deal, start_date, end_date)

        self.write_deals(crm_deal_id, manager, orgs_in_deal, unapplied_filters)
        return {'manager_id': manager, 'unapplied_filters': unapplied_filters}

class WelcomeCall:
    def __init__(self):
        self.impl = WelcomeCallImpl()
    def define_manager(self, crm_deal_id, managers, start_date=None):
        return self.impl.define_manager(crm_deal_id, managers, start_date)

__all__ = ['WelcomeCall']
