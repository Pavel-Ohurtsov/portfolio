import sys
sys.path.append('/data/common')
from lib import *
from datetime import timedelta


def new_check_stat(view, where, day):
    check = clickhouse(f"""select * from (
            select toStartOfHour(event_time) dt, count(*) zoon_stat from zoon.stat
            where ({where})
            and event_date = '{day}'
            group by dt
        ) _
        left join (
            select toStartOfHour(event_time) dt, count(*) view_stat from public.{view}
            where event_date = '{day}'
            group by dt
        ) __
        using(dt)
        where zoon_stat != view_stat
        order by dt""")
    return check

def pipeline():
    # используем ячейку если хотим проверить все доступные к даливке вьюхи на пропуски в данных
    tables = clickhouse("""show tables from public where name not like '.%'""")
    tables = tables['name'].to_list()

    mater_views = []
    for table in tables:
        describ = clickhouse(f"""show create table public.{table}""")
        stroka = describ['statement'][0].lower()
        if stroka.find('materialized view') != -1 and stroka.find('partition by') == -1:
            mater_views.append(table)


    views_wheres = {}
    for table in mater_views:
        definition = clickhouse(f"""show create table public.{table}""")['statement'][0]
        where_index = definition.lower().find('where')
        where = definition[where_index+5:].strip()
        views_wheres[table] = where

    views_selects = {}
    for table in mater_views:
        select = clickhouse(f"""describe table public.{table}""")['name'].replace("prof_id", "multiIf(object_type = 'prof', object_id, ev_sourceId) prof_id")
        views_selects[f'{table}'] = ', '.join(select)

    # здесь выбираем кол-во последних дней, за которые мы хотим проверить и долить данные
    days_back = 30
    first_day = dt.today() - timedelta(days=days_back)
    last_day = dt.today() - timedelta(days=1)

    daterange_df = pd.DataFrame(pd.date_range(first_day, last_day, normalize=True))
    daterange_df['str_date'] = daterange_df[0].apply(lambda x: str(x)[:-9])
    daterange_df = daterange_df.drop(0, axis=1)


    #print('Проверка и доливка таблиц')
    full_text = 'Долиты данные в таблицы ClickHouse: \n'
    text_corp = ''
    
    for view, where in tqdm(views_wheres.items()):
        print(view)
        text_other = ''
        problem_days = []
        corp_days = 0
        for day in tqdm(daterange_df['str_date']):
            check = new_check_stat(view, where, day)
            #print(day)
            if len(check) > 0:
                problem_days.append(day)
            if (view == 'stat_corp')  & (len(check) > 0): # исправить на 0
                corp_days += 1
            if (day == daterange_df['str_date'].to_list()[-1]) & (len(problem_days) > 0) & (view != 'stat_corp'):
                str_problem_days = str(problem_days)[1:-1]
                clickhouse(f"""
                        alter table public.{view} delete where event_date in ({str_problem_days})
                        """)

                clickhouse(f"""
                        insert into public.{view}
                        select
                        {views_selects[view]}
                        from zoon.stat
                        where
                        {views_wheres[view]}
                        and event_date in ({str_problem_days})
                        """)
                if len(problem_days) == 1:
                    text_other = f'\n{view} (за {len(problem_days)} день)'
                elif len(problem_days) > 1 and len(problem_days) < 5:
                    text_other = f'\n{view} (за {len(problem_days)} дня)'
                elif len(problem_days) >= 5:
                    text_other = f'\n{view} (за {len(problem_days)} дней)'  
            if text_other != '':
                full_text += text_other
        if corp_days == 1:
            text_corp = f'\n Не хватает данных в ClickHouse stat_corp за {corp_days} дней'
            bot.send_message(analytics_chat_id, str(text_corp))
        elif corp_days > 1 and corp_days < 5:
            text_corp = f'\n Не хватает данных в ClickHouse stat_corp за {corp_days} дня'
            bot.send_message(analytics_chat_id, str(text_corp))
        elif corp_days >= 5:
            text_corp = f'\n Не хватает данных в ClickHouse stat_corp за {corp_days} дней'   
            bot.send_message(analytics_chat_id, str(text_corp))
    if full_text != 'Долиты данные в таблицы ClickHouse: \n':
        bot.send_message(analytics_chat_id, str(full_text))
    if full_text == 'Долиты данные в таблицы ClickHouse: \n' and corp_days == 0:
        bot.send_message(analytics_chat, 'Все вьюхи ClickHouse полные')
                    
if __name__=='__main__':
    pipeline()
                    