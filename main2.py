from matplotlib.font_manager import json_dump

from _connect import _connect as _connect_

def main():
    aws_object = _connect_.get_object("awss3")
    from pprint import pprint
    pprint(aws_object.list_buckets())



def get_redshift_data():
    profile_name = "config_prod"
    redshift_obj = _connect_.get_directive("redshift", profile_name)
    db_connect_obj = redshift_obj.auto_connect()

    sql = """with movie_stats as (
select content_id, sum(tvt_millisec) as tvt
from tubidw.video_session
where start_ts >= '2025-02-01' and end_ts <= '2025-02-28' and content_type = 'MOVIE'
group by 1
), movie_info as (
select t1.content_id, t1.tvt, t2.content_name, t2.gracenote_id from movie_stats t1
inner join tubidw.content_info t2 using (content_id)
where content_type = 'MOVIE'
), tvt_rank as (
select *, dense_rank() over(order by tvt desc) as rnk
from movie_info
), tvt_2000 as (
select * from tvt_rank where rnk <= 2000
)

select * from tvt_2000
    """

    result = []

    for each_row in redshift_obj.query(connect_obj=db_connect_obj, query_string=sql):
        id, tvt, title, gid, rnk = each_row
        result.append({"id": id, "tvt": tvt, "title": title, "gid": gid})
    from _util import _util_file as _util_file_
    _util_file_.json_dump("tubi_top_2000.json", result)
    _util_file_.json_to_csv("tubi_top_2000.csv", result)


def _869232():
    from projects._869232 import solution
    solution.solution("/Users/jian.huang/Downloads/gracenote_ids.txt",
                      "/Users/jian.huang/Downloads/Feb2025TVTLIVETitles_2025-3-12_1834.csv")

def _888603():
    from projects._888603 import solution
    solution.solution("/Users/jian.huang/Downloads/No Preview List Top 1000 Titles by TVT-2025-03-13.csv")

def _try_monte():


    pymonte_obj = _connect_.get_api("pycarlo", "config_dev")
    pymonte_obj.get_alert()





if __name__ == '__main__':
    _try_monte()
    exit(0)
    _888603()
    exit(0)
    _869232()
    exit(0)
    get_redshift_data()
    exit(0)
    main()