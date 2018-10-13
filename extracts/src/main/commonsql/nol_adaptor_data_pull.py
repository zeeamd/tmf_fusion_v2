"""
File: nol_adaptor_data_pull.py
Author: KiranK

This module contains data pull sql's

"""

def get_report_credit_sql(sch_nm):
    """
    Prepares SQL string for getting report credit info - TO BE Used as supporting function in others

    :param kwargs:
       - schema_name
    :return: SQL string

    Example:
        get_metered_person_wt_sql(sch_nm)

    """
    #utils.writeToLog(lvl, "get_report_credit_sql() fds_schema:{0}")

    sql = """ SELECT rc.report_credit_key,
       rc.report_credit_id,
       rc.report_credit_type_code,
       rc.report_credit_type_desc,
       rc.report_credit_level_code,
       rc.report_credit_level_desc,
       rc.report_credit_name,
       rc.up_level_report_media_credit_id,
       rc.top_level_report_media_credit_id,
       rc.internet_category_id,
       rc.internet_category_name,
       rc.internet_subcategory_id,
       rc.internet_subcategory_name,
       rc.ad_support_level_code,
       rc.ad_support_level_desc,
       rc.is_adult_site_flag,
       rc.is_ad_network_report_credit_flag,
       rc.is_regular_report_credit_flag,
       rc.is_client_defined_report_credit_flag,
       rc.encoded_bit_flags,
       rc.parent_report_credit_id,
       rc.parent_report_credit_name,
       rc.brand_report_credit_id,
       rc.brand_report_credit_name,
       rc.channel_report_credit_id,
       rc.channel_report_credit_name,
       rc.application_report_credit_id,
       rc.application_report_credit_name,
       rc.country_id,
       rc.country_name,
       rcp.start_period_id,
       rcp.end_period_id,
       rcp.period_type_code,
       intab.intab_period_start_date as intab_period_date,
       rcp.released_for_processing_flag,
       rcp.creation_datetime,
       rcp.load_id,
       rcp.last_updated_datetime,
       rcp.last_updated_by_load_id
FROM {sch_nm}.report_credit rc
JOIN {sch_nm}.report_credit_period_monthly rcp ON rc.report_credit_key = rcp.report_credit_key
JOIN {sch_nm}.intab_period intab ON intab.intab_period_id = rcp.start_period_id
AND intab.intab_period_type_code = rcp.period_type_code
WHERE rcp.released_for_processing_flag = "Y" """.format(sch_nm=sch_nm)

    # utils.writeToLog(lvl, "get_report_credit_sql() {0}".format(sql))
    return sql


def get_nol_us_200brands(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm):
    """
    Prepares SQL string for getting NOL US 200 brands info - TO BE Used as supporting function in others

    :param kwargs:
       - intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm
    :return: SQL string

    Example:
        get_nol_us_200brands(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm)

    """

    sql = """
SELECT a.period_id,
       a.rn_id,
       a.brand_id,
       a.brand_name,
       a.surf_location_id,
       a.hits,
       a.duration
FROM
  (SELECT a.intab_period_id AS period_id,
          b.source_panelist_id AS rn_id,
          c.brand_report_credit_id AS brand_id,
          c.brand_report_credit_name AS brand_name,
          b.surf_location_id,
          sum(a.number_of_page_views) AS hits,
          sum(a.time_spent_in_seconds) AS duration
   FROM {sch_nm}.internet_person_page_view_usage_fact a,
        {sch_nm}.metered_internet_location_person_weight b,
        {tab_nm} c
   WHERE a.person_media_consumer_id=b.person_media_consumer_id
     AND a.report_credit_key = c.report_credit_key
     AND a.released_for_processing_flag = "Y"
     AND b.released_for_processing_flag = "Y"
     AND c.released_for_processing_flag = "Y"
     AND a.type_of_media_event_code = 2
     AND a.report_credit_level_code NOT IN (11,
                                            12)
     AND a.media_event_location_code IN (1,
                                         3)
     AND a.sample_collection_method_key NOT IN (43)
     AND a.intab_period_id = ("{intab_prd_id}")
     AND b.intab_period_id = ("{intab_prd_id}")
     AND c.intab_period_date BETWEEN ("{intab_prd_stdt}") AND ("{intab_prd_endt}")
     AND c.intab_period_date = ("{intab_prd_stdt}")
     AND c.report_credit_level_code IN (2,
                                        3)
   GROUP BY 1,
            2,
            3,
            4,
            5)a,

  (SELECT b.brand_report_credit_id AS brand_id,
          a.unique_audience_weighted/1000 unique_audience
   FROM {sch_nm}.internet_report_credit_summary_fact a,
        {tab_nm} b
   WHERE a.report_credit_key = b.report_credit_key
     AND a.released_for_processing_flag = "Y"
     AND b.RELEASED_FOR_PROCESSING_FLAG = "Y"
     AND a.intab_period_id = ("{intab_prd_id}")
     AND b.intab_period_date BETWEEN ("{intab_prd_stdt}") AND ("{intab_prd_endt}")
     AND b.intab_period_date = ("{intab_prd_stdt}")
     AND a.intab_period_type_code = 130
     AND a.rollup_category_id =2
     AND a.internet_summary_demographic_key =0
     AND a.type_of_media_event_code=2
     AND a.REPORT_CREDIT_LEVEL_CODE NOT IN (11,
                                            12)
     AND a.MEDIA_EVENT_LOCATION_CATEGORY_CODE IN (5)
     AND filter_criteria_id = 0
     AND a.rollup_category_id =2
     AND a.internet_summary_demographic_key =0
     AND a.REPORT_CREDIT_LEVEL_CODE IN (2)
   ORDER BY 2 DESC
   LIMIT 200)b
WHERE a.brand_id=b.brand_id""".format(intab_prd_id=intab_prd_id, intab_prd_stdt=intab_prd_stdt,
                                      intab_prd_endt=intab_prd_endt, sch_nm=sch_nm, tab_nm=tab_nm)

    return sql


def get_nol_us_200channels(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm):
    """
    Prepares SQL string for getting NOL US 200 channels info - TO BE Used as supporting function in others

    :param kwargs:
       - intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm
    :return: SQL string

    Example:
        get_nol_us_200channels(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm)

    """

    sql = """
select a.period_id, 
        a.rn_id, 
        a.channel_id, 
        a.channel_name, 
        a.surf_location_id, 
        a.hits, 
        a.duration from 
        (select a.intab_period_id as period_id,
        b.source_panelist_id as rn_id, 
        c.channel_report_credit_id as channel_id, 
        c.channel_report_credit_name as channel_name, 
        b.surf_location_id,
        sum(a.number_of_page_views) as hits, 
        sum(a.time_spent_in_seconds) as duration  
        from {sch_nm}.internet_person_page_view_usage_fact a, 
        {sch_nm}.metered_internet_location_person_weight b,
        etl_report_credit_monthly_v c 
        where a.person_media_consumer_id=b.person_media_consumer_id 
        and a.report_credit_key = c.report_credit_key 
        and a.released_for_processing_flag = "Y" 
        and b.released_for_processing_flag = "Y" 
        and c.released_for_processing_flag = "Y" 
        and a.type_of_media_event_code = 2  
        and a.report_credit_level_code not in (11,12)  
        and a.media_event_location_code in (1,3)  
        and a.sample_collection_method_key not in (43) 
        and a.intab_period_id =  ("{intab_prd_id}")
         and b.intab_period_id =  ("{intab_prd_id}")
         and c.intab_period_date between ("{intab_prd_stdt}") AND ("{intab_prd_endt}")
         and c.intab_period_date =  ("{intab_prd_stdt}")
         and c.report_credit_level_code in (3) 
        group by 1,2,3,4,5) a, 
        (select b.channel_report_credit_id as channel_id,
        a.unique_audience_weighted/1000 unique_audience 
        from {sch_nm}.internet_report_credit_summary_fact a, 
        etl_report_credit_monthly_v b 
        where a.report_credit_key = b.report_credit_key 
        and a.released_for_processing_flag = "Y" 
        and b.RELEASED_FOR_PROCESSING_FLAG = "Y" 
        and a.intab_period_id =  ("{intab_prd_id}")
         and b.intab_period_date between  ("{intab_prd_stdt}") AND ("{intab_prd_endt}")
         and b.intab_period_date =  ("{intab_prd_stdt}")
         and a.intab_period_type_code = 130 
        AND a.rollup_category_id =3  
        AND a.internet_summary_demographic_key =0  
        AND a.type_of_media_event_code=2  
        AND a.REPORT_CREDIT_LEVEL_CODE NOT IN (11,12)  
        AND a.MEDIA_EVENT_LOCATION_CATEGORY_CODE IN (5)  
        AND filter_criteria_id = 0 
        AND a.REPORT_CREDIT_LEVEL_CODE IN (3)  
        order by 2 desc 
        limit 200 
        ) b 
        where a.channel_id=b.channel_id""".format(intab_prd_id=intab_prd_id, intab_prd_stdt=intab_prd_stdt,
                                                  intab_prd_endt=intab_prd_endt, sch_nm=sch_nm, tab_nm=tab_nm)

    return sql




def get_nol_us_eligibility_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm):
    """
    Prepares SQL string for getting NOL US eligibility data - TO BE Used as supporting function in others

    :param kwargs:
       - intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm
    :return: SQL string

    Example:
        get_nol_us_200brands(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm)

    """

    sql = """
SELECT a.intab_period_id AS period_id,
       b.source_panelist_id AS rn_id,
       b.surf_location_id,
       sum(a.number_of_page_views) AS hits,
       sum(a.time_spent_in_seconds) AS duration
FROM {sch_nm}.internet_person_page_view_usage_fact a,
     {sch_nm}.metered_internet_location_person_weight b,
     {tab_nm} c
WHERE a.person_media_consumer_id=b.person_media_consumer_id
  AND a.report_credit_key = c.report_credit_key
  AND a.released_for_processing_flag = "Y"
  AND b.released_for_processing_flag = "Y"
  AND c.released_for_processing_flag = "Y"
  AND a.type_of_media_event_code = 2
  AND a.report_credit_level_code NOT IN (11,
                                         12)
  AND a.media_event_location_code IN (1,
                                      3)
  AND a.sample_collection_method_key NOT IN (43)
  AND a.intab_period_id = ("{intab_prd_id}")
  AND b.intab_period_id = ("{intab_prd_id}")
  AND c.intab_period_date BETWEEN ("{intab_prd_stdt}") AND ("{intab_prd_endt}")
GROUP BY a.intab_period_id,
         b.source_panelist_id,
         b.surf_location_id""".format(intab_prd_id=intab_prd_id, intab_prd_stdt=intab_prd_stdt,
                                      intab_prd_endt=intab_prd_endt, sch_nm=sch_nm, tab_nm=tab_nm)

    return sql


def get_metered_person_wt_sql(sch_nm):
    """
    Prepares SQL string for getting metered internet location persion weight info - TO BE Used as supporting function in others

    :param kwargs:
       - schema_name
    :return: SQL string

    Example:
        get_metered_person_wt_sql(sch_nm)

    """
    sql = """ SELECT prsn.person_media_consumer_id,
       prsn.age,
       prsn.gender_code,
       prsn.education_level_category_code,
       prsn.race_category_code,
       prsn.ethnic_identity_code,
       prsn.web_access_locations_code,
       prsn.employment_status_code,
       prsn.occupation_category_code,
       prsn.number_of_children_2_to_11,
       prsn.number_of_children_12_to_17,
       prsn.income_category_code,
       prsn.zip_code,
       prsn.county_size_code,
       prsn.internet_connection_type_code,
       prsn.spanish_language_dominance_code,
       prsn.head_of_household_indicator,
       prsn.lifestage_category_code,
       intab.intab_period_id,
       intab.intab_period_id AS end_intab_period_id,
       intab.intab_period_type_code,
       intab.intab_period_start_date AS intab_period_date,
       prsnintab.released_for_processing_flag
FROM (({sch_nm}.metered_internet_location_person prsn
       JOIN {sch_nm}.metered_internet_location_person_intab_period prsnintab 
       ON ((prsn.person_media_consumer_key = prsnintab.person_media_consumer_key)))
      JOIN {sch_nm}.intab_period intab ON 
      ((((intab.intab_period_id >= prsnintab.start_intab_period_id)
         AND (intab.intab_period_id <= prsnintab.end_intab_period_id))
         AND (intab.intab_period_type_code = 100))))
WHERE prsnintab.released_for_processing_flag = "Y" """.format(sch_nm=sch_nm)

    return sql


def get_metered_person_1mth_sql(intab_prd_stdt, intab_prd_endt, tab_nm):
    """
    Prepares SQL string for getting metered internet location persion weight one month data - TO BE Used as supporting function in others

    :param kwargs:
       - schema_name
    :return: SQL string

    Example:
        get_metered_person_1mth_sql(intab_prd_stdt, intab_prd_endt, tab_nm)

    """
    # utils.writeToLog(lvl, "get_metered_person_1mth_sql() fds_schema:{0}")

    sql = """ SELECT
            *
            FROM {tab_nm}
            WHERE intab_period_date BETWEEN ("{intab_prd_stdt}") AND ("{intab_prd_endt}") """.format(
        intab_prd_stdt=intab_prd_stdt,
        intab_prd_endt=intab_prd_endt,
        tab_nm=tab_nm)

    # utils.writeToLog(lvl, "get_metered_person_1mth_sql() {0}".format(sql))
    return sql


def get_metered_person_dedups_sql(tab_nm):
    """
    Prepares SQL string for getting metered internet location persion weight deduplication - TO BE Used as supporting function in others

    :param kwargs:
       - schema_name
    :return: SQL string

    Example:
        get_metered_person_dedups_sql(vw_nm)

    """
    # utils.writeToLog(lvl, "get_metered_person_dedups_sql() fds_schema:{0}")

    sql = """ SELECT
                a.*
              FROM {tab_nm} a
                INNER JOIN (SELECT
                                person_media_consumer_id,
                                MAX(intab_period_date) AS intab_period_date
                            FROM {tab_nm}
                            GROUP BY person_media_consumer_id) b
              ON a.person_media_consumer_id = b.person_media_consumer_id
              AND a.intab_period_date = b.intab_period_date """.format(tab_nm=tab_nm)

    # utils.writeToLog(lvl, "get_metered_person_dedups_sql() {0}".format(sql))
    return sql


def get_metered_person_demo_fnl_sql(sch_nm, tab_nm):
    """
    Prepares SQL string for getting metered internet location persion weight final data - TO BE Used as supporting function in others

    :param kwargs:
       - schema_name
    :return: SQL string

    Example:
        get_metered_person_demo_fnl_sql(sch_nm,vw1_nm)

    """
    # utils.writeToLog(lvl, "get_metered_person_demo_fnl_sql() fds_schema:{0}")

    sql = """ SELECT DISTINCT
                  a.source_panelist_id AS rn_id,
                  a.surf_location_id,
                  a.person_weight AS weight,
                  a.combined_surf_location_weight AS combo_visitors_weight,
                  b.age,
                  b.gender_code AS gender_id,
                  b.SPANISH_LANGUAGE_DOMINANCE_CODE AS spanish_lang_dominance,
                  b.RACE_CATEGORY_CODE AS race_id,
                  b.WEB_ACCESS_LOCATIONS_CODE AS web_access_locations,
                  b.EDUCATION_LEVEL_CATEGORY_CODE AS education_id,
                  b.INCOME_CATEGORY_CODE AS income_group_id,
                  b.OCCUPATION_CATEGORY_CODE AS occupation_id,
                  b.NUMBER_OF_CHILDREN_2_TO_11 AS members_2_11_count,
                  b.NUMBER_OF_CHILDREN_12_TO_17 AS members_12_17_count,
                  b.ZIP_CODE AS zip_code,
                  b.COUNTY_SIZE_CODE AS county_size_id,
                  b.ETHNIC_IDENTITY_CODE AS hispanic_origin_id,
                  b.employment_status_code AS working_status_id,
                  b.lifestage_category_code,
                  b.head_of_household_indicator,
                  b.internet_connection_type_code AS web_conn_speed_id
                FROM {sch_nm}.metered_internet_location_person_weight a
                JOIN {tab_nm} b
                  ON a.person_media_consumer_id = b.person_media_consumer_id
                  AND a.released_for_processing_flag = b.released_for_processing_flag
                  AND a.released_for_processing_flag = "Y"
                  AND a.intab_period_id = "1302018307" """.format(sch_nm=sch_nm, tab_nm=tab_nm)

    # utils.writeToLog(lvl, "get_metered_person_demo_fnl_sql() {0}".format(sql))
    return sql


def get_nol_sample_sql(intab_prd_id, sch_nm):
    """
  Prepares SQL string for getting us sample final data - TO BE Used as supporting function in others

  :param kwargs:
     - schema_name
  :return: SQL string

  Example:
      get_metered_person_demo_fnl_sql(sch_nm,vw1_nm)

  """
    # utils.writeToLog(lvl, "get_nol_sample_sql() fds_schema:{0}")
    sql = """
    SELECT
  *
FROM {sch_nm}.metered_internet_location_person_weight
WHERE intab_period_id = ("{intab_prd_id}")
AND released_for_processing_flag = "Y"
AND surf_location_id IN (1, 2) """.format(intab_prd_id=intab_prd_id, sch_nm=sch_nm)
    # utils.writeToLog(lvl, "get_nol_sample_sql() {0}".format(sql))
    return sql


def get_nol200Parents_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm):
    """
    Prepares SQL string for getting top 200 parents data - TO BE Used as supporting function in others

    :param kwargs:
       - schema_name
    :return: SQL string

    Example:
        get_metered_person_demo_fnl_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm)

    """
    # utils.writeToLog(lvl, "get_nol200Parents_sql() fds_schema:{0}")

    sql = """
    SELECT a.period_id,
       a.rn_id,
       a.parent_id,
       a.parent_name,
       a.surf_location_id,
       a.hits,
       a.duration
FROM
  (SELECT a.intab_period_id AS period_id,
          b.source_panelist_id AS rn_id,
          c.parent_report_credit_id AS parent_id,
          c.parent_report_credit_name AS parent_name,
          b.surf_location_id,
          sum(a.number_of_page_views) AS hits,
          sum(a.time_spent_in_seconds) AS duration
   FROM {sch_nm}.internet_person_page_view_usage_fact a, {sch_nm}.metered_internet_location_person_weight b, {tab_nm} c
   WHERE a.person_media_consumer_id=b.person_media_consumer_id
     AND a.report_credit_key = c.report_credit_key
     AND a.released_for_processing_flag = "Y"
     AND b.released_for_processing_flag = "Y"
     AND c.released_for_processing_flag = "Y"
     AND a.type_of_media_event_code = 2
     AND a.report_credit_level_code NOT IN (11,
                                            12)
     AND a.media_event_location_code IN (1,
                                         3)
     AND a.sample_collection_method_key NOT IN (43)
     AND a.intab_period_id = ("{intab_prd_id}")
     AND b.intab_period_id = ("{intab_prd_id}")
     AND c.intab_period_date BETWEEN ("{intab_prd_stdt}") AND ("{intab_prd_endt}")
     AND c.intab_period_date = ("{intab_prd_stdt}")
     AND c.report_credit_level_code IN (1,
                                        2,
                                        3)
   GROUP BY 1,
            2,
            3,
            4,
            5) a,

  (SELECT b.parent_report_credit_id AS parent_id,
          a.unique_audience_weighted/1000 unique_audience
   FROM {sch_nm}.internet_report_credit_summary_fact a, {tab_nm} b
   WHERE a.report_credit_key = b.report_credit_key
     AND a.released_for_processing_flag = "Y"
     AND b.RELEASED_FOR_PROCESSING_FLAG = "Y"
     AND a.intab_period_id = ("{intab_prd_id}")
     AND b.intab_period_date BETWEEN ("{intab_prd_stdt}") AND ("{intab_prd_endt}")
     AND b.intab_period_date = ("{intab_prd_stdt}")
     AND a.intab_period_type_code = 130
     AND a.rollup_category_id =1
     AND a.internet_summary_demographic_key =0
     AND a.type_of_media_event_code=2
     AND a.REPORT_CREDIT_LEVEL_CODE NOT IN (11,
                                            12)
     AND a.MEDIA_EVENT_LOCATION_CATEGORY_CODE IN (5)
     AND filter_criteria_id = 0
     AND a.REPORT_CREDIT_LEVEL_CODE IN (1)
   ORDER BY 2 DESC
   LIMIT 200)b
WHERE a.parent_id=b.parent_id """.format(intab_prd_id=intab_prd_id, intab_prd_stdt=intab_prd_stdt,
                                         intab_prd_endt=intab_prd_endt, sch_nm=sch_nm, tab_nm=tab_nm)

    #utils.writeToLog(lvl, "get_nol200Parents_sql() {0}".format(sql))
    return sql


def get_nol_aggr_top200parents_sql(tab_nm):
    """
Prepares SQL string for getting top 200 parents data - TO BE Used as supporting function in others

:param kwargs:
   - schema_name
:return: SQL string

Example:
    get_metered_person_demo_fnl_sql(tab_nm)

"""

    # utils.writeToLog(lvl, "get_nol_aggr_top200parents_sql() fds_schema:{0}")

    sql = """ SELECT period_id,
           rn_id,
           parent_id,
           parent_name,
           sum(hits) AS hits,
           sum(duration) AS duration
    FROM {tab_nm}
    GROUP BY 1,
             2,
             3,
             4 """.format(tab_nm=tab_nm)

    # utils.writeToLog(lvl, "get_nol_aggr_top200parents_sql() {0}".format(sql))
    return sql


def get_nol_us_top200sites_brand_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm, brndvw_nm):
    """
Prepares SQL string for getting top 200 parents data - TO BE Used as supporting function in others

:param kwargs:
   - schema_name
:return: SQL string

Example:
    get_metered_person_demo_fnl_sql(tab_nm)

"""

    #utils.writeToLog(lvl, "get_nol_aggr_top200parents_sql() fds_schema:{0}")

    sql = """ 
        select
   a.intab_period_id,
   b.brand_report_credit_id as tree_id,
   b.brand_report_credit_name as entity_name,
   b.report_credit_level_code as entity_level_code,
   b.report_credit_level_desc as entity_level,
   (a.unique_audience_weighted / 1000) as unique_audience 
from
   {sch_nm}.internet_report_credit_summary_fact a,
   {tab_nm} b 
where
   a.report_credit_key = b.report_credit_key 
   and a.released_for_processing_flag = "Y" 
   and b.RELEASED_FOR_PROCESSING_FLAG = "Y" 
   and a.intab_period_id = 
   (
      "{intab_prd_id}"
   )
   and b.intab_period_date between ("{intab_prd_stdt}") AND 
   (
      "{intab_prd_endt}"
   )
   and b.intab_period_date = 
   (
      "{intab_prd_stdt}"
   )
   and a.intab_period_type_code = 130 
   AND a.rollup_category_id = 2 
   AND a.internet_summary_demographic_key = 0 
   AND a.type_of_media_event_code = 2 
   AND a.REPORT_CREDIT_LEVEL_CODE NOT IN 
   (
      11,
      12
   )
   AND a.MEDIA_EVENT_LOCATION_CATEGORY_CODE IN 
   (
      5
   )
   AND filter_criteria_id = 0 
   AND a.REPORT_CREDIT_LEVEL_CODE IN 
   (
      2
   ) """.format(intab_prd_id=intab_prd_id, intab_prd_stdt=intab_prd_stdt,
                intab_prd_endt=intab_prd_endt, sch_nm=sch_nm, tab_nm=tab_nm,
                brndvw_nm=brndvw_nm)

    return sql


def get_nol_us_top200sites_channels_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm, chnvw_nm):
    """
Prepares SQL string for getting top 200 parents data - TO BE Used as supporting function in others

:param kwargs:
   - schema_name
:return: SQL string

Example:
    get_metered_person_demo_fnl_sql(tab_nm)

"""

    sql = """ 
        select
   a.intab_period_id,
   b.channel_report_credit_id as tree_id,
   b.channel_report_credit_name as entity_name,
   b.report_credit_level_code as entity_level_code,
   b.report_credit_level_desc as entity_level,
   (a.unique_audience_weighted / 1000) as unique_audience 
from
   {sch_nm}.internet_report_credit_summary_fact a,
   {tab_nm} b 
where
   a.report_credit_key = b.report_credit_key 
   and a.released_for_processing_flag = "Y" 
   and b.RELEASED_FOR_PROCESSING_FLAG = "Y" 
   and a.intab_period_id = 
   (
      "{intab_prd_id}"
   )
   and b.intab_period_date between ("{intab_prd_stdt}") AND 
   (
      "{intab_prd_endt}"
   )
   and b.intab_period_date = 
   (
      "{intab_prd_stdt}"
   )
   and a.intab_period_type_code = 130 
   AND a.rollup_category_id = 3 
   AND a.internet_summary_demographic_key = 0 
   AND a.type_of_media_event_code = 2 
   AND a.REPORT_CREDIT_LEVEL_CODE NOT IN 
   (
      11,
      12
   )
   AND a.MEDIA_EVENT_LOCATION_CATEGORY_CODE IN 
   (
      5
   )
   AND filter_criteria_id = 0 
   AND a.REPORT_CREDIT_LEVEL_CODE IN 
   (
      3
   ) """.format(intab_prd_id=intab_prd_id, intab_prd_stdt=intab_prd_stdt,
                intab_prd_endt=intab_prd_endt, sch_nm=sch_nm, tab_nm=tab_nm, chnvw_nm=chnvw_nm)

    return sql


def get_nol_us_top200sites_parents_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm, parvw_nm):
    """
Prepares SQL string for getting top 200 parents data - TO BE Used as supporting function in others

:param kwargs:
   - schema_name
:return: SQL string

Example:
    get_metered_person_demo_fnl_sql(tab_nm)

"""

    sql = """ 
        select
   a.intab_period_id,
   b.parent_report_credit_id as tree_id,
   b.parent_report_credit_name as entity_name,
   b.report_credit_level_code as entity_level_code,
   b.report_credit_level_desc as entity_level,
   (a.unique_audience_weighted / 1000) as unique_audience 
from
   {sch_nm}.internet_report_credit_summary_fact a,
   {tab_nm} b 
where
   a.report_credit_key = b.report_credit_key 
   and a.released_for_processing_flag = "Y" 
   and b.RELEASED_FOR_PROCESSING_FLAG = "Y" 
   and a.intab_period_id = 
   (
      "{intab_prd_id}"
   )
   and b.intab_period_date between ("{intab_prd_stdt}") AND 
   (
      "{intab_prd_endt}"
   )
   and b.intab_period_date = 
   (
      "{intab_prd_stdt}"
   )
   and a.intab_period_type_code = 130 
   AND a.rollup_category_id = 1 
   AND a.internet_summary_demographic_key = 0 
   AND a.type_of_media_event_code = 2 
   AND a.REPORT_CREDIT_LEVEL_CODE NOT IN 
   (
      11,
      12
   )
   AND a.MEDIA_EVENT_LOCATION_CATEGORY_CODE IN 
   (
      5
   )
   AND filter_criteria_id = 0 
   AND a.REPORT_CREDIT_LEVEL_CODE IN 
   (
      1
   ) """.format(intab_prd_id=intab_prd_id, intab_prd_stdt=intab_prd_stdt,
                intab_prd_endt=intab_prd_endt, sch_nm=sch_nm, tab_nm=tab_nm, parvw_nm=parvw_nm)

    return sql

