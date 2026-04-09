{% macro get_dbproperties(project_name, start_date=none, end_date=none) -%}


    {%- set database = none-%}
    {%- set schema = none-%}
    {%- set database_raw = none-%}
    {%- set database_etl = none-%}

    {{ log("Running get_dbproperties: " ~ project_name ~ ", " ~ start_date ~ ", " ~ end_date) }}

    {%- if project_name|lower == 'mpm' -%}
        {%- if start_date is not none and end_date is not none -%}

          {%- set start_date = modules.datetime.datetime.strptime(start_date|string, '%Y%m%d') -%}
          {%- set end_date = modules.datetime.datetime.strptime(end_date|string, '%Y%m%d') -%}

          {%- if start_date < modules.datetime.datetime(2020, 2, 1, 0, 0, 0) and end_date < modules.datetime.datetime(2020, 2, 1, 0, 0, 0) -%}

              {%- if 'prd' in target.name -%}
                {%- set database = 'PROD'-%}
                {%- set schema = 'MPM_LEGACY'-%}
              {% elif 'dev' in target.name -%}
                {%- set database = 'DEV'-%}
                {%- set schema = 'MPM_LEGACY'-%}
              {%- endif -%}

          {% elif start_date >= modules.datetime.datetime(2020, 2, 1, 0, 0, 0) and end_date >= modules.datetime.datetime(2020, 2, 1, 0, 0, 0)  %}
              {%- if 'prd' in target.name -%}
                {%- set database = 'PROD'-%}
                {%- set schema = 'MPM'-%}
              {% elif 'dev' in target.name -%}
                {%- set database = 'DEV'-%}
                {%- set schema = 'MPM'-%}
              {%- endif -%}
          {%- endif -%}

        {% else %}
            {%- if 'prd' in target.name -%}
              {%- set database = 'PROD'-%}
              {%- set schema = 'MPM'-%}
            {% elif 'dev' in target.name -%}
              {%- set database = 'DEV'-%}
              {%- set schema = 'MPM'-%}
            {%- endif -%}
        {%- endif -%}


    {# MPM_EXCHANGE #}
    {%- elif project_name.lower() == 'mpm_exchange' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MPM_EXCHANGE'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MPM_EXCHANGE'-%}
        {%- endif -%}

    {# FF CAMPAIGN & PAID MEDIA #}
     {%- elif project_name.lower() == 'campaign_paid_media' -%}

      {%- if 'prd' in target.name -%}
        {%- set database = 'PROD'-%}
        {%- set schema = 'CAMPAIGN_PAID_MEDIA'-%}
      {% elif 'dev' in target.name -%}
        {%- set database = 'DEV'-%}
        {%- set schema = 'CAMPAIGN_PAID_MEDIA'-%}
      {%- endif -%}

    {# FF_CAMPAIGN_REPORTING #}
    {%- elif project_name.lower() == 'ff_campaign_reporting' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MPM_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MPM_REPORTING'-%}
        {%- endif -%}

    {# FF_CAMPAIGN_REPORTING_STAR_SCHEMA #}
    {%- elif project_name.lower() == 'ff_campaign_reporting_star_schema_fact' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'CAMPAIGN_REPORTING_FACT'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'CAMPAIGN_REPORTING_FACT'-%}
        {%- endif -%}



    {%- elif project_name.lower() == 'ff_campaign_reporting_star_schema_report' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'CAMPAIGN_REPORTING_REPORTING'-%}
         {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'CAMPAIGN_REPORTING_REPORTING'-%}
        {%- endif -%}


    {%- elif project_name.lower() == 'ff_campaign_reporting_star_schema_stage' -%}

      {%- if 'prd' in target.name -%}
        {%- set database = 'PROD'-%}
        {%- set schema = 'CAMPAIGN_REPORTING_STAGE'-%}
       {% else %}
        {%- set database = 'DEV'-%}
        {%- set schema = 'CAMPAIGN_REPORTING_STAGE'-%}
      {%- endif -%}    


    {# campaign report trg #}
    {%- elif project_name.lower() == 'campaign_report' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {%- endif -%}

     {# campaign ALLOCATION #}
    {%- elif project_name.lower() == 'campaign_allocation' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MPM_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MPM_REPORTING'-%}
        {%- endif -%}

    {# funnel_of_engaged_contacts #}
    {%- elif project_name.lower() == 'funnel_of_engaged_contacts' -%}

      {%- if 'prd' in target.name -%}
        {%- set database = 'BSM_SANDBOX'-%}
        {%- set schema = 'MPM_REPORTING'-%}
      {% else %}
        {%- set database = 'DEV'-%}
        {%- set schema = 'MPM_REPORTING'-%}
      {%- endif -%}

    {# Secure_Persona_daily_refresh_orchestration #}
    {%- elif project_name.lower() == 'persona_pbi_omni_mkto_id' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MI'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MI'-%}
        {%- endif -%}

     {# MDF_PROD_Trigger #}
    {%- elif project_name.lower() == 'mdf' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MDF'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MDF'-%}
        {%- endif -%}


    {# MPM_REORTING #}
    {%- elif project_name.lower() == 'mpm_reporting' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MPM_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MPM_REPORTING'-%}
        {%- endif -%}
        {# LEADS_FUNNEL #}
    {%- elif project_name.lower() == 'leads_funnel' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'LEADSFUNNEL'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'LEADSFUNNEL'-%}
        {%- endif -%}


    {# ALPHA #}
    {%- elif project_name.lower() == 'alpha_csn' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'ALPHA_CSN'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'ALPHA_CSN'-%}
        {%- endif -%}

      {# KYA #}
      {%- elif project_name.lower() == 'kya' -%}

          {%- if 'prd' in target.name -%}
            {%- set database = 'PROD'-%}
            {%- set schema = 'KYA'-%}
          {% else %}
            {%- set database = 'DEV'-%}
            {%- set schema = 'KYA'-%}
          {%- endif -%}

      {# KYA_SANBOX #}
      {%- elif project_name.lower() == 'kya_sandbox' -%}

          {%- if 'prd' in target.name -%}
            {%- set database = 'BSM_SANDBOX'-%}
            {%- set schema = 'CORE_DATASETS_SECURE_METRICS'-%}
          {% else %}
            {%- set database = 'DEV'-%}
            {%- set schema = 'CORE_DATASETS_SECURE_METRICS'-%}
          {%- endif -%}

      {# mc_eba #}
      {%- elif project_name.lower() == 'mc_eba' -%}

          {%- if 'prd' in target.name -%}
            {%- set database = 'PROD'-%}
            {%- set schema = 'EBA'-%}
          {% else %}
            {%- set database = 'DEV'-%}
            {%- set schema = 'EBA'-%}
          {%- endif -%}

    {# executive_summary_gdg #}
    {%- elif project_name.lower() == 'executive_summary_gdg' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'GDG_EXECUTIVE'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'GDG_EXECUTIVE'-%}
        {%- endif -%}

    {# digital_executions_spend #}
    {%- elif project_name.lower() == 'digital_executions_spend' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'DIGITAL_SERVICES'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'DIGITAL_SERVICES'-%}
        {%- endif -%}

    {# Polaris CLC #}
    {%- elif project_name.lower() == 'polaris' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'POLARIS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'POLARIS'-%}
        {%- endif -%}

    {# digital_executions_spend #}
    {%- elif project_name.lower() == 'aex_drupal_segment' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MARKETO_INTERGATION'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MARKETO_INTERGATION'-%}
        {%- endif -%}

    {# gclid_lead #}
    {%- elif project_name.lower() == 'gclid_lead' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'GCLID'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'GCLID'-%}
        {%- endif -%}

    {# DAT Fact #}
    {%- elif project_name.lower() == 'dat' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'DAT'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'DAT'-%}
        {%- endif -%}

    {# MARKETABILITY #}
    {%- elif project_name.lower() == 'marketability' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'CORE_DATASETS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'CORE_DATASETS'-%}
        {%- endif -%}

    {# GLOBAL_CC_VIEW #}
    {%- elif project_name.lower() == 'globalccview' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {%- endif -%}

    {# FFM #}
    {%- elif project_name.lower() == 'ffm' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'FFM'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'FFM'-%}
        {%- endif -%}

     {# FULL_FUNNEL_METRICS_NEW_MODEL #}
    {%- elif project_name.lower() == 'ffm_reporting' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'FFM_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'FFM_REPORTING'-%}
        {%- endif -%}

     {# FULL_FUNNEL_METRICS_NEW_MODEL #}
    {%- elif project_name.lower() == 'fullfunnel' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'FULLFUNNEL'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'FULLFUNNEL'-%}
        {%- endif -%}

    {# GLOI STAR SCHEMA #}
    {%- elif project_name.lower() == 'gloi' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'GLOI_FACT'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'GLOI_FACT'-%}
        {%- endif -%}

    {# AMPLIFY ABM #}
    {%- elif project_name.lower() == 'amplify_abm' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MQA_AMPLIFY_ABM'-%}
        {% else %}
          {%- set database = 'BSM_SANDBOX'-%}
          {%- set schema = 'MQA_AMPLIFY_ABM_SANDBOX'-%}
        {%- endif -%}

    {# P4_METRICS #}
    {%- elif project_name.lower() == 'p4_metrics' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'P4_METRICS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'P4_METRICS'-%}
        {%- endif -%}

    {# MI_CIB #}
    {%- elif project_name.lower() == 'mi_cib' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MI'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MI'-%}
        {%- endif -%}

    {# MDP_INGESTION_TRG #}
    {%- elif project_name.lower() == 'mdp_ingestion_trg' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MDP'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MDP'-%}
        {%- endif -%}

    {# MI_BIM360 #}
    {%- elif project_name.lower() == 'mi_bim360_flow_trg' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MI'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MI'-%}
        {%- endif -%}

    {# CONTENT_POC_SUMMARY_REFRESH #}
    {%- elif project_name.lower() == 'content_poc_summary_refresh' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'CJA'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'CJA'-%}
        {%- endif -%}

    {# FACT_DA_COURSE_DATA #}
    {%- elif project_name.lower() == 'fact_da_course_data' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'AEX_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'AEX_REPORTING'-%}
        {%- endif -%}

    {# SEISMIC_REPORTING_PROD_UPDATE #}
    {%- elif project_name.lower() == 'seismic_reporting_prod_update' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'SEISMIC_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'SEISMIC_REPORTING'-%}
        {%- endif -%}

    {# LUMOS #}
    {%- elif project_name.lower() == 'lumos' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'LUMOS'-%}
        {% else %}
          {%- set database = 'BSM_SANDBOX'-%}
          {%- set schema = 'LUMOS_SANDBOX'-%}
        {%- endif -%}


    {# S3_TO_SNOWFLAKE #}
    {%- elif project_name.lower() == 's3_to_snowflake' -%}

      {%- if 'prd' in target.name -%}
        {%- set database_raw = 'RAW' -%}
        {%- set database_etl = 'BSM_ETL' -%}
        {%- set schema = 'ADP'-%}
      {% else %}
        {%- set database_raw = 'DEV' -%}
        {%- set database_etl = 'DEV' -%}
        {%- set schema = 'ADP_SANDBOX' -%}
      {%- endif -%}

    {# TELEVENDOR_REPORT_LEAD_CAMPAIGN #}
    {%- elif project_name.lower() == 'televendor_report_lead_campaign' -%}

      {%- if 'prd' in target.name -%}
        {%- set database = 'PROD' -%}
        {%- set schema = 'TELEVENDOR'-%}
      {% else %}
        {%- set database = 'BSM_SANDBOX' -%}
        {%- set schema = 'TELEVENDOR_SANDBOX' -%}
      {%- endif -%}

    {# Competitive_Intelligence_Reporting #}
    {%- elif project_name.lower() == 'competitive_intelligence_reporting' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'INSTALL_BASE'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'INSTALL_BASE'-%}
        {%- endif -%}

    {# Trials_lookup #}
    {%- elif project_name.lower() == 'trials_lookup' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {%- endif -%}

    {# bidtellect #} {# GETPARAGON-468 #}
    {%- elif project_name.lower() == 'bidtellect' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'BIDTELLECT'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'BIDTELLECT'-%}
        {%- endif -%}

    {# ydn_integration #} {# GETPARAGON-468 #}
    {%- elif project_name.lower() == 'ydn_integration' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'DIGITAL_SERVICES'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'DIGITAL_SERVICES'-%}
        {%- endif -%}

    {# sixsense_adhoc_report #} {# GETPARAGON-468 #}
    {%- elif project_name.lower() == 'sixsense_adhoc_report' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'BSM_SANDBOX'-%}
          {%- set schema = 'SIXSENSE_DATA_ANALYSIS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'SIXSENSE_DATA_ANALYSIS'-%}
        {%- endif -%}

    {# adp_portfolio #} {# GETPARAGON-537 #}
    {%- elif project_name.lower() == 'adp_portfolio' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'AWS_S3'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'AWS_S3'-%}
        {%- endif -%}

    {# genuine_sixsense_accounts #} {# GETPARAGON-537 #}
    {%- elif project_name.lower() == 'genuine_sixsense_accounts' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'SIXSENSE'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'SIXSENSE'-%}
        {%- endif -%}

    {# global_lead_report_v1 #} {# GETPARAGON-607 #}
    {%- elif project_name.lower() == 'global_lead_report_v1' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {%- endif -%}

    {%- elif project_name.lower() == 'sixsense' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'SIXSENSE'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'SIXSENSE'-%}
        {%- endif -%}

    {# nbe_quotesmc_refresh #} {# GETPARAGON-952 #}
    {%- elif project_name.lower() == 'nbe_quotesmc_refresh' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MDF'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MDF'-%}
        {%- endif -%}

    {# NAS_dashboard #} {# GETPARAGON-1306 #} 
    {%- elif project_name.lower() == 'nas_dashboard' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'NAS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'NAS'-%}
        {%- endif -%}

    {# Persona_PBI_Omni #}
    {%- elif project_name.lower() == 'persona_pbi_omni' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MI'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MI'-%}
        {%- endif -%}

    {# MKTG_T_Activities #}
    {%- elif project_name.lower() == 'mktg_t_activities' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MKTG_REPORTING'-%}
        {%- endif -%}

    {# DCM #}
    {%- elif project_name.lower() == 'dcm' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'BSM_SANDBOX'-%}
          {%- set schema = 'ADOBE_AAM'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'ADOBE_AAM_SANDBOX'-%}
        {%- endif -%}

    {# account_metadata_update #}
    {%- elif project_name.lower() == 'account_metadata_update' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MARKETO_SYNC'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MARKETO_SYNC_SANDBOX'-%}
        {%- endif -%}

    {# adp_regional_portfolio #}
    {%- elif project_name.lower() == 'adp_regional_portfolio' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'AWS_S3'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'AWS_S3'-%}
        {%- endif -%}

    {# s3_to_snowflake_hginsight_adsk_data_model #}
    {%- elif project_name.lower() == 's3_to_snowflake_hginsight_adsk_data_model' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'HG_INSIGHTS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'HG_INSIGHTS_RAW_SANDBOX'-%}
        {%- endif -%}

    {# allocadia_full_ingestion #}
    {%- elif project_name.lower() == 'allocadia_full_ingestion' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'ALLOCADIA'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'ALLOCADIA_SANDBOX'-%}
        {%- endif -%}

    {# 6sense_adhoc_orchestration #}
    {%- elif project_name.lower() == '6sense_adhoc_orchestration' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'BSM_SANDBOX'-%}
          {%- set schema = 'SIXSENSE_DATA_ANALYSIS'-%}
        {% else %}
          {%- set database = 'BSM_SANDBOX'-%}
          {%- set schema = 'SIXSENSE_DATA_ANALYSIS_SANDBOX'-%}
        {%- endif -%}

    {# SPEND EDH#}
    {%- elif project_name.lower() == 'spend_edh' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'BSM_ETL'-%}
          {%- set schema = 'MPM_PRD'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MPM_PRD'-%}
        {%- endif -%}

    {# PRODUCT LOOKUP#}
    {%- elif project_name.lower() == 'product_lookup' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'PRODUCT_LOOKUP'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'PRODUCT_LOOKUP'-%}
        {%- endif -%}

    {# MDF_VCP_CCI#}
    {%- elif project_name.lower() == 'mdf_vcp_cci' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MDF'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MDF'-%}
        {%- endif -%}


        {# CJO OCP#}
    {%- elif project_name.lower() == 'cjo_ocp' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'CJO'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'SAURABH'-%}
        {%- endif -%}

        {# DCS PREP#}
    {%- elif project_name.lower() == 'dcs_prep' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'DCS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'DCS'-%}
        {%- endif -%}

    {# AA_EVAR_ALLOCADIA_FILE #}
    {%- elif project_name.lower() == 'aa_evar_allocadia_file' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'ADOBE_AA_ALID'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'ADOBE_AA_ALID'-%}
        {%- endif -%}

    {# LEAD_MARKETO #}
    {%- elif project_name.lower() == 'lead_marketo' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'RAW'-%}
          {%- set schema = 'MARKETO'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MARKETO'-%}
        {%- endif -%}

    {# LEADS_DQ #}
    {%- elif project_name.lower() == 'leads_dq' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'DQA'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'DQA'-%}
        {%- endif -%}


    {# SPROUT_API_INGESTION #}
    {%- elif project_name.lower() == 'sprout_api_ingestion' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'SPROUT'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'SPROUT2'-%}
        {%- endif -%}

    {# MDP_I360_NEW #}
    {%- elif project_name.lower() == 'mdp_i360_new' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'BSM_ETL'-%}
          {%- set schema = 'MDF'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MDF'-%}
        {%- endif -%}
    {# D2B INSIGHTS  #}
    {%- elif project_name.lower() == 'd2b_insights' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'MARKETO_INTERGATION'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'MARKETO_INTERGATION'-%}
        {%- endif -%}

    {# Enquiry 2 Opportunity #}
    {%- elif project_name.lower() == 'e2o' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'E2O'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'E2O'-%}
        {%- endif -%}

    {# Enquiry 2 Opportunity Reporting #}
    {%- elif project_name.lower() == 'e2o_reporting' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'E2O_REPORTING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'E2O_REPORTING'-%}
        {%- endif -%}
    {# EBA EVENT SCORING #}
    {%- elif project_name.lower() == 'eba_scoring' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'EBA_SCORING'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'EBA_SCORING'-%}
        {%- endif -%}

    {# CUSTOMER_LIFECYCLE #}
    {%- elif project_name.lower() == 'customer_lifecycle' -%}

        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'CUSTOMER_LIFECYCLE'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'CUSTOMER_LIFECYCLE'-%}
        {%- endif -%}

    {# NAS_dashboard #} {# GETPARAGON-1306 #}
    {%- elif project_name.lower() == 'nas_dashboard' -%}
        {%- if 'prd' in target.name -%}
          {%- set database = 'PROD'-%}
          {%- set schema = 'NAS'-%}
        {% else %}
          {%- set database = 'DEV'-%}
          {%- set schema = 'NAS'-%}
        {%- endif -%}

    {# Raw_allocadia#}
    {%- elif 'raw' in project_name.lower() -%}

        {%- set database = 'RAW'-%}
        {%- set schema = 'ALLOCADIA'-%}
    {%- endif -%}

    {{log("Macro -> set_mpmstack_schema Database:" ~ database)}}
    {{log("Macro -> set_mpmstack_schema Schema:" ~ schema)}}

    {# return the values#}

    {{ return({'database':database, 'schema':schema, 'database_raw':database_raw, 'database_etl':database_etl})}}

{%- endmacro %}