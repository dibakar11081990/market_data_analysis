-- airflow Dag which regulates this model: 
-- dir: mdp_360/mdp_i360_new_dag 
-- dag: mdp_i360_new_trg

-- Get the right stack database properties.
{% set db_properties=get_dbproperties('MDP_I360_NEW') %}


-- Set the database properties.
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

-- Setting the Materialization.
{{ config(
        materialized='table'
) }}

-- set the right tags.
{{ config(tags=[var('TAG_MDP_I360_NEW'),var('TAG_MDP_I360_NEW_FILTER_MULTIPLES')
]) }}



 WITH 
    SingleClaimStatus as ( -- if there's only one Claim Status value, then take that one 
      SELECT PLAN_ID, FUND_NAME, ACTIVITY, PUBLICATION_NAME, PLAN_STATUS, COUNT(DISTINCT CLAIMAUDITSTATUS_NEW) AS DistinctClaimStatus
      FROM 
      -- $T{MDP_I360_BREAKOUT}
      {{ ref('MDP_I360_BREAKOUT') }}
      GROUP BY 1,2,3,4,5
      HAVING COUNT(DISTINCT CLAIMAUDITSTATUS_NEW) = 1
    )
   ,MultipleClaimStatus as ( -- Find records with multiple values for claim status 
      SELECT PLAN_ID, FUND_NAME, ACTIVITY, PUBLICATION_NAME, PLAN_STATUS, COUNT(DISTINCT CLAIMAUDITSTATUS_NEW) AS DistinctClaimStatus
      FROM 
      -- $T{MDP_I360_BREAKOUT}
      {{ ref('MDP_I360_BREAKOUT') }}
      GROUP BY 1,2,3,4,5
      HAVING COUNT(DISTINCT CLAIMAUDITSTATUS_NEW) > 1
   )
   ,PickOne as ( -- For those with multiple values, pick the non-cancelled ones. If there is more than 1 non-cancelled value, pick the newest one based on claim submit date. 
      SELECT a.PLAN_ID, a.FUND_NAME, a.ACTIVITY, a.PUBLICATION_NAME, a.PLAN_STATUS, a.CLAIMAUDITSTATUS_NEW, a.CLAIM_SUBMITTED_DATE
      FROM (
          select PLAN_ID, FUND_NAME, ACTIVITY, PUBLICATION_NAME, PLAN_STATUS, CLAIMAUDITSTATUS_NEW, CLAIM_SUBMITTED_DATE, 
                  ROW_NUMBER() OVER ( PARTITION BY PLAN_ID, FUND_NAME, ACTIVITY, PUBLICATION_NAME, PLAN_STATUS ORDER BY CASE WHEN CLAIMAUDITSTATUS_NEW = 'Cancelled' THEN 1 ELSE 0 END ASC, CLAIM_SUBMITTED_DATE DESC) as RowNum
          FROM 
          -- $T{MDP_I360_BREAKOUT}
          {{ ref('MDP_I360_BREAKOUT') }}
       ) a
      INNER JOIN MultipleClaimStatus m 
      ON a.PLAN_ID = m.PLAN_ID and a.ACTIVITY = m.ACTIVITY and a.PUBLICATION_NAME = m.PUBLICATION_NAME and a.PLAN_STATUS = m.PLAN_STATUS
      WHERE rownum = 1 
   )
      -- Union the single and the pick-one values
    SELECT a.* 
    FROM -- $T{MDP_I360_BREAKOUT} 
    {{ ref('MDP_I360_BREAKOUT') }} a
    INNER JOIN SingleClaimStatus s
    ON a.PLAN_ID = s.PLAN_ID and a.ACTIVITY = s.ACTIVITY and a.PUBLICATION_NAME = s.PUBLICATION_NAME and a.PLAN_STATUS  = s.PLAN_STATUS 
    UNION ALL
    SELECT b.*
    FROM -- $T{MDP_I360_BREAKOUT}
    {{ ref('MDP_I360_BREAKOUT') }} b
    INNER JOIN PickOne p
    ON b.PLAN_ID = p.PLAN_ID and b.ACTIVITY = p.ACTIVITY and b.PUBLICATION_NAME = p.PUBLICATION_NAME and b.PLAN_STATUS = p.PLAN_STATUS and b.CLAIMAUDITSTATUS_NEW = p.CLAIMAUDITSTATUS_NEW




