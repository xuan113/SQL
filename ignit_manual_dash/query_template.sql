use {userdb};

DROP TABLE IF EXISTS {userdb}.manual_dashboard_streaming_{testid};
CREATE TABLE {userdb}.manual_dashboard_streaming_{testid} AS
SELECT
    test_id,
    test_name,
    test_cell_nbr,
    test_cell_name AS name,
    -- Standard filters
    COALESCE(allocation_type, 'All Allocations') AS filter_allocation_type,
    COALESCE(signup_region, 'All Allocations') AS filter_signup_region,
    COALESCE(completed_activity_window, 'All Allocations') AS filter_completed_activity_window, 
    COALESCE(country_group, 'All Allocations') AS filter_country_group, 
    -- Allocation filters
    {allocation_filters_coalesce}
    -- Activity Filters
    {activity_filters}
    -- Streaming tresholds
    SUM(CASE WHEN vhs.total_sec >= 0 THEN 1 ELSE 0 END) AS tab1_99retention_and_streaming99streamed_ge0,
    SUM(CASE WHEN vhs.total_sec >= 1 * 3600 THEN 1 ELSE 0 END) AS tab1_99retention_and_streaming99streamed_ge1,
    SUM(CASE WHEN vhs.total_sec >= 5 * 3600 THEN 1 ELSE 0 END) AS tab1_99retention_and_streaming99streamed_ge5,
    SUM(CASE WHEN vhs.total_sec >= 10 * 3600 THEN 1 ELSE 0 END) AS tab1_99retention_and_streaming99streamed_ge10,
    SUM(CASE WHEN vhs.total_sec >= 20 * 3600 THEN 1 ELSE 0 END) AS tab1_99retention_and_streaming99streamed_ge20,
    SUM(CASE WHEN vhs.total_sec >= 40 * 3600 THEN 1 ELSE 0 END) AS tab1_99retention_and_streaming99streamed_ge40,
    SUM(CASE WHEN vhs.total_sec >= 80 * 3600 THEN 1 ELSE 0 END) AS tab1_99retention_and_streaming99streamed_ge80,
    -- Retention and allocation counts
    SUM(allocs.is_current_member) AS cumulative_retention,
    SUM(allocs.is_current_subscrn * allocs.current_plan_usd_price) AS collected_revenue,
    SUM(allocs.current_plan_usd_price) AS potential_revenue,
    SUM(allocs.is_ever_invol_cancel) AS mop_failure_rate,
    COUNT(allocs.account_id) AS number_of_allocations
FROM (
    SELECT
        test_id,
        test_name,
        account_id,
        {test_cell_sel},
        {name_cell_sel},
        allocation_type,
        signup_region,
        signup_subregion,   
        CASE 
          WHEN signup_subregion = 'United States' THEN 'US'
          WHEN signup_subregion IN ('Canada', 'Mexico', 'UK', 'Brazil') THEN 'Mid Size Countries'
          ELSE 'Small Countries'
        END AS country_group,
        completed_activity_window,
        is_current_subscrn,
        current_plan_usd_price,
        is_ever_invol_cancel,
        is_current_member
    FROM {userdb}.allocations_full_{testid}
    ) allocs
{alloc_joins}
LEFT OUTER JOIN (
    SELECT
        account_id,
        -- activity filters
        {activity_coalesce}
        SUM(standard_sanitized_duration_sec) AS total_sec
    FROM {userdb}.vhs_dump_for_manual_dashboard_{testid}
    WHERE allocation_region_date > -1
    AND days_since_allocation <= {tenure_grouping}
    GROUP BY
        account_id,
        -- activity filters
        {activity_case}
    WITH CUBE
    HAVING account_id IS NOT NULL
    ) vhs
ON allocs.account_id = vhs.account_id
GROUP BY
    test_id,
    test_name,
    test_cell_nbr,
    test_cell_name,
    allocation_type,
    signup_region,
    country_group,
    completed_activity_window,
    -- allocation filters
    {allocation_case}
    -- activity filters
    {activity_case_null}
WITH CUBE
HAVING test_id IS NOT NULL
AND test_name IS NOT NULL
AND test_cell_nbr IS NOT NULL
AND test_cell_name IS NOT NULL
-- activity filters
{and_activty_filters}
;

DROP TABLE IF EXISTS {userdb}.manual_dashboard_{testid};
CREATE TABLE {userdb}.manual_dashboard_{testid} AS
SELECT 
    stream.test_id,
    stream.test_name,
    stream.test_cell_nbr,
    stream.name,
    stream.filter_allocation_type,
    stream.filter_signup_region,
    stream.filter_completed_activity_window,
    stream.filter_country_group,
    -- allocation filters
    {stream_allocation_filter_names}
    -- activity filters
    {stream_activity_filter_names}
    -- retention and streaming
    ret.tab1_99retention_and_streaming99revenue_weighted_retention,
    ret.tab1_99retention_and_streaming99cumulative_retention,
    ret.tab1_99retention_and_streaming99mop_failure_rate,
    stream.tab1_99retention_and_streaming99streamed_ge0,
    stream.tab1_99retention_and_streaming99streamed_ge1,
    stream.tab1_99retention_and_streaming99streamed_ge5,
    stream.tab1_99retention_and_streaming99streamed_ge10,
    stream.tab1_99retention_and_streaming99streamed_ge20,
    stream.tab1_99retention_and_streaming99streamed_ge40,
    stream.tab1_99retention_and_streaming99streamed_ge80,
    ret.number_of_allocations
FROM {userdb}.manual_dashboard_streaming_{testid} stream
JOIN (
    SELECT
        test_cell_nbr,
        filter_allocation_type,
        filter_signup_region,
        filter_completed_activity_window,
        filter_country_group,
        -- allocation filters
        {allocation_filter_names}
        SUM(number_of_allocations) AS number_of_allocations,
        SUM(cumulative_retention) AS tab1_99retention_and_streaming99cumulative_retention,
        SUM(mop_failure_rate) AS tab1_99retention_and_streaming99mop_failure_rate,
        SUM(number_of_allocations) * SUM(collected_revenue) / CAST(SUM(potential_revenue) AS DOUBLE) AS tab1_99retention_and_streaming99revenue_weighted_retention
    FROM {userdb}.manual_dashboard_streaming_{testid}
    -- activity filters
    WHERE {activity_filter_names_in_all_or_no}
    GROUP BY 
        test_cell_nbr,
        filter_allocation_type,
        filter_signup_region,
        filter_completed_activity_window,
        filter_country_group,
        -- allocation filters
        {allocation_filter_names_nc}
    ) ret
ON stream.test_cell_nbr = ret.test_cell_nbr
AND stream.filter_allocation_type = ret.filter_allocation_type
AND stream.filter_signup_region = ret.filter_signup_region
AND stream.filter_completed_activity_window = ret.filter_completed_activity_window
AND stream.filter_country_group = ret.filter_country_group
-- allocation filters
{allocation_filter_join}
;