-- Dataset building for Novelty CS Prediction --

-------------------------------------------- LEGO Feature Dataset -----------------------------------------------------

WITH product_attribute_table AS (
    SELECT
        DISTINCT communication_no,
        global_launch_date,
        theme,
        product_age_txt,
        super_segment_txt,
        product_audience_txt,
        passion_point_main_text,
        passion_point_alternative_text,
        piece_count_of_fg,
        ip_partner_txt
    FROM
        data_product_development_products.product_attributes_v2 pat
    WHERE
        global_launch_date >= '2020-01-01'AND global_launch_date < '2024-01-01'
    AND
        communication_no IS NOT NULL
    AND
        LENGTH(CAST(communication_no AS STRING)) <= 5
    AND
        ip_partner_txt IS NOT NULL
    AND
        piece_count_of_fg > 0
),
-- Product Metadata with MIN(Global Launch Date)
    product_attribute_table_min_date AS (
    SELECT
        communication_no,
        MIN(global_launch_date) AS global_launch_date,
        theme,
        product_age_txt,
        super_segment_txt,
        product_audience_txt,
        passion_point_main_text,
        passion_point_alternative_text,
        piece_count_of_fg,
        ip_partner_txt
    FROM
        product_attribute_table
    GROUP BY
        communication_no,
        theme,
        product_age_txt,
        super_segment_txt,
        product_audience_txt,
        passion_point_main_text,
        passion_point_alternative_text,
        piece_count_of_fg,
        ip_partner_txt
),
-- Calendar Table
    calendar_lookup AS (
    SELECT
        DISTINCT calendar_date, calendar_year, iso_week_year, iso_445_month, iso_week
    FROM
        proj_web2becomm.dim_calendar
    WHERE
        iso_week_year IN (2020, 2021, 2022, 2023)
    ORDER BY
        calendar_date DESC
),
-- Product Metadata with Calendar
    product_metadata AS (
    SELECT
        pat.communication_no,
        pat.global_launch_date,
        cal.calendar_year,
        cal.iso_week_year,
        cal.iso_445_month,
        cal.iso_week,
        pat.theme,
        pat.product_age_txt,
        pat.super_segment_txt,
        pat.product_audience_txt,
        pat.passion_point_main_text,
        pat.passion_point_alternative_text,
        pat.piece_count_of_fg,
        pat.ip_partner_txt
    FROM
        product_attribute_table_min_date pat
    LEFT JOIN
        calendar_lookup cal
        ON pat.global_launch_date = cal.calendar_date
),
-- RRP at Comms and Year
    pricing_rrp_data AS (
    SELECT
        DISTINCT iso_year, communication_no, rsp_eur AS rrp
    FROM
        data_b2b_ecommerce.estore_price
    WHERE
        country = 'deu'
    AND
        retailer = 'amazon.de'
),
    final_rrp AS (
    SELECT
        iso_year,
        communication_no,
        ROUND(AVG(rrp), 2) AS rrp
    FROM
        pricing_rrp_data
    GROUP BY
      iso_year,
      communication_no
),
-- Product Metadata with RRP
    product_metadata_rrp AS (
    SELECT
        pat.communication_no,
        pat.global_launch_date,
        pat.calendar_year,
        pat.iso_week_year,
        pat.iso_445_month,
        pat.iso_week,
        pat.theme,
        rrp.rrp,
        pat.product_age_txt,
        pat.super_segment_txt,
        pat.product_audience_txt,
        pat.passion_point_main_text,
        pat.passion_point_alternative_text,
        pat.piece_count_of_fg,
        pat.ip_partner_txt
    FROM
        product_metadata pat
    LEFT JOIN
        final_rrp rrp
        ON pat.communication_no = rrp.communication_no
        AND pat.iso_week_year = rrp.iso_year
),
    sales AS (
    SELECT
        calendar_year,
        calendar_week,
        communication_no,
        theme_no,
        MAX(consumer_sales_nip_value_eur_cer) AS cs
    FROM
        data_market_sell_b2c.global_consumer_sales_tracking_weekly_v2
    WHERE
        cst_c6_linking_level_no = 1003961
    AND
        calendar_year IN (2020, 2021, 2022)
    GROUP BY
        calendar_year,
        calendar_week,
        communication_no,
        theme_no
),
-- Product Metadata with RRP
    product_metadata_cs AS (
    SELECT
        pat.communication_no,
        pat.global_launch_date,
--         pat.calendar_year,
        pat.iso_week_year,
        pat.iso_445_month,
        pat.iso_week,
        pat.theme,
        cs.cs,
        pat.rrp,
        CASE
            WHEN pat.rrp <= 10 THEN '0-10 EUR'
            WHEN pat.rrp <= 20 THEN '10-20 EUR'
            WHEN pat.rrp <= 50 THEN '20-50 EUR'
            WHEN pat.rrp <= 80 THEN '50-80 EUR'
            WHEN pat.rrp <= 150 THEN '80-150 EUR'
            WHEN pat.rrp > 150 THEN '+150 EUR'
        END AS rrp_group,
        pat.product_age_txt,
        pat.super_segment_txt,
        pat.product_audience_txt,
        pat.passion_point_main_text,
        pat.passion_point_alternative_text,
        pat.piece_count_of_fg,
        pat.ip_partner_txt
    FROM
        product_metadata_rrp pat
    LEFT JOIN
        sales cs
        ON pat.communication_no = cs.communication_no
        AND pat.calendar_year = cs.calendar_year
        AND pat.iso_week = cs.calendar_week
    WHERE
        cs.cs > 0
)
SELECT * FROM product_metadata_cs;


-------------------------------------------------- Amazon Dataset -----------------------------------------------------

-- Finding first day of spend and sum
WITH comms_asin_map AS (
    SELECT
        comms, asin
    FROM
        proj_web2becomm.dim_asin_comms_material_lookup
),
    min_start_data AS (
    SELECT
        asin,
        MIN(startdate) AS min_date
    FROM proj_web2becomm.ods_amz_daily_vc_manufacturing_sales_by_asin
    WHERE region = 'de'
    GROUP BY asin
),
    min_start_date_comms AS (
    SELECT
        cms.comms,
        dte.asin,
        dte.min_date
    FROM
        min_start_data dte
    LEFT JOIN
        comms_asin_map cms
        ON cms.asin = dte.asin
),
-- Returning the Comms and ASIN with its first day of spend in AMZ
    original_comms AS (
    SELECT
        comms,
        asin,
        min_date
    FROM
        (SELECT ROW_NUMBER() OVER (PARTITION BY comms ORDER BY min_date) as row_number, *
        FROM min_start_date_comms
        ) X
    WHERE row_number = 1
),
-- Filter for AMZ DE
    de_amz AS (
    SELECT
        *
    FROM proj_web2becomm.ods_amz_daily_vc_manufacturing_sales_by_asin
    WHERE region = 'de'
),
-- Merging AMZ MIN date with AMZ
    amz_min_amz_all_merge AS (
    SELECT
        org.comms,
        org.asin AS org_asin,
        amz.asin AS amz_asin,
        org.min_date,
        amz.startdate,
        amz.or_manuf_amt
    FROM original_comms org
    LEFT JOIN
        de_amz amz
        ON org.asin = amz.asin
),
-- 7 Day Range and Sum
    date_range_7days AS (
    SELECT
        comms,
        org_asin,
        min_date,
        startdate,
        or_manuf_amt
    FROM amz_min_amz_all_merge
    WHERE startdate >= min_date AND startdate <= DATE_ADD(min_date, 6)
),
    sum_7days AS (
    SELECT
        comms,
        org_asin,
        ROUND(SUM(or_manuf_amt)) AS or_7day
    FROM date_range_7days
    GROUP BY
        comms,
        org_asin
),
-- 14 Day Range and Sum
    date_range_14days AS (
    SELECT
        comms,
        org_asin,
        min_date,
        startdate,
        or_manuf_amt
    FROM amz_min_amz_all_merge
    WHERE startdate >= min_date AND startdate <= DATE_ADD(min_date, 13)
),
    sum_14days AS (
    SELECT
        comms,
        org_asin,
        ROUND(SUM(or_manuf_amt)) AS or_14day
    FROM date_range_14days
    GROUP BY
        comms,
        org_asin
),
-- 30 Day Range and Sum
    date_range_30days AS (
    SELECT
        comms,
        org_asin,
        min_date,
        startdate,
        or_manuf_amt
    FROM amz_min_amz_all_merge
    WHERE startdate >= min_date AND startdate <= DATE_ADD(min_date, 29)
),
    sum_30days AS (
    SELECT
        comms,
        org_asin,
        ROUND(SUM(or_manuf_amt)) AS or_30day
    FROM date_range_30days
    GROUP BY
        comms,
        org_asin
),
-- Left Join ASIN and Date Range SUM
    left_join_sums AS (
    SELECT
        DISTINCT amz.comms, amz.org_asin,
        dy7.or_7day,
        dy14.or_14day,
        dy30.or_30day
    FROM
        amz_min_amz_all_merge amz
    LEFT JOIN
        sum_7days dy7
        ON amz.org_asin = dy7.org_asin
    LEFT JOIN
        sum_14days dy14
        ON amz.org_asin = dy14.org_asin
    LEFT JOIN
        sum_30days dy30
        ON amz.org_asin = dy30.org_asin
)
SELECT * FROM left_join_sums
;


----------------------------------------------- FINAL DATASET ----------------------------------------------------------


WITH product_attribute_table AS (
    SELECT
        DISTINCT communication_no,
        global_launch_date,
        theme,
        product_age_txt,
        super_segment_txt,
        product_audience_txt,
        passion_point_main_text,
        passion_point_alternative_text,
        piece_count_of_fg,
        ip_partner_txt
    FROM
        data_product_development_products.product_attributes_v2 pat
    WHERE
        global_launch_date >= '2020-01-01'AND global_launch_date < '2024-01-01'
    AND
        communication_no IS NOT NULL
    AND
        LENGTH(CAST(communication_no AS STRING)) <= 5
    AND
        ip_partner_txt IS NOT NULL
    AND
        piece_count_of_fg > 0
),
-- Product Metadata with MIN(Global Launch Date)
    product_attribute_table_min_date AS (
    SELECT
        communication_no,
        MIN(global_launch_date) AS global_launch_date,
        theme,
        product_age_txt,
        super_segment_txt,
        product_audience_txt,
        passion_point_main_text,
        passion_point_alternative_text,
        piece_count_of_fg,
        ip_partner_txt
    FROM
        product_attribute_table
    GROUP BY
        communication_no,
        theme,
        product_age_txt,
        super_segment_txt,
        product_audience_txt,
        passion_point_main_text,
        passion_point_alternative_text,
        piece_count_of_fg,
        ip_partner_txt
),
-- Calendar Table
    calendar_lookup AS (
    SELECT
        DISTINCT calendar_date, calendar_year, iso_week_year, iso_445_month, iso_week
    FROM
        proj_web2becomm.dim_calendar
    WHERE
        iso_week_year IN (2020, 2021, 2022, 2023)
    ORDER BY
        calendar_date DESC
),
-- Product Metadata with Calendar
    product_metadata AS (
    SELECT
        pat.communication_no,
        pat.global_launch_date,
        cal.calendar_year,
        cal.iso_week_year,
        cal.iso_445_month,
        cal.iso_week,
        pat.theme,
        pat.product_age_txt,
        pat.super_segment_txt,
        pat.product_audience_txt,
        pat.passion_point_main_text,
        pat.passion_point_alternative_text,
        pat.piece_count_of_fg,
        pat.ip_partner_txt
    FROM
        product_attribute_table_min_date pat
    LEFT JOIN
        calendar_lookup cal
        ON pat.global_launch_date = cal.calendar_date
),
-- RRP at Comms and Year
    pricing_rrp_data AS (
    SELECT
        DISTINCT iso_year, communication_no, rsp_eur AS rrp
    FROM
        data_b2b_ecommerce.estore_price
    WHERE
        country = 'deu'
    AND
        retailer = 'amazon.de'
),
    final_rrp AS (
    SELECT
        iso_year,
        communication_no,
        ROUND(AVG(rrp), 2) AS rrp
    FROM
        pricing_rrp_data
    GROUP BY
      iso_year,
      communication_no
),
-- Product Metadata with RRP
    product_metadata_rrp AS (
    SELECT
        pat.communication_no,
        pat.global_launch_date,
        pat.calendar_year,
        pat.iso_week_year,
        pat.iso_445_month,
        pat.iso_week,
        pat.theme,
        rrp.rrp,
        pat.product_age_txt,
        pat.super_segment_txt,
        pat.product_audience_txt,
        pat.passion_point_main_text,
        pat.passion_point_alternative_text,
        pat.piece_count_of_fg,
        pat.ip_partner_txt
    FROM
        product_metadata pat
    LEFT JOIN
        final_rrp rrp
        ON pat.communication_no = rrp.communication_no
        AND pat.iso_week_year = rrp.iso_year
),
    sales AS (
    SELECT
        calendar_year,
        calendar_week,
        communication_no,
        theme_no,
        MAX(consumer_sales_nip_value_eur_cer) AS cs
    FROM
        data_market_sell_b2c.global_consumer_sales_tracking_weekly_v2
    WHERE
        cst_c6_linking_level_no = 1003961
    AND
        calendar_year IN (2020, 2021, 2022)
    GROUP BY
        calendar_year,
        calendar_week,
        communication_no,
        theme_no
),
-- Product Metadata with RRP
    product_metadata_cs AS (
    SELECT
        pat.communication_no,
        pat.global_launch_date,
--         pat.calendar_year,
        pat.iso_week_year,
        pat.iso_445_month,
        pat.iso_week,
        pat.theme,
        cs.cs,
        pat.rrp,
        CASE
            WHEN pat.rrp <= 10 THEN '0-10 EUR'
            WHEN pat.rrp <= 20 THEN '10-20 EUR'
            WHEN pat.rrp <= 50 THEN '20-50 EUR'
            WHEN pat.rrp <= 80 THEN '50-80 EUR'
            WHEN pat.rrp <= 150 THEN '80-150 EUR'
            WHEN pat.rrp > 150 THEN '+150 EUR'
        END AS rrp_group,
        pat.product_age_txt,
        pat.super_segment_txt,
        pat.product_audience_txt,
        pat.passion_point_main_text,
        pat.passion_point_alternative_text,
        pat.piece_count_of_fg,
        pat.ip_partner_txt
    FROM
        product_metadata_rrp pat
    LEFT JOIN
        sales cs
        ON pat.communication_no = cs.communication_no
        AND pat.calendar_year = cs.calendar_year
        AND pat.iso_week = cs.calendar_week
    WHERE
        cs.cs > 0
),
    comms_asin_map AS (
    SELECT
        comms, asin
    FROM
        proj_web2becomm.dim_asin_comms_material_lookup
),
    min_start_data AS (
    SELECT
        asin,
        MIN(startdate) AS min_date,
        SUM(or_manuf_amt) AS sum_or
    FROM proj_web2becomm.ods_amz_daily_vc_manufacturing_sales_by_asin
    WHERE region = 'de'
    GROUP BY asin
),
    min_start_date_comms AS (
    SELECT
        cms.comms,
        dte.asin,
        dte.min_date,
        dte.sum_or
    FROM
        min_start_data dte
    LEFT JOIN
        comms_asin_map cms
        ON cms.asin = dte.asin
),
-- -- Returning the Comms and ASIN with its first day of spend in AMZ
--     original_comms AS (
--     SELECT
--         comms,
--         asin,
--         min_date,
--         sum_or
--     FROM
--         (SELECT ROW_NUMBER() OVER (PARTITION BY comms ORDER BY min_date) as row_number, *
--         FROM min_start_date_comms
--         ) X
--     WHERE row_number = 1
-- ),
-- Filter for AMZ DE
    de_amz AS (
    SELECT
        *
    FROM proj_web2becomm.ods_amz_daily_vc_manufacturing_sales_by_asin
    WHERE region = 'de'
),
-- Merging AMZ MIN date with AMZ
    amz_min_amz_all_merge AS (
    SELECT
        org.comms,
        org.asin AS org_asin,
        amz.asin AS amz_asin,
        lgo.global_launch_date,
        org.min_date,
        amz.startdate,
        amz.or_manuf_amt,
        amz.scogs_manuf_amt
    FROM min_start_date_comms org
    LEFT JOIN
        de_amz amz
        ON org.asin = amz.asin
    LEFT JOIN
        product_metadata_cs lgo
        ON org.comms = lgo.communication_no
),
-- 7 Day Range and Sum
    date_range_7days AS (
    SELECT
        comms,
        org_asin,
        global_launch_date,
        min_date,
        startdate,
        or_manuf_amt,
        scogs_manuf_amt
    FROM amz_min_amz_all_merge
    WHERE startdate >= global_launch_date AND startdate <= DATE_ADD(global_launch_date, 6)
),
    sum_7days AS (
    SELECT
        comms,
        ROUND(SUM(or_manuf_amt)) AS or_7day,
        ROUND(SUM(scogs_manuf_amt)) AS scogs_7day
    FROM date_range_7days
    GROUP BY
        comms
),
-- 14 Day Range and Sum
    date_range_14days AS (
    SELECT
        comms,
        org_asin,
        min_date,
        startdate,
        or_manuf_amt,
        scogs_manuf_amt
    FROM amz_min_amz_all_merge
    WHERE startdate >= global_launch_date AND startdate <= DATE_ADD(global_launch_date, 13)
),
    sum_14days AS (
    SELECT
        comms,
        ROUND(SUM(or_manuf_amt)) AS or_14day,
        ROUND(SUM(scogs_manuf_amt)) AS scogs_14day
    FROM date_range_14days
    GROUP BY
        comms
),
-- 30 Day Range and Sum
    date_range_30days AS (
    SELECT
        comms,
        org_asin,
        min_date,
        startdate,
        or_manuf_amt,
        scogs_manuf_amt
    FROM amz_min_amz_all_merge
    WHERE startdate >= global_launch_date AND startdate <= DATE_ADD(global_launch_date, 29)
),
    sum_30days AS (
    SELECT
        comms,
        ROUND(SUM(or_manuf_amt)) AS or_30day,
        ROUND(SUM(scogs_manuf_amt)) AS scogs_30day
    FROM date_range_30days
    GROUP BY
        comms
),
-- Left Join ASIN and Date Range SUM
    left_join_sums AS (
    SELECT
        DISTINCT amz.comms,
        dy7.or_7day,
        dy14.or_14day,
        dy30.or_30day,
        dy7.scogs_7day,
        dy14.scogs_14day,
        dy30.scogs_30day
    FROM
        amz_min_amz_all_merge amz
    LEFT JOIN
        sum_7days dy7
        ON amz.comms = dy7.comms
    LEFT JOIN
        sum_14days dy14
        ON amz.comms = dy14.comms
    LEFT JOIN
        sum_30days dy30
        ON amz.comms = dy30.comms
),
    lego_features_amz_or_merge AS (
    SELECT
        lgo.*,
        amz.*
    FROM
        product_metadata_cs lgo
    LEFT JOIN
        left_join_sums amz
        ON lgo.communication_no = amz.comms
),
    average_theme_cs AS (
-- Average Theme CS
    SELECT
        calendar_year,
        theme_no,
        AVG(consumer_sales_nip_value_eur_cer) AS avg_theme_cs
    FROM
        data_market_sell_b2c.global_consumer_sales_tracking_weekly_v2
    WHERE
        cst_c6_linking_level_no = 1003961
    AND
        calendar_year IN (2020, 2021, 2022)
    GROUP BY
        calendar_year,
        theme_no
),
    features_with_cs_left_join AS (
    SELECT
        fnl.*,
        acs.avg_theme_cs
    FROM
        lego_features_amz_or_merge fnl
    LEFT JOIN
        average_theme_cs acs
        ON fnl.iso_week_year = acs.calendar_year
        AND fnl.theme = acs.theme_no
),
    features_with_top_theme_join AS (
    SELECT
        lgo.comms,
        lgo.theme,
        thm.top_theme_text AS top_theme_txt,
        lgo.global_launch_date,
        lgo.iso_week_year,
        lgo.iso_445_month,
        lgo.iso_week,
        lgo.rrp,
        lgo.rrp_group,
        lgo.piece_count_of_fg,
        lgo.super_segment_txt,
        lgo.product_audience_txt,
        lgo.product_age_txt,
        lgo.passion_point_main_text AS passion_point_main_txt,
        lgo.passion_point_alternative_text AS passion_point_alternative_txt,
        lgo.ip_partner_txt,
        lgo.cs AS launch_week_cs,
        lgo.avg_theme_cs AS avg_weekly_theme_cs,
        lgo.or_7day,
        lgo.or_14day,
        lgo.or_30day,
        lgo.scogs_7day,
        lgo.scogs_14day,
        lgo.scogs_30day
    FROM
        features_with_cs_left_join lgo
    LEFT JOIN
        proj_web2becomm.dim_theme_lookup thm
        ON lgo.theme = thm.theme
)
SELECT * FROM features_with_top_theme_join
WHERE global_launch_date >= '2020-01-01' -- This filter needs to exist because the AMZ dataset in our data lake does not have data before 2020-10-21. Since the AMZ query takes the SUM(OR) starting from the minimum date in the AMZ dataset and we are joining that final AMZ sum on COMMs without taken into account the Global Launch Date, for any COMMs that is launched before 2020-10-21, we'll get unrepresentative OR
AND or_7day >= 0
;