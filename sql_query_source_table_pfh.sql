-- Why olf subquery - contains all our major columns used in the logic

WITH olf AS 
(
    SELECT 
        order_id,      -- used in logic 
        order_date,    -- used to filter
        captain_id,    -- used for count 
        customer_id,   -- used for count
        customer_obj_gender AS customer_gender,   -- used for logic building
        city_name,     -- will be used for running loop on different cities
        customer_feedback_rating,   -- used for ratings 
        started_epoch,              -- used
        customer_rated_epoch,       -- used
        dropped_epoch,              -- used
        order_status,               -- used to filter out dropped orders 
        yyyymmdd                    -- used for some filters
    FROM orders.order_logs_fact ord
    WHERE yyyymmdd >= '{start_date}'
      AND yyyymmdd <= '{end_date}'
      AND service_category IN ('link')
      AND city IN 
      (
          '5740135d4fdf4798208bba24', -- Hyderabad
          '5ba090686fde19440c388a07', -- Jaipur
          '57af2db19729ad145ddbba66', -- Chennai
          '572ca7ff116b5db3057bd814', -- Bangalore
          '5bc5acb112477c2ece769599', -- Kolkata
          '5bc5ac2312477c2ece769591', -- Delhi
          '5bc5ac7012477c2ece769595'  -- Mumbai
      )
),

-- Why captain base subquery - take the distinct captain base from above subquery and run some filtration logic 

captain_base AS 
(
    SELECT DISTINCT captain_id
    FROM olf
    WHERE captain_id IS NOT NULL
),

-- step_a subquery is used to pick the legit service_name for our final usecase and filtering out any captain associated with invalid service name

step_a AS 
(
    SELECT 
        cs.riderinfo__userid AS captain_id,
        cs.riderinfo__servicenames,
        CASE
            WHEN LOWER(cs.riderinfo__servicenames) LIKE '%bike rated by women%' THEN 'prefbyher'
            WHEN LOWER(cs.riderinfo__servicenames) LIKE '%scooty%' THEN 'bike_scooter'
            WHEN LOWER(cs.riderinfo__servicenames) LIKE '%bike%'
              OR LOWER(cs.riderinfo__servicenames) LIKE '%link%' THEN 'bike_motorbike'
            ELSE 'others'
        END AS service_type,
        ROW_NUMBER() OVER (
            PARTITION BY cs.riderinfo__userid 
            ORDER BY cs.updated_epoch DESC
        ) AS rn
    FROM hive.canonical.iceberg_domain_entities_captainservices_immutable cs
    INNER JOIN captain_base cb
        ON cs.riderinfo__userid = cb.captain_id
),

-- csi subquery - just an extension of step_a which is essentially removing all the captains wherever the service name is 'others'

csi AS 
(
    SELECT *
    FROM step_a
    WHERE rn = 1
      AND service_type != 'others'
),

-- rs subquery - this query is needed to filter out only active captains and get the ride_city and shift_name of the captains 

rs AS 
(
    WITH rs_sub AS 
    (
        SELECT 
            rs._id AS captain_id,
            rs.shift_name,
            rs.mobile,
            rs.city,
            ROW_NUMBER() OVER (
                PARTITION BY rs._id 
                ORDER BY rs.created_on DESC
            ) AS rn
        FROM entity.riders_snapshot rs
        INNER JOIN captain_base cb
            ON rs._id = cb.captain_id
        WHERE rs.active = true
    )

    SELECT 
        captain_id,
        shift_name,
        mobile,
        city
    FROM rs_sub 
    WHERE rn = 1
      AND city IN 
      (
          '5740135d4fdf4798208bba24', -- Hyderabad
          '5ba090686fde19440c388a07', -- Jaipur
          '57af2db19729ad145ddbba66', -- Chennai
          '572ca7ff116b5db3057bd814', -- Bangalore
          '5bc5acb112477c2ece769599', -- Kolkata
          '5bc5ac2312477c2ece769591', -- Delhi
          '5bc5ac7012477c2ece769595'  -- Mumbai
      )
),

-- csj table - to include only link captains 

csj AS 
(
    SELECT DISTINCT csj.captain_id
    FROM datasets.captain_supply_journey_summary csj
    INNER JOIN captain_base cb
        ON csj.captain_id = cb.captain_id
    WHERE csj.mode_id = '5fbe8a6fb1c45500077393da'
),

-- now this captain_mapping subquery - this is simply doing inner join on bunch of subqueries above to get final valid captains cohort

captain_mapping AS 
(
    SELECT 
        csi.captain_id AS cm_captain_id,
        csi.riderinfo__servicenames,
        csi.service_type,
        rs.shift_name,
        rs.city AS ride_city
    FROM csi
    INNER JOIN rs 
        ON csi.captain_id = rs.captain_id
    INNER JOIN csj 
        ON csi.captain_id = csj.captain_id
),

-- ocara - this is a primary signal for us where we are filtering out captains who cherry pick women ride. only the relevant columns are there 

ocara AS 
(
    WITH step_a AS 
    (
        SELECT 
            captain_id AS ocara_captain_id,
            event_type AS ocara_event_type,
            customer_gender AS ocara_customer_gender,
            yyyymmdd AS ocara_yyyymmdd,
            ROW_NUMBER() OVER (
                PARTITION BY order_id 
                ORDER BY yyyymmdd DESC
            ) AS rn 
        FROM experiments.ocara_event_fact_classified
        WHERE yyyymmdd >= '{start_date}'
          AND yyyymmdd <= '{end_date}'
          AND LOWER(city) IN ('bangalore','hyderabad','jaipur','chennai','kolkata','delhi','mumbai')
          AND LOWER(service) IN ('link')
          AND LOWER(event_type) IN ('rider_cancelled', 'customer_cancelled')
          AND LOWER(customer_gender) <> 'unknown'
    )

    SELECT 
        ocara_yyyymmdd,
        ocara_captain_id,

        SUM(
            CASE 
                WHEN LOWER(ocara_customer_gender) = 'male' 
                 AND ocara_event_type = 'rider_cancelled' 
                THEN 1 
                ELSE 0 
            END
        ) AS male_ocara_count_captainlevel,

        SUM(
            CASE 
                WHEN LOWER(ocara_customer_gender) = 'female' 
                 AND ocara_event_type = 'rider_cancelled' 
                THEN 1 
                ELSE 0 
            END
        ) AS female_ocara_count_captainlevel,

        SUM(
            CASE 
                WHEN LOWER(ocara_customer_gender) = 'male'  
                 AND ocara_event_type = 'customer_cancelled' 
                THEN 1 
                ELSE 0 
            END
        ) AS male_cc_count_captainlevel,

        SUM(
            CASE 
                WHEN LOWER(ocara_customer_gender) = 'female' 
                 AND ocara_event_type = 'customer_cancelled' 
                THEN 1 
                ELSE 0 
            END
        ) AS female_cc_count_captainlevel

    FROM step_a 
    WHERE rn = 1
    GROUP BY 1, 2
),

-- calls_data subquery: again this is a primary signal for us where we detect if a captain selectively calls female customers more than male customers

calls_data AS 
(
    SELECT 
        eventprops__orderid AS cd_order_id,
        COUNT(DISTINCT eventid) AS total_calls_doneby_captain
    FROM canonical.iceberg_67a68d6a_callCustomer_immutable
    WHERE yyyymmdd >= '{start_date}'
      AND yyyymmdd <= '{end_date}'
      AND LOWER(eventprops__city) IN ('bangalore','hyderabad','jaipur','chennai','kolkata','delhi','mumbai')
    GROUP BY 1
),

-- text_ai_agent_base:
-- common base for all 3 input types.
-- keeping only CHAT, SUPPORT_TICKET and POST_RIDE_REVIEW.
-- response__issue_type is filtered as non-null before joining.

text_ai_agent_base AS 
(
    WITH output AS 
    (
        SELECT 
            row_id,
            yyyymmdd,
            response__issue_type
        FROM canonical.iceberg_info_captain_chat_classification_agent_immutable
        WHERE yyyymmdd >= '{start_date}' 
          AND yyyymmdd <= '{end_date}'
          AND response__issue_type IS NOT NULL
    ),

    input AS 
    (
        SELECT 
            row_id,
            city_name,
            yyyymmdd,
            order_id,
            captain_id,
            UPPER(input_type) AS input_type
        FROM hive.reports.captain_ai_agent_input_chat_view
        WHERE yyyymmdd >= '{start_date}' 
          AND yyyymmdd <= '{end_date}'
          AND LOWER(city_name) IN ('bangalore','hyderabad','jaipur','chennai','kolkata','delhi','mumbai')
          AND UPPER(input_type) IN ('CHAT', 'SUPPORT_TICKET', 'POST_RIDE_REVIEW')
          AND order_id IS NOT NULL
          AND captain_id IS NOT NULL
    )

    SELECT 
        input.order_id              AS agent_order_id,
        input.captain_id            AS agent_captain_id,
        input.input_type            AS agent_input_type,
        input.city_name             AS agent_city_name,
        output.response__issue_type AS agent_response_issue_type,
        input.yyyymmdd              AS agent_yyyymmdd
    FROM input 
    INNER JOIN output 
        ON input.row_id = output.row_id
),

-- chat-level text ai signal:
-- one latest non-null CHAT issue type per order_id + captain_id.
-- if multiple non-null values exist on the same latest day, pick any one.

text_ai_agent_chat AS 
(
    SELECT 
        agent_order_id AS chat_agent_order_id,
        agent_captain_id AS chat_agent_captain_id,
        agent_input_type AS chat_agent_input_type,
        agent_response_issue_type AS chat_agent_response_issue_type
    FROM 
    (
        SELECT 
            agent_order_id,
            agent_captain_id,
            agent_input_type,
            agent_response_issue_type,
            ROW_NUMBER() OVER (
                PARTITION BY agent_order_id, agent_captain_id
                ORDER BY agent_yyyymmdd DESC
            ) AS rn
        FROM text_ai_agent_base
        WHERE agent_input_type = 'CHAT'
    ) x
    WHERE rn = 1
),

-- support-ticket-level text ai signal:
-- one latest non-null SUPPORT_TICKET issue type per order_id + captain_id.
-- if multiple non-null values exist on the same latest day, pick any one.

text_ai_agent_support_ticket AS 
(
    SELECT 
        agent_order_id AS support_ticket_agent_order_id,
        agent_captain_id AS support_ticket_agent_captain_id,
        agent_input_type AS support_ticket_agent_input_type,
        agent_response_issue_type AS support_ticket_agent_response_issue_type
    FROM 
    (
        SELECT 
            agent_order_id,
            agent_captain_id,
            agent_input_type,
            agent_response_issue_type,
            ROW_NUMBER() OVER (
                PARTITION BY agent_order_id, agent_captain_id
                ORDER BY agent_yyyymmdd DESC
            ) AS rn
        FROM text_ai_agent_base
        WHERE agent_input_type = 'SUPPORT_TICKET'
    ) x
    WHERE rn = 1
),

-- post-ride-review-level text ai signal:
-- one latest non-null POST_RIDE_REVIEW issue type per order_id + captain_id.
-- if multiple non-null values exist on the same latest day, pick any one.

text_ai_agent_post_ride_review AS 
(
    SELECT 
        agent_order_id AS post_ride_review_agent_order_id,
        agent_captain_id AS post_ride_review_agent_captain_id,
        agent_input_type AS post_ride_review_agent_input_type,
        agent_response_issue_type AS post_ride_review_agent_response_issue_type
    FROM 
    (
        SELECT 
            agent_order_id,
            agent_captain_id,
            agent_input_type,
            agent_response_issue_type,
            ROW_NUMBER() OVER (
                PARTITION BY agent_order_id, agent_captain_id
                ORDER BY agent_yyyymmdd DESC
            ) AS rn
        FROM text_ai_agent_base
        WHERE agent_input_type = 'POST_RIDE_REVIEW'
    ) x
    WHERE rn = 1
),

-- now this subquery new_feedback: this subquery is used to collect signals on our new safety response we ask. all the columns taken are used in the final logic

new_feedback AS 
(
    WITH sdi_tbl AS 
    (
        SELECT 
            service_level, 
            city_display_name, 
            service_detail_id
        FROM datasets.service_mapping
        WHERE service_level IN ('Link', 'Scooty', 'Bike Rated by Women')
          AND LOWER(city_display_name) IN ('bangalore','hyderabad','jaipur','chennai','kolkata','delhi','mumbai')
    ),

    users_tbl AS 
    (
        SELECT
            _id AS customer_id,
            CASE 
                WHEN gender = 0 THEN 'Male'
                WHEN gender = 1 THEN 'Female'
                ELSE 'Others' 
            END AS customer_gender
        FROM entity.users_snapshot
    ),

    live_feedback_raw AS 
    (
        SELECT
            a.data__orderid AS order_id,
            a.eventtime AS eventtime_livefeedback,
            a.data__text AS live_feedback_text,
            a.data__feedback AS live_feedback_feedback,
            ROW_NUMBER() OVER (
                PARTITION BY a.data__orderid
                ORDER BY a.eventtime DESC
            ) AS rn
        FROM iceberg.canonical.iceberg_domain_rapido_quality_immutable a
        INNER JOIN users_tbl d 
            ON a.data__customerid = d.customer_id
        INNER JOIN sdi_tbl s 
            ON a.data__servicedetail = s.service_detail_id
        WHERE a.yyyymmdd BETWEEN '{start_date}' AND '{end_date}'
          AND a.data__additionalfeedback = 'liveFeedback'
          AND a.data__feedback IS NOT NULL
          AND  data__questionid in ('69c5042209637f1e5928ea8c', '69a0409cbf982b02b7c76713', '69b7fbd7eff2e7db86ff29a7', '69e8b9154c3ca1633f34f5e6', '69b7feab0f2fd330c732f88f','69eb2fe47dd5f8572670ac3d')
    ),

    rating_screen_raw AS 
    (
        SELECT
            a.data__orderid AS order_id,
            a.eventtime AS eventtime_rating_screen,
            a.data__text AS rating_screen_text,
            a.data__feedback AS rating_screen_feedback,
            ROW_NUMBER() OVER (
                PARTITION BY a.data__orderid
                ORDER BY a.eventtime DESC
            ) AS rn
        FROM iceberg.canonical.iceberg_domain_rapido_quality_immutable a
        INNER JOIN users_tbl d 
            ON a.data__customerid = d.customer_id
        INNER JOIN sdi_tbl s 
            ON a.data__servicedetail = s.service_detail_id
        WHERE a.yyyymmdd BETWEEN '{start_date}' AND '{end_date}'
          AND a.data__screen = 'ratings'
          AND  data__questionid in ('69c5042209637f1e5928ea8c', '69a0409cbf982b02b7c76713', '69b7fbd7eff2e7db86ff29a7', '69e8b9154c3ca1633f34f5e6','69b7feab0f2fd330c732f88f','69eb2fe47dd5f8572670ac3d')
          AND a.data__feedback IS NOT NULL
    ),

    live_feedback AS 
    (
        SELECT * 
        FROM live_feedback_raw 
        WHERE rn = 1
    ),

    rating_screen AS 
    (
        SELECT * 
        FROM rating_screen_raw 
        WHERE rn = 1
    )

    SELECT
        COALESCE(rs.order_id, lf.order_id) AS newfeedback_order_id,
        rs.eventtime_rating_screen,
        rs.rating_screen_text,
        rs.rating_screen_feedback,
        lf.eventtime_livefeedback,
        lf.live_feedback_text,
        lf.live_feedback_feedback
    FROM rating_screen rs
    FULL OUTER JOIN live_feedback lf
        ON rs.order_id = lf.order_id
)

-- now the final select 

SELECT 
    olf.order_id,
    olf.order_date,
    olf.captain_id,
    olf.customer_id,
    olf.customer_gender,
    olf.city_name,
    olf.customer_feedback_rating,
    olf.started_epoch,
    olf.customer_rated_epoch,
    olf.dropped_epoch,
    olf.order_status,
    olf.yyyymmdd,

    cm.riderinfo__servicenames,
    cm.service_type,
    cm.shift_name,
    cm.ride_city,

    ocara.male_ocara_count_captainlevel,
    ocara.female_ocara_count_captainlevel,
    ocara.male_cc_count_captainlevel,
    ocara.female_cc_count_captainlevel,

    calls_data.total_calls_doneby_captain,

    ta_chat.chat_agent_input_type,
    ta_chat.chat_agent_response_issue_type,

    ta_st.support_ticket_agent_input_type,
    ta_st.support_ticket_agent_response_issue_type,

    ta_pr.post_ride_review_agent_input_type,
    ta_pr.post_ride_review_agent_response_issue_type,

    new_feedback.eventtime_rating_screen,
    new_feedback.rating_screen_text,
    new_feedback.rating_screen_feedback,
    new_feedback.eventtime_livefeedback,
    new_feedback.live_feedback_text,
    new_feedback.live_feedback_feedback

FROM olf

INNER JOIN captain_mapping cm
    ON olf.captain_id = cm.cm_captain_id

LEFT JOIN ocara 
    ON olf.captain_id = ocara.ocara_captain_id
   AND olf.yyyymmdd = ocara.ocara_yyyymmdd

LEFT JOIN calls_data
    ON olf.order_id = calls_data.cd_order_id

LEFT JOIN text_ai_agent_chat ta_chat
    ON olf.order_id = ta_chat.chat_agent_order_id
   AND olf.captain_id = ta_chat.chat_agent_captain_id

LEFT JOIN text_ai_agent_support_ticket ta_st
    ON olf.order_id = ta_st.support_ticket_agent_order_id
   AND olf.captain_id = ta_st.support_ticket_agent_captain_id

LEFT JOIN text_ai_agent_post_ride_review ta_pr
    ON olf.order_id = ta_pr.post_ride_review_agent_order_id
   AND olf.captain_id = ta_pr.post_ride_review_agent_captain_id

LEFT JOIN new_feedback
    ON olf.order_id = new_feedback.newfeedback_order_id

-- this is the complete sql file, additionally have added the cities filters specifically for which we need the data