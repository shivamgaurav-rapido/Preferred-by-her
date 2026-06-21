# PBH seeding pipeline — single-df source 
# All 5 risky-captain signals + df_new_feedback (live_feedback + rating_screen) + shift filter.

import os
import gc
import warnings
from datetime import date
from dateutil.relativedelta import relativedelta

import polars as pl

warnings.filterwarnings("ignore")

# =============================================================================
# CONFIG
# =============================================================================
#
# ## What this section controls
# This section defines all runtime inputs and outputs for the PBH seeding
# pipeline. The script reads one pre-built parquet source, applies the
# production eligibility and risk-removal logic, and writes three cohort files
# plus one audit log.
#
# ## Columns expected from the source parquet
# The downstream logic expects captain/order metadata and the signal columns
# produced by the SQL base query:
# - captain_id, order_id, order_date, order_status, ride_city
# - customer_gender, customer_feedback_rating
# - service_type, mobile, shift_name
# - chat_agent_input_type, chat_agent_response_issue_type
# - support_ticket_agent_input_type, support_ticket_agent_response_issue_type
# - post_ride_review_agent_input_type, post_ride_review_agent_response_issue_type
# - total_calls_doneby_captain
# - ocara_event_type
# - live_feedback_feedback, rating_screen_feedback
#
# ## Important production note
# CITY_ID is required by the parquet pushdown filter and by the base cohort
# filter. In the uploaded file it was commented out, so the script would fail
# with NameError before reading data. It is explicitly set below for Bangalore,
# matching the BLR shift list already used later in the file.

BASE_DIR = "/Users/SVG/Documents/rapido_ds/preffered_by_her/notebooks"

SOURCE_DF_FILE   = f"{BASE_DIR}/bang_brw_3june.parquet"     # <-- single source df (parquet)

RUN_TAG          = date.today().strftime("%d%b%Y").lower()       # e.g. 22may2026
OUT_SEEDING      = f"{BASE_DIR}/shivam_seeding_list_{RUN_TAG}_prefbyher.csv"
OUT_NMINUS1      = f"{BASE_DIR}/seed_v19_nminus1_{RUN_TAG}.csv"
OUT_FINAL        = f"{BASE_DIR}/pfh_{RUN_TAG}_baserefresh.csv"
OUT_AUDIT        = f"{BASE_DIR}/audit_log_{RUN_TAG}.csv"

CITY_IDS = [
    "5740135d4fdf4798208bba24",  # Hyderabad
    "5ba090686fde19440c388a07",  # Jaipur
    "57af2db19729ad145ddbba66",  # Chennai
    "572ca7ff116b5db3057bd814",  # Bangalore
    "5bc5acb112477c2ece769599",  # Kolkata
    "5bc5ac2312477c2ece769591",  # Delhi
    "5bc5ac7012477c2ece769595",  # Mumbai
]  # currently BLR; swap for other cities
MIN_RIDES_PER_GENDER = 5                            # used by Signal 4 (calls) and Signal 5 (ocara)

# BLR shifts
SHIFT_LIST = [
    "BLR_LINK", "BLR_CU", "BLR_KM", "BLR_CU1", "FT_Bounce_BLR",
    "BLR_MC_Bellandur", "BLR_BIKE_LINK_PINK", "BLR_Zero Commission",
    "BLR_KM1", "BLR_commision10", "BLR_Low_APR", "BLR_MC_Bellandur_1",
    "BLR_MC_Bellendur", "BLR_MG", "BLR_Test", "BLR_Test_KM", "BLR_rated_women"
]

# =============================================================================
# HELPERS
# =============================================================================
#
# ## What this section does
# These helpers keep the production code readable and consistent. They do not
# change any business logic; they only standardize common operations used later.
#
# ## Helper details
# - safe_ratio: protects percentage calculations from divide-by-zero.
# - print_summary: prints clear stage-level counts for monitoring the run.
# - _pivot_genders: converts long gender-level aggregates into one row per
#   captain, creating missing Male/Female columns with a safe default.
#
# ## Columns handled
# _pivot_genders expects captain_id, gender, and the value_col passed by the
# caller. It is used in gender-asymmetry signals where Female and Male metrics
# need to be compared side by side.

def safe_ratio(n, d):
    return float(n) / float(d) if d else 0.0

def print_summary(title, rows):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    for k, v in rows:
        print(f"{k:<42} : {v}")

def _pivot_genders(df, value_col, rename_map, fill=0):
    """Pivot on 'gender' and rename; create missing gender columns with fill."""
    p = df.pivot(values=value_col, index="captain_id", on="gender")
    for old, new in rename_map.items():
        if old in p.columns:
            p = p.rename({old: new})
        else:
            p = p.with_columns(pl.lit(fill).cast(pl.Int64).alias(new))
    return p

# =============================================================================
# LOAD + COHORT (df_cohort)
# =============================================================================
#
# ## Signal / section purpose
# This is the eligibility foundation for the whole pipeline. Before any risky
# captain removal is applied, the script builds a clean candidate cohort of
# captains who are active, belong to the target city, have dropped rides, and
# have a minimum positive female-rider history.
#
# ## Logic in simple language
# 1. Read the source parquet with row-group filter pushdown wherever possible.
#    This keeps only the target city, dropped orders, and roughly the last six
#    months before materializing data in memory.
# 2. Standardize key column types so filters and comparisons behave reliably.
# 3. Keep only known binary customer genders: 0 = Male and 1 = Female.
# 4. Build a six-month dropped-order base for the target city.
# 5. Restrict the cohort universe to captains active in the last three months.
# 6. Keep only captains who have at least 3 female rides, at least 1 rated
#    female ride, and zero bad female ratings.
# 7. Restrict all downstream signal logic to this eligible cohort only.
#
# ## Columns utilized
# - ride_city: city filter and parquet pushdown.
# - order_status: keeps dropped orders only.
# - order_date: six-month base window and last-three-month active cohort.
# - captain_id: captain-level cohort and downstream joins.
# - order_id: ride counting.
# - customer_gender: Female/Male cohort quality checks.
# - customer_feedback_rating: rated ride and bad-rating checks.
# - total_calls_doneby_captain: cast here for later Signal 4 usage.

today = date.today()
six_months_ago_dt   = pl.lit(today - relativedelta(months=6)).cast(pl.Datetime)
three_months_ago_dt = pl.lit(today - relativedelta(months=3)).cast(pl.Datetime)
today_dt            = pl.lit(today).cast(pl.Datetime)

import pyarrow.parquet as pq
import pyarrow.compute as pc

# 4.4 GB parquet on a 16-32 GB Mac → eager read OOM-kills the process.
# Use pyarrow's row-group filter pushdown so we only ever materialize the
# rows the pipeline actually uses: city == CITY_ID AND dropped AND last 6 months.
# This typically reduces the in-memory footprint to a small fraction.
#
# We do *not* push down customer_gender / customer_feedback_rating filters
# because those are applied downstream after type casting (parquet may store
# them as strings in some writes).
six_months_ago_dtstr = (today - relativedelta(months=6)).isoformat()

_parquet_filter = (
    pc.field("ride_city").isin(CITY_IDS) &
    (pc.field("order_status") == "dropped") &
    (pc.field("order_date") >= six_months_ago_dtstr)
)

try:
    arrow_tbl = pq.read_table(SOURCE_DF_FILE, filters=_parquet_filter)
except Exception:
    # Fallback: filter pushdown unsupported by writer; load full table.
    # On a memory-constrained machine this may still OOM — in that case
    # split the source by city or date and rerun.
    arrow_tbl = pq.read_table(SOURCE_DF_FILE)

df_raw = pl.from_arrow(arrow_tbl)
del arrow_tbl   # free pyarrow buffer; polars now owns the data

# order_date may be either string (CSV roundtrip) or already a datetime
# (clean parquet). Pick the right parser so we don't crash either way.
if df_raw.schema["order_date"] == pl.Utf8:
    order_date_expr = pl.col("order_date").str.to_datetime("%Y-%m-%d", strict=False)
else:
    order_date_expr = pl.col("order_date").cast(pl.Datetime)

df = (
    df_raw
    .with_columns([
        pl.col("captain_id").cast(pl.Utf8),
        pl.col("order_id").cast(pl.Utf8),
        order_date_expr,
        # Cast types up front so downstream filters/== checks behave as ints/floats.
        # In some parquet writes, gender/calls land as strings — protect against that.
        pl.col("customer_gender").cast(pl.Int32, strict=False),
        pl.col("customer_feedback_rating").cast(pl.Float64, strict=False),
        pl.col("total_calls_doneby_captain").cast(pl.Float64, strict=False),
    ])
    .filter(pl.col("customer_gender").is_in([0, 1]))   # 0=Male, 1=Female; drop nulls/strays
)
del df_raw

# 6-month base in city, dropped orders only
base = df.filter(
    (pl.col("order_date") >= six_months_ago_dt) &
    (pl.col("order_date") <= today_dt) &
    (pl.col("order_status") == "dropped") &
    (pl.col("ride_city").is_in(CITY_IDS))
)

# captains active in last 3 months (cohort universe)
cohort_caps = (
    base
    .filter(pl.col("order_date") >= three_months_ago_dt)
    .select(pl.col("captain_id").unique())
)

cohort_orders = base.join(cohort_caps, on="captain_id", how="inner")

# female total / rated / bad-rated counts per captain
fem = cohort_orders.filter(pl.col("customer_gender") == 1)

female_stats = (
    fem.group_by("captain_id").agg([
        pl.len().alias("female_total_rides"),
        pl.col("customer_feedback_rating").is_in([1, 2, 3, 4, 5]).sum().alias("female_rated_rides"),
        pl.col("customer_feedback_rating").is_in([1, 2, 3]).sum().alias("bad_female_ratings"),
    ])
)

df_cohort = (
    female_stats
    .filter(
        (pl.col("female_total_rides") >= 3) &
        (pl.col("female_rated_rides") >= 1) &
        (pl.col("bad_female_ratings") == 0)
    )
    .select("captain_id")
    .unique()
)

cohort_ids = set(df_cohort.get_column("captain_id").to_list())

# restrict everything downstream to cohort captains
base_cohort = cohort_orders.filter(pl.col("captain_id").is_in(cohort_ids))

print_summary("COHORT BUILD", [
    ("source_rows",          f"{df.height:,}"),
    ("base_6m_dropped",      f"{base.height:,}"),
    ("cohort_captains",      f"{df_cohort.height:,}"),
    ("cohort_orders",        f"{base_cohort.height:,}"),
])

# =============================================================================
# SIGNAL 1: IN-APP CHAT - rude behavior
# =============================================================================
#
# ## Signal purpose
# This signal removes captains who have an in-app chat classification of
# rude behavior. It is a direct safety/experience signal from chat-based
# customer-captain interactions.
#
# ## Logic in simple language
# The code looks only at cohort rides where the AI-agent input type is CHAT.
# It limits the classification values to the known chat issue taxonomy used by
# this pipeline, derives a readable gender label from customer_gender, and then
# flags every captain who has at least one CHAT record classified as
# rude behavior.
#
# ## Columns utilized
# - chat_agent_input_type: selects CHAT records.
# - chat_agent_response_issue_type: identifies rude behavior and valid chat labels.
# - customer_gender: derives Male/Female labels for summary and audit evidence.
# - captain_id: final captain-level flag.

CHAT_ISSUES = ["demanded extra cash", "No issue", "pickup location issue", "rude behavior"]

chat_df = (
    base_cohort
    .filter(pl.col("chat_agent_response_issue_type").is_in(CHAT_ISSUES))
    .with_columns(
        pl.when(pl.col("customer_gender") == 1).then(pl.lit("Female"))
        .otherwise(pl.lit("Male")).alias("gender")
    )
)

rude = chat_df.filter(pl.col("chat_agent_response_issue_type") == "rude behavior")
ids_inappchat = set(rude.get_column("captain_id").unique().to_list())

chat_summary = rude.select([
    pl.col("captain_id").n_unique().alias("caps"),
    (pl.col("gender") == "Female").sum().alias("fem"),
    (pl.col("gender") == "Male").sum().alias("male"),
])

print_summary("SIGNAL 1: IN-APP CHAT", [
    ("captains_flagged",  f"{chat_summary['caps'][0]:,}"),
    ("female_incidents",  f"{chat_summary['fem'][0]:,}"),
    ("male_incidents",    f"{chat_summary['male'][0]:,}"),
])

# =============================================================================
# SIGNAL 2: POST-RIDE ESCALATION - P0, P1 only
# =============================================================================
#
# ## Signal purpose
# This signal removes captains with severe post-ride support-ticket escalations.
# P0 and P1 are treated as high-severity enough to exclude from the seeding
# cohort. P2 and P3 are read into the ticket universe for monitoring, but are
# not removal tiers in this logic.
#
# ## Logic in simple language
# The code keeps cohort records where the AI-agent input type is
# SUPPORT_TICKET and the response issue type is one of P0/P1/P2/P3. From that
# ticket universe, only captains with P0 or P1 are flagged.
#
# ## Columns utilized
# - support_ticket_agent_input_type: selects SUPPORT_TICKET records.
# - support_ticket_agent_response_issue_type: reads ticket severity tier and removes P0/P1.
# - captain_id: final captain-level flag.

TICKET_TIERS_ALL    = ["P0", "P1", "P2", "P3"]
TICKET_TIERS_REMOVE = ["P0", "P1"]

ticket_df = (
    base_cohort
    .filter(pl.col("support_ticket_agent_response_issue_type").is_in(TICKET_TIERS_ALL))
)

ids_ticket = set(
    ticket_df
    .filter(pl.col("support_ticket_agent_response_issue_type").is_in(TICKET_TIERS_REMOVE))
    .get_column("captain_id").unique().to_list()
)

print_summary("SIGNAL 2: POST-RIDE ESCALATION", [
    ("tickets_total",      f"{ticket_df.height:,}"),
    ("p0_p1_captains",     f"{len(ids_ticket):,}"),
])

# =============================================================================
# SIGNAL 3: FEEDBACK REVIEWS - pre-classified labels (L0/L1/L2/L3)
# =============================================================================
#
# ## Signal purpose
# This signal removes captains based on post-ride review classifications. The
# review labels are already pre-classified into severity buckets by the upstream
# AI-agent flow. The logic treats the highest-risk labels as direct removals
# and handles medium-risk L2 using a gender-asymmetry threshold.
#
# ## Logic in simple language
# 1. Keep only POST_RIDE_REVIEW records with labels in L0/L1/L2/L3.
# 2. L0_HIGHEST_THREAT: any occurrence flags the captain.
# 3. L1_HIGH_CONCERN: flags the captain only when the review came from a
#    female customer.
# 4. L2_MEDIUM: calculate L2 rate separately for Female and Male rides. A
#    captain is analyzable only if they have at least one L2 from both genders.
#    The captain is flagged when Female L2 rate minus Male L2 rate is at or
#    above the cohort p95 threshold.
# 5. The final review-risky set is the union of L0, L1, and L2 flags.
#
# ## Columns utilized
# - post_ride_review_agent_input_type: selects POST_RIDE_REVIEW records.
# - post_ride_review_agent_response_issue_type: reads L0/L1/L2/L3 labels.
# - customer_gender: separates Female and Male review behavior.
# - order_id: unique ride counts for rate denominators.
# - captain_id: final captain-level flag.
#
# ## Derived columns
# - gender: readable Female/Male label.
# - total_rides: unique rides per captain and gender.
# - l2_count: number of L2 review records per captain and gender.
# - l2_rate: L2 count divided by total rides.
# - rate_diff: Female L2 rate minus Male L2 rate.

REVIEW_LABELS = ["L0_HIGHEST_THREAT", "L1_HIGH_CONCERN", "L2_MEDIUM", "L3_POSITIVE"]

reviews = (
    base_cohort
    .filter(pl.col("post_ride_review_agent_response_issue_type").is_in(REVIEW_LABELS))
    .with_columns(
        pl.when(pl.col("customer_gender") == 1).then(pl.lit("Female"))
        .otherwise(pl.lit("Male")).alias("gender")
    )
)

# L0: any captain who received any L0 → flag
l0_ids = set(
    reviews.filter(pl.col("post_ride_review_agent_response_issue_type") == "L0_HIGHEST_THREAT")
    .get_column("captain_id").unique().to_list()
)

# L1: captain flagged if any L1 from female customer
l1_ids = set(
    reviews.filter(
        (pl.col("post_ride_review_agent_response_issue_type") == "L1_HIGH_CONCERN") &
        (pl.col("gender") == "Female")
    ).get_column("captain_id").unique().to_list()
)

# L2: rate-diff (F - M) ≥ p95, both genders ≥1 L2
base_gender_counts = (
    base_cohort
    .with_columns(
        pl.when(pl.col("customer_gender") == 1).then(pl.lit("Female"))
        .otherwise(pl.lit("Male")).alias("gender")
    )
    .group_by(["captain_id", "gender"])
    .agg(pl.col("order_id").n_unique().alias("total_rides"))
)

l2_counts = (
    reviews.filter(pl.col("post_ride_review_agent_response_issue_type") == "L2_MEDIUM")
    .group_by(["captain_id", "gender"])
    .agg(pl.len().alias("l2_count"))
)

l2_pivot = (
    base_gender_counts.join(l2_counts, on=["captain_id", "gender"], how="left")
    .with_columns(pl.col("l2_count").fill_null(0))
    .with_columns((pl.col("l2_count") / pl.col("total_rides")).alias("l2_rate"))
    .pivot(values=["l2_count", "total_rides", "l2_rate"], index="captain_id", on="gender")
    .with_columns([
        pl.col("l2_count_Female").fill_null(0),
        pl.col("l2_count_Male").fill_null(0),
        pl.col("l2_rate_Female").fill_null(0.0),
        pl.col("l2_rate_Male").fill_null(0.0),
    ])
    .with_columns((pl.col("l2_rate_Female") - pl.col("l2_rate_Male")).alias("rate_diff"))
)

l2_analyzable = l2_pivot.filter(
    (pl.col("l2_count_Female") >= 1) & (pl.col("l2_count_Male") >= 1)
)

p95_l2 = l2_analyzable.select(pl.col("rate_diff").quantile(0.95)).item() if l2_analyzable.height else 1.0

l2_ids = set(
    l2_analyzable.filter(pl.col("rate_diff") >= p95_l2)
    .get_column("captain_id").to_list()
)

ids_review = l0_ids | l1_ids | l2_ids

print_summary("SIGNAL 3: FEEDBACK REVIEWS", [
    ("L0 flagged",        f"{len(l0_ids):,}"),
    ("L1 flagged (fem)",  f"{len(l1_ids):,}"),
    ("L2 flagged",        f"{len(l2_ids):,}"),
    ("p95_l2_threshold",  f"{p95_l2:.4f}"),
    ("total_review_risky",f"{len(ids_review):,}"),
])

# =============================================================================
# SIGNAL 4: CALLS - total_calls_doneby_captain (asymmetry by gender)
# =============================================================================
#
# ## Signal purpose
# This signal detects captains whose calling behavior is materially different
# across female and male customers. The intent is to identify unusual gendered
# calling patterns without flagging on a single noisy metric.
#
# ## Logic in simple language
# 1. Calculate the p99 value of total_calls_doneby_captain and cap call counts
#    at p99 to reduce outlier impact. Null call values are treated as zero.
# 2. For each captain and gender, compute:
#    - ride_count: unique rides.
#    - call_rate: share of rides where at least one call was made.
#    - avg_calls: average capped call volume.
#    - std_calls: standard deviation of capped call volume.
# 3. Keep only captains with at least MIN_RIDES_PER_GENDER rides for both Male
#    and Female customers, and with female call_rate greater than zero.
# 4. Compute three asymmetry metrics:
#    - call_rate_diff = Female call rate minus Male call rate.
#    - calls_diff = Female average calls minus Male average calls.
#    - cv_diff = Male coefficient of variation minus Female coefficient of
#      variation, only when Female call rate is greater than 10%.
# 5. Dynamic thresholds are set from p98, with lower floors to avoid too-small
#    thresholds: call_rate_diff >= max(p98, 0.05), calls_diff >= max(p98, 0.30),
#    cv_diff >= max(p98, 0.10).
# 6. A captain is removed only if at least two of the three thresholds breach.
#
# ## Columns utilized
# - total_calls_doneby_captain: raw per-order call count signal.
# - customer_gender: separates Female and Male behavior.
# - order_id: unique ride counts.
# - captain_id: final captain-level flag.
#
# ## Derived columns
# - calls_capped, called_atleast_once, ride_count, call_rate, avg_calls,
#   std_calls, call_rate_diff, calls_diff, cv_f, cv_m, cv_diff, f1, f2, f3,
#   breaches.

# p99 cap from non-null values (matches original: is_not_null filter, no fill_null first)
p99_calls = (
    base_cohort
    .filter(pl.col("total_calls_doneby_captain").is_not_null())
    .select(pl.col("total_calls_doneby_captain").quantile(0.99))
    .item()
) or 0.0

calls_stats = (
    base_cohort
    .with_columns([
        pl.when(pl.col("total_calls_doneby_captain").is_null())
          .then(pl.lit(0))
          .otherwise(pl.col("total_calls_doneby_captain").clip(upper_bound=p99_calls))
          .alias("calls_capped"),

        pl.when(pl.col("total_calls_doneby_captain").is_null())
          .then(pl.lit(0))
          .otherwise(pl.lit(1))
          .cast(pl.Int32)
          .alias("called_atleast_once"),

        pl.when(pl.col("customer_gender") == 1).then(pl.lit("Female"))
          .otherwise(pl.lit("Male")).alias("gender"),
    ])
    .group_by(["captain_id", "gender"])
    .agg([
        pl.col("order_id").n_unique().alias("ride_count"),
        pl.col("called_atleast_once").mean().alias("call_rate"),
        pl.col("calls_capped").mean().alias("avg_calls"),
        pl.col("calls_capped").std().alias("std_calls"),
    ])
)

calls_pivot = (
    calls_stats.pivot(
        values=["ride_count", "call_rate", "avg_calls", "std_calls"],
        index="captain_id", on="gender",
    )
    .filter(
        (pl.col("ride_count_Male") >= MIN_RIDES_PER_GENDER) &
        (pl.col("ride_count_Female") >= MIN_RIDES_PER_GENDER) &
        (pl.col("call_rate_Female") > 0.0)
    )
    .with_columns([
        (pl.col("call_rate_Female") - pl.col("call_rate_Male")).alias("call_rate_diff"),
        (pl.col("avg_calls_Female") - pl.col("avg_calls_Male")).alias("calls_diff"),
        (pl.col("std_calls_Female") / (pl.col("avg_calls_Female") + 1e-6)).alias("cv_f"),
        (pl.col("std_calls_Male")   / (pl.col("avg_calls_Male")   + 1e-6)).alias("cv_m"),
    ])
    .with_columns(
        pl.when(pl.col("call_rate_Female") > 0.10)
        .then(pl.col("cv_m") - pl.col("cv_f"))
        .otherwise(None)
        .alias("cv_diff")
    )
)

t_rate  = max(calls_pivot.select(pl.col("call_rate_diff").quantile(0.98)).item() or 0.0, 0.05)
t_vol   = max(calls_pivot.select(pl.col("calls_diff").quantile(0.98)).item() or 0.0, 0.30)
t_cv    = max(
    calls_pivot.filter(pl.col("cv_diff").is_not_null())
               .select(pl.col("cv_diff").quantile(0.98)).item() or 0.0,
    0.10,
)

ids_calls = set(
    calls_pivot
    .with_columns([
        (pl.col("call_rate_diff") >= t_rate).cast(pl.Int32).alias("f1"),
        (pl.col("calls_diff")     >= t_vol ).cast(pl.Int32).alias("f2"),
        (pl.col("cv_diff").is_not_null() & (pl.col("cv_diff") >= t_cv))
            .cast(pl.Int32).alias("f3"),
    ])
    .with_columns((pl.col("f1") + pl.col("f2") + pl.col("f3")).alias("breaches"))
    .filter(pl.col("breaches") >= 2)
    .get_column("captain_id").to_list()
)

print_summary("SIGNAL 4: CALLS", [
    ("qualifying_captains",     f"{calls_pivot.height:,}"),
    ("risky_captains",          f"{len(ids_calls):,}"),
    ("call_rate_threshold",     f"{t_rate:.4f}"),
    ("calls_diff_threshold",    f"{t_vol:.4f}"),
    ("cv_diff_threshold",       f"{t_cv:.4f}"),
])

# =============================================================================
# SIGNAL 5: OCARA - rider_cancelled asymmetry
# =============================================================================
#
# ## Signal purpose
# This signal checks whether a captain has unusually asymmetric rider-cancelled
# behavior across female and male customers. It is designed to catch selective
# cancellation patterns while requiring enough rides from both genders.
#
# ## Logic in simple language
# 1. Keep only records where ocara_event_type is rider_cancelled.
# 2. Count total rides per captain and gender from the full cohort base.
# 3. Count rider cancellations per captain and gender from the OCARA subset.
# 4. Keep only captains with at least MIN_RIDES_PER_GENDER rides for both
#    Female and Male customers.
# 5. Compute cancellation rates for Female and Male customers.
# 6. Compute diff = Female cancellation rate minus Male cancellation rate.
# 7. Flag captains in either extreme tail: diff >= p98 or diff <= p02.
# 8. Tail guards prevent false removals when p98 is not positive or p02 is not
#    negative in the current batch.
#
# ## Columns utilized
# - ocara_event_type: selects rider_cancelled records.
# - customer_gender: separates Female and Male ride/cancel rates.
# - order_id: ride and cancel counting.
# - captain_id: final captain-level flag.
#
# ## Derived columns
# - rides_f, rides_m, cancels_f, cancels_m, rate_f, rate_m, diff, upper, lower.

# =============================================================================
# SIGNAL 5: OCARA — rider_cancelled asymmetry
# Lazy / memory-safe
# =============================================================================
# SQL does not provide ocara_event_type.
# It provides pre-aggregated captain-level/day-level OCARA counts:
#   female_ocara_count_captainlevel
#   male_ocara_count_captainlevel
#
# So we preserve the same asymmetry logic:
#   female cancel rate - male cancel rate
# but use the available SQL count columns as numerator.
# =============================================================================

rides_by_g_lf = (
    base_cohort
    .with_columns(
        pl.when(pl.col("customer_gender") == 1)
          .then(pl.lit("Female"))
          .otherwise(pl.lit("Male"))
          .alias("gender")
    )
    .group_by(["captain_id", "gender"])
    .agg(
        pl.col("order_id").n_unique().alias("total_rides")
    )
)

rides_by_g = rides_by_g_lf.collect(streaming=True)

rides_pivot = _pivot_genders(
    rides_by_g,
    "total_rides",
    {"Female": "rides_f", "Male": "rides_m"},
    fill=0,
)

# OCARA / CC counts are captain-date level fields repeated across ride rows.
# Safe aggregation:
#   1. max per captain-date to dedupe repeated ride rows
#   2. sum per captain across dates
ocara_counts = (
    base_cohort
    .group_by(["captain_id", "order_date"])
    .agg([
        pl.col("female_ocara_count_captainlevel")
          .fill_null(0)
          .max()
          .alias("daily_cancels_f"),

        pl.col("male_ocara_count_captainlevel")
          .fill_null(0)
          .max()
          .alias("daily_cancels_m"),

        pl.col("female_cc_count_captainlevel")
          .fill_null(0)
          .max()
          .alias("daily_cc_f"),

        pl.col("male_cc_count_captainlevel")
          .fill_null(0)
          .max()
          .alias("daily_cc_m"),
    ])
    .group_by("captain_id")
    .agg([
        pl.col("daily_cancels_f").sum().alias("cancels_f"),
        pl.col("daily_cancels_m").sum().alias("cancels_m"),

        pl.col("daily_cc_f").sum().alias("cc_f"),
        pl.col("daily_cc_m").sum().alias("cc_m"),
    ])
    .collect(streaming=True)
)

ocara_qual = (
    rides_pivot
    .join(ocara_counts, on="captain_id", how="left")
    .with_columns([
        pl.col("cancels_f").fill_null(0),
        pl.col("cancels_m").fill_null(0),

        pl.col("cc_f").fill_null(0),
        pl.col("cc_m").fill_null(0),
    ])
    .with_columns([
        (
            pl.col("rides_f")
            + pl.col("cancels_f")
            + pl.col("cc_f")
        ).alias("accepted_f"),

        (
            pl.col("rides_m")
            + pl.col("cancels_m")
            + pl.col("cc_m")
        ).alias("accepted_m"),
    ])
    .filter(
        (pl.col("accepted_f") >= MIN_RIDES_PER_GENDER) &
        (pl.col("accepted_m") >= MIN_RIDES_PER_GENDER)
    )
    .with_columns([
        (pl.col("cancels_f") / pl.col("accepted_f")).alias("rate_f"),
        (pl.col("cancels_m") / pl.col("accepted_m")).alias("rate_m"),
    ])
    .with_columns(
        (pl.col("rate_f") - pl.col("rate_m")).alias("diff")
    )
)

p98 = (
    ocara_qual.select(pl.col("diff").quantile(0.98)).item()
    if ocara_qual.height
    else 1.0
)

p02 = (
    ocara_qual.select(pl.col("diff").quantile(0.02)).item()
    if ocara_qual.height
    else -1.0
)

upper = p98 if (p98 is not None and p98 > 0) else float("inf")
lower = p02 if (p02 is not None and p02 < 0) else float("-inf")

ocara_risky = ocara_qual.filter(
    (pl.col("diff") >= upper) |
    (pl.col("diff") <= lower)
)

ids_ocara = set(ocara_risky.get_column("captain_id").to_list())

print_summary("SIGNAL 5: OCARA", [
    ("qualifying_captains", f"{ocara_qual.height:,}"),
    ("risky_captains", f"{len(ids_ocara):,}"),
    ("p98_threshold", f"{p98:+.4f}"),
    ("p02_threshold", f"{p02:+.4f}"),
    ("upper_used", "inf (no +ve signal)" if upper == float("inf") else f"{upper:+.4f}"),
    ("lower_used", "-inf (no -ve signal)" if lower == float("-inf") else f"{lower:+.4f}"),
])



# =============================================================================
# UNION OF RISKY -> df_shivam (cohort minus risky)
# =============================================================================
#
# ## Section purpose
# This section combines all five primary risky-captain signals into one master
# removal set, then subtracts that set from the eligible cohort. The remaining
# captains become the first safe seeding candidate list, df_shivam.
#
# ## Logic in simple language
# The five risky sets are unioned so each captain appears once, even if they
# breached multiple signals. master_risky keeps one row per risky captain and
# boolean columns showing exactly which signals they breached. df_shivam keeps
# captains from df_cohort who are not present in the risky union.
#
# ## Columns utilized
# - captain_id: key used to union, audit, and subtract risky captains.
#
# ## Signal sets used
# - ids_review, ids_calls, ids_ocara, ids_ticket, ids_inappchat.

all_risky = ids_review | ids_calls | ids_ocara | ids_ticket | ids_inappchat

master_risky = (
    pl.DataFrame({"captain_id": sorted(all_risky)})
    .with_columns([
        pl.col("captain_id").is_in(list(ids_review)).alias("flag_review"),
        pl.col("captain_id").is_in(list(ids_calls)).alias("flag_calls"),
        pl.col("captain_id").is_in(list(ids_ocara)).alias("flag_ocara"),
        pl.col("captain_id").is_in(list(ids_ticket)).alias("flag_ticket"),
        pl.col("captain_id").is_in(list(ids_inappchat)).alias("flag_inappchat"),
    ])
    .with_columns(
        (pl.col("flag_review").cast(pl.Int32) +
         pl.col("flag_calls").cast(pl.Int32) +
         pl.col("flag_ocara").cast(pl.Int32) +
         pl.col("flag_ticket").cast(pl.Int32) +
         pl.col("flag_inappchat").cast(pl.Int32)).alias("total_signals")
    )
    .sort("total_signals", descending=True)
)

print_summary("ALL RISKY CAPTAINS", [
    ("cohort_captains",      f"{df_cohort.height:,}"),
    ("total_unique_risky",   f"{len(all_risky):,}"),
    ("risky_pct",            f"{safe_ratio(len(all_risky), df_cohort.height) * 100:.2f}%"),
    ("review",               f"{len(ids_review):,}"),
    ("calls",                f"{len(ids_calls):,}"),
    ("ocara",                f"{len(ids_ocara):,}"),
    ("post_ride_ticket",     f"{len(ids_ticket):,}"),
    ("inappchat",            f"{len(ids_inappchat):,}"),
])

df_shivam = df_cohort.filter(~pl.col("captain_id").is_in(list(all_risky)))
df_shivam.write_csv(OUT_SEEDING)

print_summary("SEEDING INTERMEDIATE", [
    ("safe_seed_captains", f"{df_shivam.height:,}"),
    ("output",             OUT_SEEDING),
])

# =============================================================================
# df_new_feedback - built inline from live_feedback_* + rating_screen_*
# =============================================================================
#
# ## Section purpose
# This section applies the newer explicit safety-feedback removals. It is kept
# separate from the five primary signals so the script can report how many
# captains are incremental removals versus already excluded by earlier signals.
#
# ## Logic in simple language
# Live feedback logic:
# - Look at orders where live_feedback_feedback is available.
# - Count unique orders per captain as total_count.
# - Count unique Yes and No responses.
# - Exclude captains when total_count > 1 and at least one No exists.
#
# Rating screen logic:
# - Look at orders where rating_screen_feedback is available.
# - Count unique orders per captain as total_count.
# - Count Yes responses using both accepted positive labels: Yes and
#   Yes, I felt safe!.
# - Count No responses.
# - Compute pct_yes = yes_count / total_count * 100.
# - Exclude captains when total_count > 2 and pct_yes < 95.
#
# The final df_new_feedback is the union of live-feedback and rating-screen
# exclusions.
#
# ## Columns utilized
# - captain_id: captain-level exclusion key.
# - order_id: unique order counts, matching pandas nunique behavior.
# - live_feedback_feedback: live safety feedback value.
# - rating_screen_feedback: rating-screen safety feedback value.
#
# ## Derived columns
# - total_count, yes_count, no_count, pct_yes.

# Live feedback (safety question): exclude captains with any 'No' when total>1
# Counts are nunique(order_id) to mirror pandas .nunique() in user's snippet
# =============================================================================
# df_new_feedback - built inline from live_feedback_* + rating_screen_*
# Female rides only
# =============================================================================

# Live feedback: female rides only
# Exclude captains with any 'No' when total_count > 1
# Counts are nunique(order_id) to mirror pandas .nunique()

lf = (
    df
    .filter(pl.col("customer_gender") == 1)
    .filter(pl.col("live_feedback_feedback").is_not_null())
    .group_by("captain_id")
    .agg([
        pl.col("order_id").n_unique().alias("total_count"),
        pl.col("order_id")
            .filter(pl.col("live_feedback_feedback") == "Yes")
            .n_unique()
            .alias("yes_count"),
        pl.col("order_id")
            .filter(pl.col("live_feedback_feedback") == "No")
            .n_unique()
            .alias("no_count"),
    ])
)

excl_lf = set(
    lf
    .filter((pl.col("total_count") > 1) & (pl.col("no_count") > 0))
    .get_column("captain_id")
    .to_list()
)

# Rating screen: female rides only
# %yes computed across {"Yes", "Yes, I felt safe!", "Yes, I felt safe"}
# Exclude captains where total_count > 2 and pct_yes < 90
# Counts are nunique(order_id) to mirror pandas .nunique()

rs = (
    df
    .filter(pl.col("customer_gender") == 1)
    .filter(pl.col("rating_screen_feedback").is_not_null())
    .group_by("captain_id")
    .agg([
        pl.col("order_id").n_unique().alias("total_count"),
        pl.col("order_id")
            .filter(pl.col("rating_screen_feedback").is_in([
                "Yes",
                "Yes, I felt safe!",
                "Yes, I felt safe"
            ]))
            .n_unique()
            .alias("yes_count"),
        pl.col("order_id")
            .filter(pl.col("rating_screen_feedback") == "No")
            .n_unique()
            .alias("no_count"),
    ])
    .with_columns(
        ((pl.col("yes_count") / pl.col("total_count")) * 100)
        .round(2)
        .alias("pct_yes")
    )
)

excl_rs = set(
    rs
    .filter((pl.col("total_count") > 2) & (pl.col("pct_yes") < 90))
    .get_column("captain_id")
    .to_list()
)

df_new_feedback = pl.DataFrame({"captain_id": sorted(excl_lf | excl_rs)})

print_summary("df_new_feedback (female live_feedback + female rating_screen)", [
    ("live_feedback_excluded", f"{len(excl_lf):,}"),
    ("rating_screen_excluded", f"{len(excl_rs):,}"),
    ("union",                  f"{df_new_feedback.height:,}"),
])

# =============================================================================
# FINAL: df_shivam minus df_new_feedback -> shift filter
# =============================================================================
#
# ## Section purpose
# This section creates the final production output cohort. It starts from the
# five-signal-safe captain set, removes additional explicit safety-feedback
# exclusions, enriches the remaining captains with latest metadata, and applies
# the allowed BLR shift filter.
#
# ## Logic in simple language
# 1. Remove all captains present in df_new_feedback from df_shivam. This mirrors
#    a pandas left-merge with _merge == left_only.
# 2. Report how many df_new_feedback removals were already covered by the five
#    primary signals and how many are incremental.
# 3. Pull latest available service_type, mobile, and shift_name for each
#    remaining captain by sorting by order_date descending and taking first.
# 4. Join metadata back to the candidate list.
# 5. Keep only captains whose shift_name is present in SHIFT_LIST.
# 6. Write the final seeding output with captain_id, mobile, and shift_name.
#
# ## Columns utilized
# - captain_id: join and exclusion key.
# - order_date: determines latest metadata row.
# - service_type: retained in cap_meta for traceability, though final output
#   selects captain_id, mobile, shift_name.
# - mobile: final contact field.
# - shift_name: production shift eligibility filter and final output field.

# Mirrors original pandas pattern: left-merge + filter '_merge == left_only'
# i.e. keep captains in df_shivam that are NOT in df_new_feedback
new_feedback_ids = df_new_feedback.get_column("captain_id").to_list()

# How many of df_new_feedback are *new* removals (not already excluded by the 5 signals)
incremental = set(new_feedback_ids) - all_risky
already_excluded = set(new_feedback_ids) & all_risky

df_nminus1 = df_shivam.filter(~pl.col("captain_id").is_in(new_feedback_ids))
df_nminus1.write_csv(OUT_NMINUS1)

print_summary("df_new_feedback application", [
    ("df_new_feedback_total",       f"{len(new_feedback_ids):,}"),
    ("already_in_5signal_risky",    f"{len(already_excluded):,}"),
    ("incremental_new_removals",    f"{len(incremental):,}"),
    ("df_shivam_before",            f"{df_shivam.height:,}"),
    ("df_nminus1_after",            f"{df_nminus1.height:,}"),
])

# pull shift_name / mobile / service_type from df itself (latest per captain)
cap_meta = (
    df
    .filter(pl.col("captain_id").is_in(df_nminus1.get_column("captain_id").to_list()))
    .sort("order_date", descending=True)
    .group_by("captain_id")
    .agg([
        pl.col("service_type").first(),
        pl.col("mobile").first(),
        pl.col("shift_name").first(),
    ])
)

df_final = (
    df_nminus1.join(cap_meta, on="captain_id", how="inner")
    # .filter(pl.col("shift_name").is_in(SHIFT_LIST))
    .select(["captain_id", "mobile", "shift_name"])
)

df_final.write_csv(OUT_FINAL)

print_summary("FINAL OUTPUTS", [
    ("nminus1_after_new_feedback", f"{df_nminus1.height:,}"),
    ("after_shift_filter",   f"{df_final.height:,}"),
    ("nminus1_csv",          OUT_NMINUS1),
    ("final_csv",            OUT_FINAL),
])

# =============================================================================
# AUDIT LOG - per-captain reason for every removal
# =============================================================================
#
# ## Section purpose
# The audit log makes the pipeline explainable. For every removed captain, it
# records which signal removed them, the exact logic bucket, and compact evidence
# values needed for review or debugging.
#
# ## Logic in simple language
# Each audit part rebuilds the flagged subset for one signal and emits a common
# schema:
# - captain_id: captain removed or filtered.
# - signal: broad signal family.
# - logic: specific removal rule inside that signal.
# - evidence: key metrics that explain why the rule fired.
#
# The audit parts cover in-app chat, support tickets, review labels, calls,
# OCARA asymmetry, new feedback, and shift-list exclusion. They are concatenated
# into one CSV so production reviewers can trace every removal reason.
#
# ## Columns utilized
# This section reuses derived signal tables and evidence columns from earlier
# sections, including captain_id, gender, post_ride_review_agent_response_issue_type, L2
# rate_diff, call diffs, OCARA F_minus_M diff, feedback counts, pct_yes, and
# shift_name.

def _kv(**fields):
    parts = []
    for i, (k, e) in enumerate(fields.items()):
        if i:
            parts.append(pl.lit(", "))
        parts.append(pl.lit(f"{k}="))
        parts.append(e.cast(pl.Utf8).fill_null("NA"))
    return pl.concat_str(parts)

audit_parts = [
    # Signal 1 — chat: rude_behavior
    rude.group_by("captain_id").agg([
        pl.len().alias("n"),
        (pl.col("gender") == "Female").sum().alias("fem"),
    ]).select(
        "captain_id",
        pl.lit("inappchat").alias("signal"),
        pl.lit("rude_behavior").alias("logic"),
        _kv(incidents=pl.col("n"), female=pl.col("fem")).alias("evidence"),
    ),

    # Signal 2 — tickets: P0 / P1 (one row per tier)
    ticket_df.filter(pl.col("support_ticket_agent_response_issue_type").is_in(TICKET_TIERS_REMOVE))
    .group_by(["captain_id", "support_ticket_agent_response_issue_type"])
    .agg(pl.len().alias("n"))
    .select(
        "captain_id",
        pl.lit("ticket").alias("signal"),
        pl.col("support_ticket_agent_response_issue_type").alias("logic"),
        _kv(count=pl.col("n")).alias("evidence"),
    ),

    # Signal 3 — review L0 (any)
    reviews.filter(pl.col("post_ride_review_agent_response_issue_type") == "L0_HIGHEST_THREAT")
    .group_by("captain_id").agg(pl.len().alias("n"))
    .select(
        "captain_id",
        pl.lit("review").alias("signal"),
        pl.lit("L0_any").alias("logic"),
        _kv(count=pl.col("n")).alias("evidence"),
    ),

    # Signal 3 — review L1 (female only)
    reviews.filter(
        (pl.col("post_ride_review_agent_response_issue_type") == "L1_HIGH_CONCERN") &
        (pl.col("gender") == "Female")
    ).group_by("captain_id").agg(pl.len().alias("n"))
    .select(
        "captain_id",
        pl.lit("review").alias("signal"),
        pl.lit("L1_female").alias("logic"),
        _kv(count=pl.col("n")).alias("evidence"),
    ),

    # Signal 3 — review L2 (F-M rate diff ≥ p95)
    l2_analyzable.filter(pl.col("rate_diff") >= p95_l2).select(
        "captain_id",
        pl.lit("review").alias("signal"),
        pl.lit("L2_p95").alias("logic"),
        _kv(rate_diff=pl.col("rate_diff").round(4)).alias("evidence"),
    ),

    # Signal 4 — calls: which thresholds breached
    calls_pivot.with_columns([
        (pl.col("call_rate_diff") >= t_rate).alias("f1"),
        (pl.col("calls_diff") >= t_vol).alias("f2"),
        (pl.col("cv_diff").is_not_null() & (pl.col("cv_diff") >= t_cv)).alias("f3"),
    ]).with_columns(
        (pl.col("f1").cast(pl.Int32) +
         pl.col("f2").cast(pl.Int32) +
         pl.col("f3").cast(pl.Int32)).alias("n_breached")
    ).filter(pl.col("n_breached") >= 2).select(
        "captain_id",
        pl.lit("calls").alias("signal"),
        pl.concat_str([
            pl.when(pl.col("f1")).then(pl.lit("rate+")).otherwise(pl.lit("")),
            pl.when(pl.col("f2")).then(pl.lit("vol+")).otherwise(pl.lit("")),
            pl.when(pl.col("f3")).then(pl.lit("cv+")).otherwise(pl.lit("")),
        ]).str.strip_chars("+").alias("logic"),
        _kv(
            rate_diff=pl.col("call_rate_diff").round(4),
            calls_diff=pl.col("calls_diff").round(4),
            cv_diff=pl.col("cv_diff").round(4),
        ).alias("evidence"),
    ),

    # Signal 5 — ocara: direction of asymmetry
    ocara_qual.filter((pl.col("diff") >= upper) | (pl.col("diff") <= lower)).select(
        "captain_id",
        pl.lit("ocara").alias("signal"),
        pl.when(pl.col("diff") >= upper).then(pl.lit("avoids_female"))
        .otherwise(pl.lit("avoids_male")).alias("logic"),
        _kv(F_minus_M=pl.col("diff").round(4)).alias("evidence"),
    ),

    # df_new_feedback — live feedback "No" present
    lf.filter((pl.col("total_count") > 1) & (pl.col("no_count") > 0)).select(
        "captain_id",
        pl.lit("new_feedback").alias("signal"),
        pl.lit("live_feedback_no").alias("logic"),
        _kv(
            total=pl.col("total_count"),
            yes=pl.col("yes_count"),
            no=pl.col("no_count"),
        ).alias("evidence"),
    ),

    # df_new_feedback — rating screen %yes < 95
    rs.filter((pl.col("total_count") > 2) & (pl.col("pct_yes") < 95)).select(
        "captain_id",
        pl.lit("new_feedback").alias("signal"),
        pl.lit("rating_screen_low_yes").alias("logic"),
        _kv(total=pl.col("total_count"), pct_yes=pl.col("pct_yes")).alias("evidence"),
    ),

    # Shift filter — captains dropped because shift_name not in SHIFT_LIST
    df_nminus1.join(cap_meta, on="captain_id", how="inner")
    .filter(~pl.col("shift_name").is_in(SHIFT_LIST)).select(
        "captain_id",
        pl.lit("shift").alias("signal"),
        pl.lit("shift_not_in_list").alias("logic"),
        _kv(shift_name=pl.col("shift_name")).alias("evidence"),
    ),
]

audit_log = pl.concat(audit_parts)
audit_log.write_csv(OUT_AUDIT)

print_summary("AUDIT LOG", [
    ("rows",            f"{audit_log.height:,}"),
    ("unique_captains", f"{audit_log.get_column('captain_id').n_unique():,}"),
    ("output",          OUT_AUDIT),
])

# preview per signal
with pl.Config(tbl_rows=5, tbl_width_chars=200, fmt_str_lengths=120):
    for sig in audit_log.get_column("signal").unique().sort().to_list():
        sub = audit_log.filter(pl.col("signal") == sig)
        print(f"\n--- {sig} ({sub.height:,} rows) ---")
        print(sub.head(5))

gc.collect()
print("\nPipeline completed successfully.")