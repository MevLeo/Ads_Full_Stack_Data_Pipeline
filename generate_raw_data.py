"""
AdFlow Media — Raw Data Generator
==================================
Generates the three Bronze CSV files for the AdFlow Media pipeline:
  - google_ads_raw.csv   : Filtered directly from the Kaggle dataset
  - youtube_ads_raw.csv  : Filtered from Kaggle + enriched with YouTube-specific metrics
  - bing_ads_raw.csv     : Fully simulated from Google Ads base with Bing-realistic multipliers

Usage:
    python generate_raw_data.py --input marketing_campaign_dataset.csv --output ./data

Requirements:
    pip install pandas numpy

Data source:
    https://www.kaggle.com/datasets/manishabhatt22/marketing-campaign-performance-dataset
"""

import argparse
import os
import numpy as np
import pandas as pd


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

RANDOM_SEED = 42

# Campaign types that don't belong in paid search/display platforms
INVALID_CAMPAIGN_TYPES = ["Email", "Influencer"]

# YouTube view rate range (real-world benchmark: 25–35%)
YOUTUBE_VIEW_RATE_MIN = 0.25
YOUTUBE_VIEW_RATE_MAX = 0.35

# YouTube conversion boost over Google (engineers the lower CPA narrative)
YOUTUBE_CONVERSION_BOOST_MIN = 1.15
YOUTUBE_CONVERSION_BOOST_MAX = 1.25

# YouTube video ad type distribution
YOUTUBE_AD_TYPES = ["skippable_in_stream", "non_skippable", "bumper"]
YOUTUBE_AD_TYPE_PROBS = [0.60, 0.25, 0.15]

# Bing volume multipliers relative to Google (~35% of Google market share)
BING_IMPRESSION_MULTIPLIER_MIN = 0.30
BING_IMPRESSION_MULTIPLIER_MAX = 0.40
BING_CLICK_MULTIPLIER_MIN = 0.30
BING_CLICK_MULTIPLIER_MAX = 0.40
BING_SPEND_MULTIPLIER_MIN = 0.28
BING_SPEND_MULTIPLIER_MAX = 0.38

# Bing conversion rate slightly lower than Google
BING_CONVERSION_RATE_MULTIPLIER_MIN = 0.85
BING_CONVERSION_RATE_MULTIPLIER_MAX = 0.95

# Bing match type distribution
BING_MATCH_TYPES = ["broad", "phrase", "exact"]
BING_MATCH_TYPE_PROBS = [0.40, 0.35, 0.25]


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def parse_spend(series: pd.Series) -> pd.Series:
    """Parse spend strings like '$16,174.00' into floats."""
    return series.str.replace(r"[$,]", "", regex=True).astype(float)


def safe_divide(numerator: pd.Series, denominator: pd.Series, decimals: int = 4) -> pd.Series:
    """Divide two series safely, returning NULL on division by zero."""
    return (numerator / denominator.replace(0, np.nan)).round(decimals)


# ─────────────────────────────────────────────
# GOOGLE ADS
# ─────────────────────────────────────────────

def build_google_ads(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter Google Ads rows from the Kaggle dataset and clean them.

    Steps:
    - Filter Channel_Used == 'Google Ads'
    - Remove invalid campaign types (Email, Influencer)
    - Parse spend string to float
    - Derive conversions, ctr, cpc
    - Rename columns to snake_case
    """
    google = df[df["Channel_Used"] == "Google Ads"].copy()

    google["platform"] = "google_ads"
    google["spend_usd"] = parse_spend(google["Acquisition_Cost"])
    google["conversions"] = (google["Clicks"] * google["Conversion_Rate"]).round().astype(int)

    google = google.rename(columns={
        "Campaign_ID": "campaign_id",
        "Company": "company",
        "Campaign_Type": "campaign_type",
        "Target_Audience": "target_audience",
        "Duration": "duration",
        "Channel_Used": "channel",
        "Conversion_Rate": "conversion_rate",
        "ROI": "roi",
        "Location": "location",
        "Language": "language",
        "Clicks": "clicks",
        "Impressions": "impressions",
        "Engagement_Score": "engagement_score",
        "Customer_Segment": "customer_segment",
        "Date": "date",
    })

    # Filter invalid campaign types
    google = google[~google["campaign_type"].isin(INVALID_CAMPAIGN_TYPES)]

    # Derived metrics
    google["ctr"] = safe_divide(google["clicks"], google["impressions"])
    google["cpc"] = safe_divide(google["spend_usd"], google["clicks"], decimals=2)

    return google[[
        "campaign_id", "company", "platform", "campaign_type", "target_audience",
        "duration", "channel", "clicks", "impressions", "spend_usd", "conversions",
        "conversion_rate", "ctr", "cpc", "roi", "engagement_score",
        "location", "language", "customer_segment", "date",
    ]]


# ─────────────────────────────────────────────
# YOUTUBE ADS
# ─────────────────────────────────────────────

def build_youtube_ads(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Filter YouTube rows from the Kaggle dataset and enrich with
    YouTube-specific metrics that don't exist in the source data.

    YouTube-specific enrichment:
    - view_rate    : simulated at 25–35% (real-world benchmark)
    - views        : impressions × view_rate
    - cpv          : spend_usd / views (cost per view)
    - video_ad_type: skippable_in_stream / non_skippable / bumper
    - conversions  : boosted 15–25% over base to engineer lower CPA narrative
    """
    youtube = df[df["Channel_Used"] == "YouTube"].copy()

    youtube["platform"] = "youtube_ads"
    youtube["spend_usd"] = parse_spend(youtube["Acquisition_Cost"])

    n = len(youtube)

    # Simulate YouTube-specific fields
    youtube["view_rate"] = rng.uniform(YOUTUBE_VIEW_RATE_MIN, YOUTUBE_VIEW_RATE_MAX, n).round(3)
    youtube["views"] = (youtube["Impressions"] * youtube["view_rate"]).round().astype(int)
    youtube["cpv"] = safe_divide(youtube["spend_usd"], youtube["views"])
    youtube["video_ad_type"] = rng.choice(YOUTUBE_AD_TYPES, n, p=YOUTUBE_AD_TYPE_PROBS)

    # Boost conversions to engineer lower CPA than Google
    boost = rng.uniform(YOUTUBE_CONVERSION_BOOST_MIN, YOUTUBE_CONVERSION_BOOST_MAX, n)
    youtube["conversions"] = (youtube["Clicks"] * youtube["Conversion_Rate"] * boost).round().astype(int)

    youtube = youtube.rename(columns={
        "Campaign_ID": "campaign_id",
        "Company": "company",
        "Campaign_Type": "campaign_type",
        "Target_Audience": "target_audience",
        "Duration": "duration",
        "Channel_Used": "channel",
        "Conversion_Rate": "conversion_rate",
        "ROI": "roi",
        "Location": "location",
        "Language": "language",
        "Clicks": "clicks",
        "Impressions": "impressions",
        "Engagement_Score": "engagement_score",
        "Customer_Segment": "customer_segment",
        "Date": "date",
    })

    # Filter invalid campaign types
    youtube = youtube[~youtube["campaign_type"].isin(INVALID_CAMPAIGN_TYPES)]

    # Derived metrics
    youtube["ctr"] = safe_divide(youtube["clicks"], youtube["impressions"])
    youtube["cpc"] = safe_divide(youtube["spend_usd"], youtube["clicks"], decimals=2)

    return youtube[[
        "campaign_id", "company", "platform", "campaign_type", "target_audience",
        "duration", "channel", "clicks", "impressions", "views", "view_rate", "cpv",
        "spend_usd", "conversions", "conversion_rate", "ctr", "cpc", "roi",
        "engagement_score", "video_ad_type", "location", "language", "customer_segment", "date",
    ]]


# ─────────────────────────────────────────────
# BING ADS
# ─────────────────────────────────────────────

def build_bing_ads(google: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Simulate Bing Ads data using Google Ads as the structural base.

    Bing Ads has no usable public dataset — simulation uses realistic
    platform multipliers reflecting Bing's real-world market position:
    - ~35% of Google's impression and click volume
    - Slightly higher CPC than Google
    - Slightly lower conversion rate than Google
    - Bing-specific match_type field added (broad / phrase / exact)
    """
    bing = google.copy()
    n = len(bing)

    bing["platform"] = "bing_ads"
    bing["channel"] = "Bing Ads"

    # Apply Bing-realistic volume multipliers
    bing["impressions"] = (
        bing["impressions"] * rng.uniform(BING_IMPRESSION_MULTIPLIER_MIN, BING_IMPRESSION_MULTIPLIER_MAX, n)
    ).round().astype(int)

    bing["clicks"] = (
        bing["clicks"] * rng.uniform(BING_CLICK_MULTIPLIER_MIN, BING_CLICK_MULTIPLIER_MAX, n)
    ).round().astype(int)

    bing["spend_usd"] = (
        bing["spend_usd"] * rng.uniform(BING_SPEND_MULTIPLIER_MIN, BING_SPEND_MULTIPLIER_MAX, n)
    ).round(2)

    # Slightly lower conversions than Google
    conv_multiplier = rng.uniform(BING_CONVERSION_RATE_MULTIPLIER_MIN, BING_CONVERSION_RATE_MULTIPLIER_MAX, n)
    bing["conversions"] = (bing["clicks"] * bing["conversion_rate"] * conv_multiplier).round().astype(int)

    # Bing-specific keyword match type
    bing["match_type"] = rng.choice(BING_MATCH_TYPES, n, p=BING_MATCH_TYPE_PROBS)

    # Recompute derived metrics based on new Bing values
    bing["ctr"] = safe_divide(bing["clicks"], bing["impressions"])
    bing["cpc"] = safe_divide(bing["spend_usd"], bing["clicks"], decimals=2)

    return bing[[
        "campaign_id", "company", "platform", "campaign_type", "target_audience",
        "duration", "channel", "clicks", "impressions", "spend_usd", "conversions",
        "conversion_rate", "ctr", "cpc", "roi", "engagement_score", "match_type",
        "location", "language", "customer_segment", "date",
    ]]


# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────

def print_summary(google: pd.DataFrame, youtube: pd.DataFrame, bing: pd.DataFrame) -> None:
    """Print a summary of the generated datasets and validate the CPA narrative."""

    g_cpa = google["spend_usd"].sum() / google["conversions"].sum()
    y_cpa = youtube["spend_usd"].sum() / youtube["conversions"].sum()
    b_cpa = bing["spend_usd"].sum() / bing["conversions"].sum()

    print("\n" + "=" * 55)
    print("AdFlow Media — Data Generation Summary")
    print("=" * 55)

    print(f"\nGOOGLE ADS")
    print(f"  Rows     : {len(google):,}")
    print(f"  Columns  : {len(google.columns)}")
    print(f"  CPA      : ${g_cpa:,.2f}")

    print(f"\nYOUTUBE ADS")
    print(f"  Rows     : {len(youtube):,}")
    print(f"  Columns  : {len(youtube.columns)}")
    print(f"  Avg CPV  : ${youtube['cpv'].mean():.4f}")
    print(f"  View rate: {youtube['view_rate'].mean():.1%}")
    print(f"  CPA      : ${y_cpa:,.2f}  ← should be lowest")

    print(f"\nBING ADS")
    print(f"  Rows     : {len(bing):,}")
    print(f"  Columns  : {len(bing.columns)}")
    print(f"  Volume vs Google: {bing['impressions'].mean() / google['impressions'].mean():.1%}")
    print(f"  CPA      : ${b_cpa:,.2f}  ← should be highest")

    print(f"\nNARRATIVE CHECK")
    narrative_ok = y_cpa < g_cpa < b_cpa
    status = "PASS" if narrative_ok else "FAIL"
    print(f"  YouTube CPA < Google CPA < Bing CPA : {status}")
    print(f"  YouTube ${y_cpa:.0f} < Google ${g_cpa:.0f} < Bing ${b_cpa:.0f}")

    total = len(google) + len(youtube) + len(bing)
    print(f"\nTOTAL ROWS (all platforms) : {total:,}")
    print("=" * 55 + "\n")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate AdFlow Media Bronze CSV files from Kaggle source data."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the Kaggle CSV file (marketing_campaign_dataset.csv)",
    )
    parser.add_argument(
        "--output",
        default="./data",
        help="Output directory for the three CSV files (default: ./data)",
    )
    args = parser.parse_args()

    # Validate input
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file not found: {args.input}")

    # Create output directory
    os.makedirs(args.output, exist_ok=True)

    print(f"Reading source data from: {args.input}")
    df = pd.read_csv(args.input)
    print(f"Loaded {len(df):,} rows from Kaggle dataset.")
    print(f"Channels found: {df['Channel_Used'].unique().tolist()}")

    # Seed for reproducibility
    rng = np.random.default_rng(RANDOM_SEED)

    # Build all three platform datasets
    print("\nGenerating Google Ads data...")
    google = build_google_ads(df)

    print("Generating YouTube Ads data...")
    youtube = build_youtube_ads(df, rng)

    print("Generating Bing Ads data...")
    bing = build_bing_ads(google, rng)

    # Save to CSV
    google_path = os.path.join(args.output, "google_ads_raw.csv")
    youtube_path = os.path.join(args.output, "youtube_ads_raw.csv")
    bing_path = os.path.join(args.output, "bing_ads_raw.csv")

    google.to_csv(google_path, index=False)
    youtube.to_csv(youtube_path, index=False)
    bing.to_csv(bing_path, index=False)

    print(f"\nFiles saved to: {args.output}/")
    print(f"  google_ads_raw.csv")
    print(f"  youtube_ads_raw.csv")
    print(f"  bing_ads_raw.csv")

    # Print summary and narrative validation
    print_summary(google, youtube, bing)


if __name__ == "__main__":
    main()
