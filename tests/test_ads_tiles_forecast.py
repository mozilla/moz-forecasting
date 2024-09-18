"""Testing for ad_tiles_forecast."""

import pandas as pd
import pytest

from moz_forecasting.ad_tiles_forecast.flow import get_direct_allocation_df


def test_direct_allocation_exception():
    """Test get_direct_allocation_df when allocation goes about 100%."""
    config_dict = [
        {"markets": ["US"], "allocation": 0.5},
        {"markets": ["US"], "allocation": 0.6},
    ]

    with pytest.raises(
        ValueError, match="More than 100% of inventory allocated for direct sales"
    ):
        get_direct_allocation_df(
            config_dict,
            min_month=pd.to_datetime("2024-01"),
            max_month=pd.to_datetime("2025-01"),
        )


def test_direct_allocation():
    """Test get_direct_allocation_df with normal inputs."""
    config_dict = [
        {"markets": ["US", "DE"], "allocation": 0.2},
        {"markets": ["US"], "allocation": 0.3, "start_month": "2024-02"},
        {"markets": ["DE"], "allocation": 0.4, "end_month": "2024-03"},
        {
            "markets": ["DE", "US"],
            "allocation": 0.1,
            "start_month": "2024-04",
            "end_month": "2024-04",
        },
    ]

    output_df = get_direct_allocation_df(
        config_dict,
        min_month=pd.to_datetime("2024-01"),
        max_month=pd.to_datetime("2024-04"),
    )

    expected_df = pd.DataFrame(
        [
            {
                "submission_month": pd.to_datetime("2024-01"),
                "country": "DE",
                "direct_sales_allocations": 0.6,
            },
            {
                "submission_month": pd.to_datetime("2024-02"),
                "country": "DE",
                "direct_sales_allocations": 0.6,
            },
            {
                "submission_month": pd.to_datetime("2024-03"),
                "country": "DE",
                "direct_sales_allocations": 0.6,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "DE",
                "direct_sales_allocations": 0.3,
            },
            {
                "submission_month": pd.to_datetime("2024-01"),
                "country": "US",
                "direct_sales_allocations": 0.2,
            },
            {
                "submission_month": pd.to_datetime("2024-02"),
                "country": "US",
                "direct_sales_allocations": 0.5,
            },
            {
                "submission_month": pd.to_datetime("2024-03"),
                "country": "US",
                "direct_sales_allocations": 0.5,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "US",
                "direct_sales_allocations": 0.6,
            },
        ]
    )

    assert set(expected_df.columns) == set(output_df.columns)
    pd.testing.assert_frame_equal(
        output_df.sort_values(["country", "submission_month"]).reset_index(drop=True),
        expected_df[output_df.columns],
    )
