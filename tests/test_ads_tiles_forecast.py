"""Testing for ad_tiles_forecast."""

import pandas as pd
import pytest

from moz_forecasting.ad_tiles_forecast.flow import (
    get_direct_allocation_df,
)


def test_direct_allocation_exception():
    """Test get_direct_allocation_df when allocation goes about 100%."""
    config_dict = [
        {"markets": {"US": 0.7}, "allocation": 0.5, "positions": [1]},
        {"markets": {"US": 0.3}, "allocation": 0.6, "positions": [1]},
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
        {"markets": {"US": 1.0, "DE": 0.5}, "allocation": 0.2, "positions": [1, 2]},
        {
            "markets": {"US": 0.7},
            "allocation": 0.3,
            "start_month": "2024-02",
            "positions": [2],
        },
        {
            "markets": {"DE": 0.7},
            "allocation": 0.4,
            "end_month": "2024-03",
            "positions": [1],
        },
        {
            "markets": {"DE": 0.1, "US": 0.1},
            "allocation": 0.1,
            "start_month": "2024-04",
            "end_month": "2024-04",
            "positions": [3],
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
                "CPM": (0.2 * 0.5 + 0.4 * 0.7) / 0.6,
                "position": 1,
            },
            {
                "submission_month": pd.to_datetime("2024-01"),
                "country": "DE",
                "direct_sales_allocations": 0.2,
                "CPM": 0.5,
                "position": 2,
            },
            {
                "submission_month": pd.to_datetime("2024-02"),
                "country": "DE",
                "direct_sales_allocations": 0.6,
                "CPM": (0.2 * 0.5 + 0.4 * 0.7) / 0.6,
                "position": 1,
            },
            {
                "submission_month": pd.to_datetime("2024-02"),
                "country": "DE",
                "direct_sales_allocations": 0.2,
                "CPM": 0.5,
                "position": 2,
            },
            {
                "submission_month": pd.to_datetime("2024-03"),
                "country": "DE",
                "direct_sales_allocations": 0.6,
                "CPM": (0.2 * 0.5 + 0.4 * 0.7) / 0.6,
                "position": 1,
            },
            {
                "submission_month": pd.to_datetime("2024-03"),
                "country": "DE",
                "direct_sales_allocations": 0.2,
                "CPM": 0.5,
                "position": 2,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "DE",
                "direct_sales_allocations": 0.2,
                "CPM": 0.5,
                "position": 1,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "DE",
                "direct_sales_allocations": 0.2,
                "CPM": 0.5,
                "position": 2,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "DE",
                "direct_sales_allocations": 0.1,
                "CPM": 0.1,
                "position": 3,
            },
            {
                "submission_month": pd.to_datetime("2024-01"),
                "country": "US",
                "direct_sales_allocations": 0.2,
                "position": 1,
                "CPM": 1.0,
            },
            {
                "submission_month": pd.to_datetime("2024-01"),
                "country": "US",
                "direct_sales_allocations": 0.2,
                "position": 2,
                "CPM": 1.0,
            },
            {
                "submission_month": pd.to_datetime("2024-02"),
                "country": "US",
                "direct_sales_allocations": 0.2,
                "position": 1,
                "CPM": 1.0,
            },
            {
                "submission_month": pd.to_datetime("2024-02"),
                "country": "US",
                "direct_sales_allocations": 0.5,
                "position": 2,
                "CPM": (0.3 * 0.7 + 1.0 * 0.2) / 0.5,
            },
            {
                "submission_month": pd.to_datetime("2024-03"),
                "country": "US",
                "direct_sales_allocations": 0.2,
                "position": 1,
                "CPM": 1.0,
            },
            {
                "submission_month": pd.to_datetime("2024-03"),
                "country": "US",
                "direct_sales_allocations": 0.5,
                "position": 2,
                "CPM": (0.3 * 0.7 + 1.0 * 0.2) / 0.5,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "US",
                "direct_sales_allocations": 0.2,
                "position": 1,
                "CPM": 1.0,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "US",
                "direct_sales_allocations": 0.5,
                "position": 2,
                "CPM": (0.3 * 0.7 + 1.0 * 0.2) / 0.5,
            },
            {
                "submission_month": pd.to_datetime("2024-04"),
                "country": "US",
                "direct_sales_allocations": 0.1,
                "CPM": 0.1,
                "position": 3,
            },
        ]
    )

    assert set(expected_df.columns) == set(output_df.columns)
    pd.testing.assert_frame_equal(
        output_df.sort_values(["country", "submission_month"]).reset_index(drop=True),
        expected_df[output_df.columns],
    )
