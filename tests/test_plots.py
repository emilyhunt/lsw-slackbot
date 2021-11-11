"""Tests for the plots.py submodule."""

import asyncio
from datetime import datetime
from pathlib import Path

from lsw_slackbot import plots


async def test_plot_resource_use(aggregation_level=None, dpi=100):
    """Tests plots.plot_resource_use"""
    await plots.plot_resource_use(Path("test_data"),
                                  Path(f"test_plots/stack_{aggregation_level}.png"),
                                  datetime(2020, 1, 1, 12, 53),
                                  end_time=datetime(2020, 1, 2, 7, 4),
                                  aggregation_level=aggregation_level, dpi=dpi)


async def test_plot_resource_use_all_aggregation_levels(
        levels_to_try=(None, "minute", "hour", "day", "week", "month", "year"), dpi=100):
    """Runs test_plot_resource_use at every available aggregation level."""
    for a_level in levels_to_try:
        print(f"Plotting level {a_level}")
        await test_plot_resource_use(a_level, dpi=dpi)


if __name__ == "__main__":
    asyncio.run(test_plot_resource_use_all_aggregation_levels(dpi=300))
