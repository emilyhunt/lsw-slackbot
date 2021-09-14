import asyncio
import pandas as pd
from lsw_slackbot import resources


async def test_get_resource_usage_dataframe():
    dataframe = await resources.get_resource_usage_dataframe(measurement_time=1)

    # There aren't many tests we can do as this is *very* system-dependent. It even working is almost good enough... lol
    # Check column names
    assert (dataframe.columns == pd.Index(['cpu_percent', 'memory', 'threads', 'time'])).all()

    return dataframe


async def test_get_current_resource_use():
    return await resources.current_resource_use()


def test_get_system_info():
    return resources.get_system_info()


if __name__ == "__main__":
    df = asyncio.run(test_get_resource_usage_dataframe())
    cru = asyncio.run(test_get_current_resource_use())
    system_info = test_get_system_info()
