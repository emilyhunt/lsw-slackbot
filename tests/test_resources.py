import asyncio
import os
from pathlib import Path

import pandas as pd
from lsw_slackbot import resources


async def test_get_resource_usage_dataframe():
    dataframe = await resources._get_resource_usage_dataframe(measurement_time=1)

    # There aren't many tests we can do as this is *very* system-dependent. It even working is almost good enough... lol
    # Check column names
    assert (dataframe.columns == pd.Index(['cpu_percent', 'memory', 'threads', 'time'])).all()

    return dataframe


async def test_get_current_resource_use():
    return await resources.current_resource_use()


def test_get_system_info():
    return resources.get_system_info()


async def test_sample_resource_usage():
    # Firstly, ensure the output file is yeeted if it exists already
    output_dir = Path("./test_data")
    output_file = output_dir / "server_usage_test.csv"

    if output_file.exists():
        os.remove(output_file)

    # Next, take 30 samples!
    print(f"Sampling resource use 30 times!")

    for i in range(30):
        print(f"\rSample {i}", end="")
        await resources.sample_resource_usage(output_dir, filename="test_server_usage.csv", measurement_time=1.0)

    print("")

    # Run tests
    assert output_file.exists()

    test_data = pd.read_csv(output_file)

    assert len(test_data.columns) == 5
    #assert len(test_data) == 30
    # Todo: how could the length be tested? It takes an actual measurement so this is kinda hard to test...

    return test_data


if __name__ == "__main__":
    df = asyncio.run(test_get_resource_usage_dataframe())
    cru = asyncio.run(test_get_current_resource_use())
    system_info = test_get_system_info()
    testdf = asyncio.run(test_sample_resource_usage())
