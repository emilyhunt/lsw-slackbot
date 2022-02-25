"""Functions for getting current server resource use."""
from typing import Optional, Union

import psutil
import pandas as pd
import logging
import asyncio
import datetime
import numpy as np
import platform
import socket
import subprocess

from pathlib import Path


async def sample_resource_usage(data_dir: Path, filename: Optional[Union[str, Path]] = None,
                                measurement_time: Union[int, float] = 10, measurement_cycles: int = 1,
                                inter_measurement_time: Union[int, float] = 0):
    """Samples resource usage and saves it to the data directory."""
    logging.debug("generating a resource usage dataframe")

    # Firstly, let's do a number of measurement cycles
    dataframe = []
    for i in range(measurement_cycles):
        dataframe.append(await _get_resource_usage_dataframe(measurement_time=measurement_time, add_a_time=False))
        await asyncio.sleep(inter_measurement_time)

    # Now we can combine the multiple measurements...
    dataframe = pd.concat(dataframe, ignore_index=True)
    dataframe = (dataframe.groupby("username")
                 .agg({"cpu_percent": "mean", "memory": "mean", "threads": "mean"})
                 .reset_index())

    dataframe['time'] = datetime.datetime.now()

    # ... and save it!
    if filename is None:
        filename = data_dir / datetime.datetime.now().strftime("%Y-%m-%d_server_usage.csv")
    else:
        filename = data_dir / Path(filename)

    # Work out if it exists already - this would mean we only want to append to the existing file and without
    # adding new header names
    if filename.exists():
        mode = "a"
        header = False
    else:
        mode = "w"
        header = True

    # Save it!
    data_dir.mkdir(exist_ok=True, parents=True)  # Ensures that the directory exists
    dataframe.to_csv(filename, header=header, mode=mode, index=True)
    logging.debug("resource usage dataframe successfully saved")


async def _get_resource_usage_dataframe(groupby_username: bool = True, measurement_time: Union[int, float] = 10,
                                        add_a_time=True):
    """Generates a full resource usage dataframe with usage grouped by user."""
    # Loop over all current processes
    data_dict = {}
    processes = list(psutil.process_iter())

    # We call cpu_percent initially with zero time. The eventual measurement will be between this point and the next,
    # but in a non-blocking way =)
    for a_process in processes:
        try:
            a_process.cpu_percent()

        # Catch typical errors. The process may not exist anymore or may be a system process that we aren't allowed to
        # query unless the app is running as root.
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            pass

    await asyncio.sleep(measurement_time)

    # Now, we can loop for real!
    n_cores = psutil.cpu_count(logical=False)
    for i, a_process in enumerate(psutil.process_iter()):

        try:
            data_dict[i] = {
                "username": a_process.username(),
                "cpu_percent": a_process.cpu_percent() / n_cores,
                "memory": a_process.memory_full_info().pss / 1024**3,  # Proportional set size converted to GB - see [1]
                "threads": 1,
            }

            # [1] - see this for why PSS is a better measure of memory use in multiprocessing contexts:
            # https://gmpy.dev/blog/2016/real-process-memory-and-environ-in-python

        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            pass

    dataframe = pd.DataFrame.from_dict(data_dict, orient="index")

    if groupby_username:
        dataframe = dataframe.groupby("username").agg({"cpu_percent": "sum", "memory": "sum", "threads": "sum"})

    if add_a_time:
        dataframe['time'] = datetime.datetime.now()
    return dataframe


def current_memory_fraction():
    """Quick function to get a basic fraction of memory being used."""
    mem_use = psutil.virtual_memory()

    return mem_use.used / mem_use.total


async def current_resource_use(measurement_time: Union[int, float] = 0.5):
    """Returns a quick summary of current server use - a dict with various stats."""
    logging.debug("taking intermittent resource use measurement")

    # Get CPU use - we briefly sleep to get a better quality measurement
    psutil.cpu_percent(percpu=True)
    await asyncio.sleep(measurement_time)
    cpu_use = np.asarray(psutil.cpu_percent(percpu=True))

    # Memory use
    mem_use = psutil.virtual_memory()

    # Make and return a nice dict!
    return {
        "cpu_percent": np.sum(cpu_use) / len(cpu_use),
        "cores_with_<1%_use": np.count_nonzero(cpu_use < 1.0),
        "cores_with_<25%_use": np.count_nonzero(cpu_use < 25.0),
        "cores_with_<50%_use": np.count_nonzero(cpu_use < 50.0),
        "total_cores": len(cpu_use),
        "memory_used": mem_use.used / 1024**3,
        "memory_available": mem_use.available / 1024**3,
        "memory_total": mem_use.total / 1024**3
    }


def _get_cpu_info(required_keys=None):
    """Get CPU info on Linux as a dict (actually hilariously difficult)"""
    # Get a list where each entry is a property we care about
    cpu_info = subprocess.check_output("lscpu", shell=True).strip().decode().split("\n")

    cpu_info_dict = {}
    for a_line in cpu_info:
        split_values = a_line.split(":")

        if len(split_values) == 2:
            key, value = split_values
            cpu_info_dict[key] = value.strip()
        elif len(split_values) > 2:
            key = split_values[0]
            value = ":".join(split_values[1:])
            cpu_info_dict[key] = value.strip()

    # We can also add certain keys to the dict to make sure they aren't missing
    if required_keys is not None:
        for a_key in required_keys:
            if a_key not in cpu_info_dict:
                cpu_info_dict[a_key] = "This information not returned by lscpu!"

    return cpu_info_dict


def get_system_info():
    """Returns a basic string of system information."""
    cpu_info = _get_cpu_info(required_keys=('Model name', 'CPU(s)', 'Thread(s) per core'))

    return (f"-- SYSTEM INFO --\n"
            f"hostname: {socket.gethostname()}\n"
            f"platform: {platform.system()}\n"
            f"platform-release: {platform.release()}\n"
            f"platform-version: {platform.version()}\n"
            f"architecture: {platform.architecture()}\n"
            f"cpu-model: {cpu_info['Model name']}\n"
            f"cpu-cores: {cpu_info['CPU(s)']} - {cpu_info['Thread(s) per core']} thread(s) per core\n"
            f"total-ram: {psutil.virtual_memory().total / 1024**3:.1f} GB")
