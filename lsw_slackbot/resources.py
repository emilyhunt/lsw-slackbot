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


async def save_resource_usage_dataframe(data_dir: Path, filename: Optional[Union[str, Path]] = None):
    """Saves resource usage dataframe to the data directory."""
    logging.debug("generating a resource usage dataframe")

    # Firstly, let's get it
    dataframe = await get_resource_usage_dataframe()

    # ... and save it!
    if filename is None:
        filename = data_dir / datetime.datetime.now().strftime("%Y-%m-%d_server_usage.csv")
    else:
        filename = Path(filename)

    # Work out if it exists already - this would mean we only want to append to the existing file and without
    # adding new header names
    if filename.exists():
        mode = "a"
        header = False
    else:
        mode = "w"
        header = True

    # Save it!
    dataframe.to_csv(filename, header=header, mode=mode)
    logging.debug("resource usage dataframe successfully saved")


async def get_resource_usage_dataframe(groupby_username: bool = True, measurement_time: int = 10):
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
    n_cores = psutil.cpu_count()
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
        dataframe = dataframe.groupby("username").agg({"cpu_percent": "sum", "memory": "sum", "threads": sum})

    dataframe['time'] = datetime.datetime.now()
    return dataframe


async def current_resource_use(measurement_time=0.5):
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
        "free_cores": np.count_nonzero(cpu_use < 1.0),
        "total_cores": len(cpu_use),
        "memory_used": mem_use.used / 1024**3,
        "memory_available": mem_use.available / 1024**3,
        "memory_total": mem_use.total / 1024**3
    }


def _get_cpu_info():
    """Get CPU info on Linux as a dict (actually hilariously difficult)"""
    # Get a list where each entry is a property we care about
    cpu_info = subprocess.check_output("lscpu", shell=True).strip().decode().split("\n")

    cpu_info_dict = {}
    for a_line in cpu_info:
        key, value = a_line.split(":")
        cpu_info_dict[key] = value.strip()

    return cpu_info_dict


def get_system_info():
    """Returns a basic string of system information."""
    cpu_info = _get_cpu_info()

    return (f"-- SYSTEM INFO --\n"
            f"hostname: {socket.gethostname()}\n"
            f"platform: {platform.system()}\n"
            f"platform-release: {platform.release()}\n"
            f"platform-version: {platform.version()}\n"
            f"architecture: {platform.architecture()}\n"
            f"cpu-model: {cpu_info['Model name']}\n"
            f"cpu-cores: {cpu_info['CPU(s)']} - {cpu_info['Thread(s) per core']} thread(s) per core\n"
            f"total-ram: {psutil.virtual_memory().total / 1024**3:.1f} GB")
