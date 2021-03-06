"""Functions for plotting system resource use."""
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import gridspec


async def _read_resource_files(data_location: Path, start_time: datetime, end_time: Optional[datetime] = None,
                               processes_to_treat_as_root: Optional[Union[tuple, list]] = None):
    """Function for automatically checking the data location and reading in requisite files."""
    if end_time is not None:
        if end_time <= start_time:
            raise ValueError("end time may not be equal to or sooner than the start time!")

    all_files = np.sort(list(data_location.glob("*_server_usage.csv")))

    if len(all_files) == 0:
        raise FileNotFoundError("no valid files to read were found!")

    # Convert the times in the filenames into datetime objects
    time_strings = [x.stem.rsplit("_")[0] for x in all_files]
    time_objects = np.asarray([datetime.strptime(x, "%Y-%m-%d") for x in time_strings])

    # Get midnight on the start time (+ end time) and see which files are valid
    start_time_midnight = datetime(start_time.year, start_time.month, start_time.day)
    valid_files = time_objects >= start_time_midnight

    if end_time is not None:
        end_time_midnight = datetime(end_time.year, end_time.month, end_time.day) + timedelta(days=1)
        valid_files = np.logical_and(valid_files, time_objects <= end_time_midnight)

    # Read in all valid files
    dataframe = []
    for a_file in np.asarray(all_files)[valid_files]:
        a_dataframe = pd.read_csv(a_file)
        dataframe.append(a_dataframe)

    dataframe = pd.concat(dataframe, ignore_index=True)
    dataframe['time'] = pd.to_datetime(dataframe['time'])

    # Restrict measurements to just being the ones absolutely within the time range
    valid_measurements = dataframe['time'] >= start_time
    if end_time is not None:
        valid_measurements = np.logical_and(dataframe['time'] <= end_time, valid_measurements)
    dataframe = dataframe.loc[valid_measurements].reset_index(drop=True)

    # Fold some processes as being root processes if necessary
    if processes_to_treat_as_root is not None:
        to_replace = np.isin(dataframe['username'], processes_to_treat_as_root)
        dataframe['username'] = np.where(to_replace, "root", dataframe['username'])

        # Re-combine all of the root processes that are with the same username & time
        dataframe = (dataframe
                     .groupby(["username", "time"])
                     .agg({"cpu_percent": "sum", "memory": "sum", "threads": sum})
                     .reset_index())

    return dataframe


async def plot_resource_use(data_location: Path, output_location: Path,
                            start_time: Union[datetime, timedelta], end_time: Optional[datetime] = None,
                            aggregation_level: Optional[str] = None,
                            default_tick_format_string: str = "%Y.%m.%d %H:%M",
                            tick_format_string_overwrite: Optional[str] = None, dpi=300,
                            processes_to_treat_as_root: Optional[Union[tuple, list]] = None,
                            minimum_resources_to_plot: Union[tuple, list] = (0.0, 0.0),
                            memory_aggregation_mode="mean", cpu_aggregation_mode="mean"):
    """Function for plotting resource usage in a certain timeframe and dumping this information to a file."""
    # Process start_time if it's a timedelta
    if isinstance(start_time, timedelta):
        start_time = datetime.now() - start_time

    logging.debug(f"  plot is within range\n  start time: {start_time}\n  end time: {end_time}")

    # Read in the data
    logging.debug("fetching files")
    dataframe = await _read_resource_files(data_location, start_time, end_time=end_time,
                                           processes_to_treat_as_root=processes_to_treat_as_root)

    # Firstly, contemplate dropping users if they never cross the minimum resource usage threshold
    if minimum_resources_to_plot[0] > 0 and minimum_resources_to_plot[1] > 0:
        # Aggregate all datapoints by user & maximum
        dataframe_by_user = dataframe.groupby("username").agg({"cpu_percent": "max", "memory": "max"}).reset_index()

        # Remove users whose max datapoint isn't above the minimum allowed value
        good_users = np.logical_or(dataframe_by_user['cpu_percent'] > minimum_resources_to_plot[0],
                                   dataframe_by_user['memory'] > minimum_resources_to_plot[1])
        users_to_keep = dataframe_by_user.loc[good_users, 'username'].to_numpy()

        dataframe = dataframe.loc[np.isin(dataframe['username'], users_to_keep)].reset_index(drop=True)

    # Make a dataframe grouped by time - this is the total usage at every sampled step
    logging.debug("manipulating dataframe and plotting")
    dataframe_by_time = (dataframe
                         .groupby("time")
                         .agg({"cpu_percent": "sum", "memory": "sum", "threads": "sum"})
                         .reset_index())

    # Also do some other bits of setup
    unique_users = np.unique(dataframe["username"])
    unique_times = dataframe_by_time["time"].copy()

    # Add implied missing values and make a per-user dataframe for each
    user_dataframes = {}
    total_usage = {}
    for a_user in unique_users:
        # Make the user dataframe
        user_dataframes[a_user] = dataframe.loc[dataframe["username"] == a_user].reset_index(drop=True)

        # Add missing times onto the user dataframe by concatenating with a dataframe with an empty value for every
        # missing time
        time_is_missing = np.isin(unique_times, user_dataframes[a_user]["time"], invert=True)

        missing_times_df = pd.DataFrame(
            {"username": a_user, "cpu_percent": 0.0, "memory": 0.0, "threads": 0,
             "time": unique_times[time_is_missing]})

        user_dataframes[a_user] = (
            pd.concat([user_dataframes[a_user], missing_times_df], ignore_index=True)
            .sort_values("time")
            .reset_index(drop=True))

        # Record time delta between every measurement and the start, in hours
        user_dataframes[a_user]["time_delta"] = (user_dataframes[a_user]["time"] - start_time) / np.timedelta64(1, "h")

        # & use this to compute total use in hours
        total_usage[a_user] = {
            "cpu_hours": np.sum(user_dataframes[a_user]["time_delta"] * user_dataframes[a_user]["cpu_percent"]) / 100,
            "memory_hours": np.sum(user_dataframes[a_user]["time_delta"] * user_dataframes[a_user]["memory"]),
        }

    total_usage = pd.DataFrame(total_usage).T.reset_index()

    # Convert these to normalised percents
    total_usage['cpu_use_normed_percent'] = total_usage['cpu_hours'] / total_usage['cpu_hours'].sum() * 100
    total_usage['memory_use_normed_percent'] = total_usage['memory_hours'] / total_usage['memory_hours'].sum() * 100

    # Aggregate the data if necessary
    if aggregation_level is not None:

        # Firstly, let's create an array of times where each time is the start of every time window in unique_times
        if aggregation_level == "minute":
            unique_times_aggregated = np.asarray([
                datetime(x.year, x.month, x.day, x.hour, x.minute) for x in unique_times])
            tick_format_string = "%y.%m.%d %H:%M"

        elif aggregation_level == "hour":
            unique_times_aggregated = np.asarray([
                datetime(x.year, x.month, x.day, x.hour) for x in unique_times])
            tick_format_string = "%y.%m.%d %H:%M"

        elif aggregation_level == "day":
            unique_times_aggregated = np.asarray([
                datetime(x.year, x.month, x.day) for x in unique_times])
            tick_format_string = "%y.%m.%d"

        elif aggregation_level == "week":
            unique_times_aggregated = np.asarray([
                datetime(x.year, x.month, x.day) - timedelta(days=x.weekday()) for x in unique_times])
            tick_format_string = "%Y Wk. %V"

        elif aggregation_level == "month":
            unique_times_aggregated = np.asarray([
                datetime(x.year, x.month, 1) for x in unique_times])
            tick_format_string = "%Y.%m"

        elif aggregation_level == "year":
            unique_times_aggregated = np.asarray([
                datetime(x.year, 1, 1) for x in unique_times])
            tick_format_string = "%Y"

        else:
            raise ValueError(f"specified aggregation level {aggregation_level} not recognised! Must be one of "
                             f"minute, hour, day, week, month or year.")

        # Also allow the user to overwrite the default format string!
        if tick_format_string_overwrite is not None:
            tick_format_string = tick_format_string_overwrite

        # Assign the unique number of times here as our unique_times (this will later be our x values)
        unique_times = np.unique(unique_times_aggregated)

        # Next, we need to aggregate everything we want to plot later by these aggregated times
        dataframe_by_time["time"] = unique_times_aggregated

        dataframe_by_time = dataframe_by_time.groupby("time").agg({
            "cpu_percent": cpu_aggregation_mode, "memory": memory_aggregation_mode, "threads": "mean"})

        for a_user in unique_users:
            user_dataframes[a_user]["time"] = unique_times_aggregated
            user_dataframes[a_user] = user_dataframes[a_user].groupby("time").agg({
                "cpu_percent": cpu_aggregation_mode, "memory": memory_aggregation_mode, "threads": "mean"})

        # In the case of max aggregation modes, ensure that the plotted values add up to the true maximum (and not an
        # instantaneous maximum.) This is a bit of a hack to make sure that the total line and the user lines line up.
        # This happens because users will have max memory uses at different types, but the *total* max memory won't
        # always be when all users had their maximum.
        for a_mode, a_type in zip((cpu_aggregation_mode, memory_aggregation_mode), ("cpu_percent", "memory")):
            if a_mode == "max":
                real_maximums = dataframe_by_time[a_type]

                user_maximums = np.zeros(len(real_maximums))
                for a_user in unique_users:
                    user_maximums += user_dataframes[a_user][a_type].to_numpy()

                normalisation_constant = real_maximums / user_maximums

                for a_user in unique_users:
                    user_dataframes[a_user][a_type] *= normalisation_constant

    else:
        tick_format_string = default_tick_format_string

    # Start plotting!
    fig = plt.figure(figsize=(10, 6), dpi=300)
    gs = gridspec.GridSpec(2, 3)
    ax = []
    ax.append(fig.add_subplot(gs[0, 0:2]))
    ax.append(fig.add_subplot(gs[1, 0:2], sharex=ax[0]))
    ax.append(fig.add_subplot(gs[0, 2]))
    ax.append(fig.add_subplot(gs[1, 2]))

    # Plot the usage as a function of time
    # Todo: should aggregated plots really be plotted from the first value in the bin? It may be more appropriate to
    #   either a) use the middle value or b) make a stacked bar chart instead. Maybe I could add a kwarg for this? Since
    #   both may be useful in different circumstances.
    for an_ax, a_type in zip(ax[0:2], ("cpu_percent", "memory")):
        an_ax.plot(unique_times, dataframe_by_time[a_type], "k-", label="total", lw=1)

        y_users = [user_dataframes[x][a_type] for x in unique_users]
        an_ax.stackplot(unique_times, *y_users, labels=unique_users, alpha=1.0)

        an_ax.grid(which="major", alpha=0.7, axis="x")
        an_ax.grid(which="major", alpha=0.4, axis="y")

    # Plot total usage bar charts
    for an_ax, a_type in zip(ax[2:], ("cpu_use_normed_percent", "memory_use_normed_percent")):
        for i in range(len(unique_users)):
            an_ax.bar(i, total_usage.loc[i, a_type])
        an_ax.set_xticks(np.arange(len(unique_users)))
        an_ax.set_xticklabels(unique_users)
        an_ax.grid(which="major", alpha=0.4, axis="y")

    # Beautification
    ax[0].legend(edgecolor="k", framealpha=1.0, fontsize="x-small",)  #loc="center left", bbox_to_anchor=(1.1, -0.01), )
    ax[0].set(ylabel=cpu_aggregation_mode.capitalize() + " CPU usage (%)")
    ax[1].set(xlabel="Time", ylabel=memory_aggregation_mode.capitalize() + " memory usage (GB)",
              xlim=(np.min(unique_times), np.max(unique_times)))
    ax[2].set(ylabel="Share of CPU usage (%)")
    ax[3].set(xlabel="User", ylabel="Share of memory usage (%)")

    if end_time is None:
        end_time = datetime.now()
    start_time_formatted = start_time.strftime("%y.%m.%d %H:%M")
    end_time_formatted = end_time.strftime("%y.%m.%d %H:%M")
    fig.suptitle(
        f"Server utilisation from {start_time_formatted} to {end_time_formatted}, aggregation={aggregation_level}",
        x=0.5, y=0.93, fontsize="large"
    )

    for an_ax in ax:
        an_ax.minorticks_on()

    # X axis date formatting
    # (See https://stackoverflow.com/questions/9627686/plotting-dates-on-the-x-axis-with-pythons-matplotlib)
    ax[1].xaxis.set_major_formatter(mdates.DateFormatter(tick_format_string))

    # X label rotation
    # https://stackoverflow.com/questions/28062745/frequency-and-rotation-of-x-axis-labels-in-matplotlib/28063506
    plt.setp(ax[1].get_xticklabels(), rotation=45, horizontalalignment='right', fontsize=8)
    plt.setp(ax[3].get_xticklabels(), rotation=45, horizontalalignment='right')

    plt.setp(ax[0].get_xticklabels(), visible=False)
    plt.setp(ax[2].get_xticklabels(), visible=False)
    #ax[1].set_xticklabels(ax[1].get_xticks(), rotation=45)

    # Saving
    fig.subplots_adjust(hspace=0.05, wspace=0.35)
    fig.savefig(output_location, bbox_inches="tight", facecolor="w", dpi=dpi)
    plt.close(fig)

    return output_location
