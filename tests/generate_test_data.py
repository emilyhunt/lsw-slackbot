"""File for generating simple test data."""
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# Firstly, let's make three test dataframes recorded on different times
np.random.seed(42)

for a_day in (1, 2, 3):
    start_date = datetime(2020, 1, a_day)

    output_file = Path("./test_data/" + start_date.strftime("%Y-%m-%d_server_usage.csv"))

    x = np.linspace(0, 50 * np.pi, num=1000)
    y_jack = np.cos(x)
    y_jill = np.sin(x)

    current_date = start_date + timedelta(minutes=np.random.randint(0, 9))

    datadict = {"username": [], "cpu_percent": [], "memory": [], "threads": [], "time": []}

    i = 0
    while current_date < start_date + timedelta(days=1):
        print(f"\r{a_day} {current_date.strftime('%H:%M')}", end="")

        datadict["username"].append("root")
        datadict["cpu_percent"].append(np.random.uniform(0.2, 2.5))
        datadict["memory"].append(np.random.uniform(0.5, 1.0))
        datadict["threads"].append(np.random.randint(115, 150))
        datadict["time"].append(current_date)

        if y_jack[i] > 0:
            datadict["username"].append("jack")
            datadict["cpu_percent"].append(np.random.uniform(40, 45))
            datadict["memory"].append(y_jack[i])
            datadict["threads"].append(4)
            datadict["time"].append(current_date)

        if y_jill[i] > 0:
            datadict["username"].append("jill")
            datadict["cpu_percent"].append(np.random.uniform(15, 20))
            datadict["memory"].append(y_jill[i])
            datadict["threads"].append(2)
            datadict["time"].append(current_date)

        current_date = current_date + timedelta(minutes=10)
        i += 1

    pd.DataFrame(datadict).to_csv(output_file, index=False)
