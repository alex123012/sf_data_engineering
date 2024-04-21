from __future__ import annotations  # noqa: INP001, EXE002

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pendulum
from airflow.decorators import dag, task


if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


SHARED_FOLER = Path(__file__).parent / ".." / "shared_folder"
PRODUCTS = ("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")


def generate_time_delta(execution_date: str) -> pd.Index:
    start_date = pd.to_datetime(execution_date) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(execution_date) + pd.DateOffset(months=1)
    return pd.date_range(start=start_date, end=end_date, freq="M").strftime("%Y-%m-01")


@dag(
    dag_id="Makhonin_Alex_HW_DAG_Bonus",
    schedule="0 0 5 * *",
    start_date=pendulum.datetime(year=2023, month=10, day=1),
)
def taskflow() -> None:

    @task(task_id="extract")
    def extract(ti: TaskInstance) -> str:
        profit_table = pd.read_csv(SHARED_FOLER / "profit_table.csv")
        date_list = generate_time_delta(ti.execution_date)
        profit_table_filtered = (
            profit_table[profit_table["date"].isin(date_list)].drop("date", axis=1).groupby("id").sum()
        )
        return profit_table_filtered.to_json()

    @task(task_id="transform")
    def transform(product: str, profit_table_filtered_json: str) -> str:
        profit_table_filtered = pd.read_json(profit_table_filtered_json)
        product_data = profit_table_filtered.apply(
            lambda x: x[f"sum_{product}"] != 0 and x[f"count_{product}"] != 0,
            axis=1,
        ).astype(int)

        return product_data.to_json()

    @task(task_id="load")
    def load(data: dict[str, str]) -> None:
        flags_activity = pd.DataFrame({key: pd.read_json(data[key], typ="series") for key in sorted(data.keys())})
        flags_activity.index.name = "id"
        flags_activity.to_csv(SHARED_FOLER / "flags_activity_bonus.csv", mode="a")

    profit_table_filtered = extract()  # type: ignore[call-arg, misc]
    load({f"flag_{p}": transform.override(task_id=f"transform_{p}")(p, profit_table_filtered) for p in PRODUCTS})  # type: ignore[call-arg, arg-type, misc] # noqa: E501


taskflow()
