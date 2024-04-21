from __future__ import annotations  # noqa: INP001

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from transform_script import transfrom as transform_script


if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


SHARED_FOLER = Path(__file__).parent / ".." / "shared_folder"


@dag(dag_id="Makhonin_Alex_HW_DAG", schedule="0 0 5 * *", start_date=pendulum.datetime(year=2023, month=10, day=1))
def taskflow() -> None:

    @task(task_id="extract")
    def extract() -> str:
        return pd.read_csv(SHARED_FOLER / "profit_table.csv").to_json()

    @task(task_id="transform")
    def transform(profit_table_json: str, ti: TaskInstance) -> str:
        profit_table = pd.read_json(profit_table_json)
        return transform_script(profit_table, ti.execution_date).to_json()

    @task(task_id="load")
    def load(data: str) -> None:
        pd.read_json(data).to_csv(SHARED_FOLER / "flags_activity.csv", index=False, mode="a")

    load(transform(extract()))  # type: ignore[call-arg, arg-type, misc]


taskflow()
