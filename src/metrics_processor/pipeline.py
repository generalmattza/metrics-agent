#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass, asdict, field, is_dataclass
from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
import json
import yaml
import pytz
import logging

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

logger = logging.getLogger(__name__)


# Default Parameters
# *******************************************************************
PIPELINE_CONFIG_DEFAULT = "config/metric_pipelines.toml"

# Helper Functions
# *******************************************************************


def load_yaml_file(filepath):
    with open(filepath, "r") as file:
        return yaml.safe_load(file)


def load_toml_file(filepath):
    with open(filepath, mode="rb") as fp:
        return tomllib.load(fp)

def shorten_data(data: str, max_length: int = 75) -> str:
    """Shorten data to a maximum length."""
    if not isinstance(data, str):
        data = str(data)
    data = data.strip()
    return data[:max_length] + "..." if len(data) > max_length else data

# Helper Functions
# *******************************************************************


def expand_metric_fields(original_dict):

    metrics_expanded = []

    for field, value in original_dict["fields"].items():
        new_dict = {
            "measurement": original_dict["measurement"],
            "fields": {field: value},
            "tags": original_dict.get("tags", {}),
            "time": original_dict.get("time", {}),
        }
        metrics_expanded.append(new_dict)

    return metrics_expanded


def expand_metrics(metrics):
    expanded_metrics = []
    for metric in metrics:
        if is_dataclass(metric):
            metric = asdict(metric)
        if not isinstance(metric, dict):
            message = (
                "Metric is not dict, convert to a dict before using this processor"
            )
            logger.error(message)
            raise TypeError(message)
        expanded_metric = expand_metric_fields(metric)
        expanded_metrics.extend(expanded_metric)
    return expanded_metrics


def localize_timestamp(timestamp, timezone_str="UTC") -> datetime:
    """
    Localize a timestamp to a timezone
    :param timestamp: The timestamp to localize
    :param timezone_str: The timezone to localize to
    :return: The localized timestamp
    """

    if isinstance(timestamp, (int, float)):
        dt_utc = datetime.fromtimestamp(timestamp)
    elif isinstance(timestamp, datetime):
        dt_utc = timestamp
    else:
        raise ValueError("timestamp must be a float, int, or datetime object")
    timezone = pytz.timezone(timezone_str)
    return int(timezone.localize(dt_utc).timestamp())


# Dataclasses
# *******************************************************************
@dataclass
class MetricStats:
    name: str
    value: dict = field(
        default_factory=dict(
            mean=None,
            max=None,
            min=None,
            count=None,
            std=None,
            sum=None,
        )
    )
    time: datetime = None

    def __iter__(self):
        yield from asdict(self).values()


# Pipeline Classes
# *******************************************************************

class MetricsPipeline(ABC):
    def __init__(self, config=None) -> None:

        if config:
            self._external_config = False
            self.config = config
        else:
            self._external_config = True
            self.config = self._load_config(PIPELINE_CONFIG_DEFAULT)

    def refresh_config(self):
        if self._external_config:
            self.config = self._load_config(PIPELINE_CONFIG_DEFAULT)

    def _load_config(self, filepath):
        class_name = self.__class__.__name__
        try:
            return load_toml_file(filepath)[class_name]
        except KeyError:
            logger.debug(f"No configuration specified for class {class_name}")
            return None

    def process(self, metrics):
        self.refresh_config()
        results = self.process_method(metrics)

        return results

    @abstractmethod
    def process_method(self, metrics): ...

    def __repr__(self):
        return self.__class__.__name__


class AggregateStatistics(MetricsPipeline):
    def process_method(self, metrics):
        df = pd.DataFrame(metrics).set_index("name")
        df_mean = df.groupby("name").mean()
        df_time = df_mean.drop(columns=["value"])
        df_notime = df.drop(columns=["time"]).groupby("name")

        mean = df_mean.drop(columns=["time"]).rename(columns={"value": "mean"})
        max = df_notime.max().rename(columns={"value": "max"})
        min = df_notime.min().rename(columns={"value": "min"})
        count = df_notime.count().rename(columns={"value": "count"})
        std = df_notime.std().rename(columns={"value": "std"})
        sum = df_notime.sum().rename(columns={"value": "sum"})

        metrics_stats_dict = pd.concat(
            [mean, max, min, count, std, sum],
            axis=1,
        ).to_dict(orient="index")

        metrics_stats = [
            MetricStats(name=k, value=v, time=df_time.loc[k, "time"])
            for k, v in metrics_stats_dict.items()
        ]

        return metrics_stats


class JSONReader(MetricsPipeline):
    def process_method(self, metrics):
        for i, metric in enumerate(metrics):
            if isinstance(metric, str):
                metrics[i] = json.loads(metric)
        return metrics


class ExtraTagger(MetricsPipeline):

    def process_method(self, metrics):

        tags_extra = load_yaml_file(self.config["extra_tags_filepath"])

        for metric in metrics:
            metric["tags"] = metric["tags"] | tags_extra

        return metrics


class TimeLocalizer(MetricsPipeline):

    def process_method(self, metrics):
        self.local_tz = self.config["local_tz"]
        for metric in metrics:
            metric["time"] = localize_timestamp(metric["time"], self.local_tz)
        return metrics


class TimePrecision(MetricsPipeline):

    def process_method(self, metrics):
        for metric in metrics:
            metric["time"] = int(metric["time"])
        return metrics


class FieldExpander(MetricsPipeline):

    def process_method(self, metrics):
        metrics = expand_metrics(metrics)
        return metrics


class Formatter(MetricsPipeline):

    def process_method(self, metrics):

        formats = load_yaml_file(self.config["formats_filepath"])

        metrics = self.format_metrics(metrics, formats)

        return metrics

    def format_metrics(self, metrics, formats):

        for metric in metrics:
            for k, _ in metric["fields"].items():

                try:
                    format = formats[k]
                except KeyError:
                    # No format specified for key, continue
                    continue

                if format["type"] == "float":
                    metric["fields"][k] = float(metric["fields"][k])
                elif format["type"] == "str":
                    metric["fields"][k] = str(metric["fields"][k])
                else:
                    logger.warning(
                        "type not specified in metric format, defaulting to str"
                    )
                    metric["fields"][k] = str(metric["fields"][k])

                # try:
                #     metric["fields"] = {format["db_fieldname"]: metric["fields"][k]}
                # except KeyError:
                #     # No database fieldname specified, use existing field name
                #     logger.debug(
                #         f'No database fieldname specified for metric {metric["measurement"]}:{metric["fields"][k]}, use existing field name'
                #     )
                #     continue
                try:
                    metric["tags"] = metric["tags"] | format["tags"]
                except KeyError:
                    # No additonal tags have been specified for metric, continue
                    pass

        return metrics

class Renamer(MetricsPipeline):
    def process_method(self, metrics):
        name_mapping = load_yaml_file(self.config["name_mapping_filepath"])
        metrics = self.rename_metrics(metrics, name_mapping)
        return metrics

    def rename_metrics(self, metrics, name_mapping):
        for metric in metrics:
            for field in metric["fields"]:
                try:
                    metric["fields"] = {name_mapping[field]: metric["fields"][field]}
                except KeyError:
                    # No database fieldname specified, use existing field name
                    logger.debug(
                        f'No database fieldname specified for metric {metric["measurement"]}:{metric["fields"][field]}, use existing field name'
                    )
        return metrics


class OutlierRemover(MetricsPipeline):

    def process_method(self, metrics):
        boundaries = load_yaml_file(self.config["boundaries_filepath"])
        metrics = self.remove_outliers(metrics, boundaries)
        return metrics

    def remove_outliers(self, metrics, boundaries):
        metrics_filtered = []
        metrics_removed = []
        for metric in metrics:
            for field in metric["fields"]:
                try:
                    boundary = boundaries[field]
                except KeyError:
                    pass
                value = metric["fields"][field]
                if isinstance(value, str):
                    # If value is string, do nothing (This may be changed in future)
                    metrics_filtered.append(metric)
                    continue
                try:
                    if value > boundary["max"]:
                        metrics_removed.append(metric)
                        continue
                except KeyError:
                    pass
                try:
                    if value < boundary["min"]:
                        metrics_removed.append(metric)
                        continue
                except KeyError:
                    pass
                metrics_filtered.append(metric)
        logger.debug(
            f"Removed {len(metrics_removed)} metrics: {shorten_data(str(metrics_removed))}"
        )
        return metrics_filtered