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
from datetime import datetime, timedelta
import pandas as pd
import json
from multiprocessing import Pool
import yaml
import pytz
import logging
import time

from prometheus_client import Histogram, Counter

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
TIMEZONE_CACHE = {}  # Added to help improve performance


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


def get_timezone(timezone_str):
    """
    Get a timezone object from cache or create and cache it if not found
    """
    if timezone_str not in TIMEZONE_CACHE:
        TIMEZONE_CACHE[timezone_str] = pytz.timezone(timezone_str)
    return TIMEZONE_CACHE[timezone_str]


def localize_timestamp(timestamp, timezone_str="UTC", offset=(0, 0, 0)) -> datetime:
    """
    Localize a timestamp to a timezone
    :param timestamp: The timestamp to localize
    :param timezone_str: The timezone to localize to
    :return: The localized timestamp
    """
    # Convert to datetime if not already
    if isinstance(timestamp, (int, float)):
        dt_utc = datetime.fromtimestamp(timestamp)
    elif isinstance(timestamp, datetime):
        dt_utc = timestamp
    else:
        raise ValueError("timestamp must be a float, int, or datetime object")

    # Apply offset in the form (0,0,0) representing (hours, minutes, seconds)
    dt_utc = dt_utc + timedelta(hours=offset[0], minutes=offset[1], seconds=offset[2])

    # Retrieve timezone. Previously used timezones are cached
    timezone = get_timezone(timezone_str)

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

    processing_time = Histogram(
        "metrics_processor_processing_time",
        "Average time taken to process a metric",
        ["agent", "pipeline"],
    )

    metrics_processed = Counter(
        "metrics_processed_pipeline",
        "Number of metrics processed",
        ["agent", "pipeline"],
    )

    metrics_filtered = Counter(
        "metricsprocessor_metrics_filtered",
        "Number of metrics filtered out",
        ["agent", "pipeline", "id", "reason"],
    )

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
        start_time = time.perf_counter()
        number_of_metrics = len(metrics)
        self.refresh_config()
        if metrics:
            results = self.process_method(metrics)
        else:
            logger.info(
                f"No metrics to process in {self.__class__.__name__}. Continuing"
            )
            return None

        end_time = time.perf_counter()

        if number_of_metrics != 0:
            self.processing_time.labels(
                agent="metrics_processor", pipeline=self.__class__.__name__
            ).observe((end_time - start_time) / number_of_metrics)
            self.metrics_processed.labels(
                agent="metrics_processor", pipeline=self.__class__.__name__
            ).inc(number_of_metrics)
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


class FilterNone(MetricsPipeline):
    def process_method(self, metrics):
        # Remove all None values from metrics
        number_metrics_initial = len(metrics)
        metrics = [metric for metric in metrics if metric is not None]
        number_metrics_final = len(metrics)
        self.metrics_filtered.labels(
            agent="metrics_processor",
            pipeline=self.__class__.__name__,
            id="None",
            reason="Invalid metric",
        ).inc(number_metrics_initial - number_metrics_final)
        return metrics


class JSONReader(MetricsPipeline):

    def process_method(self, metrics):
        if self.num_processes.get("num_processes"):
            with Pool(self.config.num_processes) as pool:
                results = pool.map(self.parse_json, metrics)
            return results
        else:
            return [self.parse_json(metric) for metric in metrics]

    @staticmethod
    def parse_json(metric):
        if isinstance(metric, str):
            return json.loads(metric)
        return metric


class ExtraTagger(MetricsPipeline):

    def process_method(self, metrics):

        tags_extra = self.config

        for metric in metrics:
            metric["tags"] = metric["tags"] | tags_extra

        return metrics


class TimeLocalizer(MetricsPipeline):

    def process_method(self, metrics):
        self.local_tz = self.config["local_tz"]
        for metric in metrics:
            # logger.debug("TimeLocalizer: Raw time is %s", metric["time"])
            local_time = localize_timestamp(
                metric["time"], timezone_str=self.local_tz, offset=self.config["offset"]
            )
            # if local_time differs by more than 59 minutes from actual local time, then offset by one hour using datime.timedelta
            if abs(local_time - int(time.time())) > 3540:
                reverse_offset = [-offset for offset in self.config["offset"]]
                local_time = datetime.fromtimestamp(local_time) + timedelta(
                    hours=reverse_offset[0],
                    minutes=reverse_offset[1],
                    seconds=reverse_offset[2],
                )
                local_time = local_time.timestamp()
            metric["time"] = local_time
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
                    logger.debug(
                        f"Metric:{metric['fields'][k]} - Type not specified in metric format, defaulting to str"
                    )
                    metric["fields"][k] = str(metric["fields"][k])

                try:
                    metric["tags"] = metric["tags"] | format["tags"]
                except KeyError:
                    # No additonal tags have been specified for metric, continue
                    pass

        return metrics


class PropertyMapper(MetricsPipeline):
    def __init__(self, config=None):
        super().__init__(config)
        self.property_mapping = self.load_property_mapping()

    def load_property_mapping(self):
        # Load the property mapping only once during initialization
        return load_yaml_file(self.config["property_mapping_filepath"])

    def process_method(self, metrics):
        # Directly use the loaded property mapping
        return self.map_metric_properties(metrics)

    def map_metric_properties(self, metrics):
        # Initialize an empty list to store the updated metrics
        updated_metrics = []

        for metric in metrics:
            new_metric = {}
            for property, values in metric.items():
                if property in self.property_mapping:
                    # Map each property using the preloaded mapping
                    new_values = {
                        self.property_mapping[property].get(p, p): values[p]
                        for p in values
                    }
                    new_metric[property] = new_values
                else:
                    new_metric[property] = values
            updated_metrics.append(new_metric)

        return updated_metrics

    # def map_metric_properties(self, metrics):
    #     # Initialize an empty list to store the updated metrics
    #     updated_metrics = []

    #     for metric in metrics:
    #         new_metric = {}
    #         for property, values in metric.items():
    #             if property in self.property_mapping:
    #                 # Map each property using the preloaded mapping
    #                 new_values = {}
    #                 for p in values:
    #                     if p in self.property_mapping[property]:
    #                         new_key = self.property_mapping[property][p]
    #                         new_values[new_key] = values[p]
    #                         logger.debug(
    #                             f'Remapped property {property} to {new_key} for metric {metric["measurement"]}'
    #                         )
    #                     else:
    #                         new_values[p] = values[p]
    #                         logger.debug(
    #                             f'No property mapping specified for {metric["measurement"]}:{values[p]}, use existing field name'
    #                         )
    #                 new_metric[property] = new_values
    #             else:
    #                 new_metric[property] = values
    #         updated_metrics.append(new_metric)

    #     return updated_metrics


class OutlierRemover(MetricsPipeline):

    def __init__(self, config=None) -> None:
        super().__init__(config=config)

    def process_method(self, metrics):
        boundaries = load_yaml_file(self.config["boundaries_filepath"])
        metrics = self.remove_outliers(metrics, boundaries)
        return metrics

    def remove_outliers(self, metrics, boundaries):
        metrics_filtered = []
        metrics_removed = []
        for metric in metrics:
            for field in metric["fields"]:
                boundary = boundaries.get(field)
                if boundary is None:
                    metrics_filtered.append(metric)
                    continue

                value = metric["fields"][field]
                if isinstance(value, str):
                    metrics_filtered.append(metric)
                    continue

                try:
                    if "max" in boundary and value > boundary["max"]:
                        metrics_removed.append(metric)
                        self.metrics_filtered.labels(
                            agent="metrics_processor",
                            pipeline=self.__class__.__name__,
                            id=field,
                            reason="Value excceeded max",
                        ).inc()
                        continue
                except KeyError:
                    pass

                try:
                    if "min" in boundary and value < boundary["min"]:
                        metrics_removed.append(metric)
                        self.metrics_filtered.labels(
                            agent="metrics_processor",
                            pipeline=self.__class__.__name__,
                            id=field,
                            reason="Value below min",
                        ).inc()
                        continue
                except KeyError:
                    pass

                metrics_filtered.append(metric)

        number_of_outliers_removed = len(metrics_removed)

        logger.debug(
            f"Removed {number_of_outliers_removed} metrics: {shorten_data(str(metrics_removed))}"
        )
        return metrics_filtered
