#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# ---------------------------------------------------------------------------
"""Some demonstrative code for using the metrics agent"""
# ---------------------------------------------------------------------------
import logging
import logging.handlers
import os
import asyncio

from custom_logging import setup_logger, ColoredLogFormatter
from metrics_agent import MetricsAgent
from fast_database_clients.fast_influxdb_client import FastInfluxDBClient
from metrics_agent.processors import (
    JSONReader,
    Formatter,
    TimeLocalizer,
    ExpandFields,
    TimePrecision,
    RemoveOutliers,
    Rename,
)
from html_scraper_agent import HTMLScraperAgent


def setup_logging():
    import sys

    # Setup logging
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    debug_file_handler = logging.handlers.RotatingFileHandler(
        filename=f"logs/{script_name}.debug.log",
        mode="a",
        maxBytes=10_485_760,
        backupCount=10,
    )
    debug_file_handler.setLevel(logging.DEBUG)

    info_file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=f"logs/{script_name}.info.log",
        when="midnight",
        interval=1,
        backupCount=7,
    )
    info_file_handler.setLevel(logging.INFO)

    # set the log format for the handlers
    console_handler.setFormatter(
        ColoredLogFormatter(
            fmt="%(asctime)s,%(msecs)d - %(name)s - %(levelname)-8s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    debug_file_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s,%(msecs)d - %(name)s - %(levelname)-8s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    info_file_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s,%(msecs)d - %(name)s - %(levelname)-8s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    # create the logger
    logger = setup_logger(
        handlers=[
            console_handler,
            debug_file_handler,
            info_file_handler,
        ]
    )

    return logger


def main():

    from node_client import NodeSwarmClient
    from metrics_agent import load_config
    from network_simple import SimpleServerTCP

    config = load_config("config/application.toml")
    logger = setup_logging()

    # Create a client for the agent to write data to a database
    database_client = FastInfluxDBClient.from_config_file(
        config_file="config/influx_test.toml"
    )

    # create the agent and assign it the client and desired processors
    agent = MetricsAgent(
        database_client=database_client,
        processors=[
            JSONReader(),
            TimeLocalizer(),
            TimePrecision(),
            ExpandFields(),
            Formatter(),
            RemoveOutliers(),
            Rename(),
        ],
        config=config["agent"],
    )

    # Start TCP Server

    server_address = (
        config["server"]["host"],
        config["server"]["port"],
    )

    # server_tcp = SimpleServerTCP(
    #     output_buffer=agent._input_buffer,
    #     server_address=server_address,
    # )

    # # Set up an Agent to retrieve data from the Arduino nodes
    # node_client = NodeSwarmClient(
    #     buffer=agent._input_buffer,
    #     update_interval=config["node_client"]["update_interval"],
    # )

    # Initialize html scraper
    scraper_agent = HTMLScraperAgent(agent._input_buffer)

    config_scraper = config["html_scraper_agent"]
    # scraper_address = "config/test.html"

    async def gather_data_from_agents():
        await asyncio.gather(
            scraper_agent.do_work_periodically(
                update_interval=config_scraper["update_interval"],
                server_address=config_scraper["scrape_address"],
            ),
            # node_client.request_data_periodically(),
        )

    asyncio.run(gather_data_from_agents())


def example_with_server():

    from metrics_agent import load_toml_file

    config = load_toml_file("config/application.toml")

    # Create a client for the agent to write data to a database
    database_client = FastInfluxDBClient(
        config="config/influx_test.toml",
        default_bucket="testing",
    )
    logger = setup_logging(database_client._client)

    # create the agent and assign it the client and desired processors
    agent = MetricsAgent(
        database_client=database_client,
        processors=[
            JSONReader(),
            TimeLocalizer(),
            TimePrecision(),
            ExpandFields(),
            Formatter(),
        ],
        config=config["agent"],
    )

    # Specify server address directly
    # server_address = ("localhost", 0)
    # Or from configuration
    server_address = (
        config["server"]["host"],
        config["server"]["port"],
    )

    # Start TCP Server
    from network_simple import SimpleServerTCP

    server_tcp = SimpleServerTCP(
        output_buffer=agent._input_buffer,
        server_address=server_address,
    )

    # Start UDP Server
    # from network_simple.server import SimpleServerUDP

    # server_udp = SimpleServerUDP(
    #     output_buffer=agent._input_buffer,
    #     server_address=server_address,
    # )
    import time

    while True:
        time.sleep(1)


def test():
    from metrics_agent import load_toml_file

    config = load_toml_file("config/application.toml")

    # Create a client for the agent to write data to a database
    database_client = FastInfluxDBClient(
        config="config/influx_test.toml",
        default_bucket="testing",
    )
    logger = setup_logging(database_client._client)

    # # create the agent and assign it the client and desired aggregator, as well as the desired interval for updating the database
    agent = MetricsAgent(
        db_client=database_client,
        processors=[JSONReader(), TimeLocalizer(), ExpandFields(), Formatter()],
        config=config["agent"],
    )

    # Create a client to send metrics over TCP
    from metrics_agent import csv_to_metrics

    test_metrics = csv_to_metrics("test/test_metrics.csv")
    for metric in test_metrics:
        agent.add_metric_to_queue(measurement="test_measurement", **metric)

    import time

    while True:
        time.sleep(1)


if __name__ == "__main__":
    # test()
    # example_with_server()
    main()
