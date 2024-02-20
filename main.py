# #!/usr/bin/env python
# # -*- coding: utf-8 -*-
# # ----------------------------------------------------------------------------
# # Created By  : Matthew Davidson
# # Created Date: 2024-01-23
# # ---------------------------------------------------------------------------
# """Some demonstrative code for using the metrics agent"""
# # ---------------------------------------------------------------------------
# from logging.config import dictConfig
# import asyncio

<<<<<<< HEAD
# from data_node_network.node_client import Node, NodeClientTCP
# from data_node_network.configuration import node_config
# from metrics_agent import MetricsAgent
# from fast_database_clients.fast_influxdb_client import FastInfluxDBClient
# from metrics_agent import (
#     JSONReader,
#     Formatter,
#     TimeLocalizer,
#     ExpandFields,
#     TimePrecision,
# )


# def setup_logging(filepath="config/logger.yaml"):
#     import yaml
#     from pathlib import Path
=======
from custom_logging import setup_logger, ColoredLogFormatter
from metrics_agent import MetricsAgent
from fast_database_clients.fast_influxdb_client import FastInfluxDBClient
from metrics_agent import (
    JSONReader,
    Formatter,
    TimeLocalizer,
    ExpandFields,
    TimePrecision,
)
from html_scraper_agent import HTMLScraperAgent


def setup_logging():
    import sys
>>>>>>> 610e534d407cd43d85bef99732fc3fd49201d77d

#     if Path(filepath).exists():
#         with open(filepath, "r") as stream:
#             config = yaml.load(stream, Loader=yaml.FullLoader)
#     else:
#         raise FileNotFoundError
#     logger = dictConfig(config)
#     return logger

# def main():

<<<<<<< HEAD
#     from metrics_agent import load_config
#     from network_simple import SimpleServerTCP
=======
    debug_file_handler = logging.handlers.RotatingFileHandler(
        filename=f"logs/{script_name}.debug.log",
        mode="a",
        maxBytes=10_485_760,
        backupCount=10,
    )
    debug_file_handler.setLevel(logging.DEBUG)
>>>>>>> 610e534d407cd43d85bef99732fc3fd49201d77d

#     config = load_config("config/application.toml")

#     # Create a client for the agent to write data to a database
#     database_client = FastInfluxDBClient.from_config_file(
#         config_file="config/influx_test.toml"
#     )
#     logger = setup_logging(database_client)

<<<<<<< HEAD
#     # create the agent and assign it the client and desired processors
#     agent = MetricsAgent(
#         database_client=database_client,
#         processors=[
#             JSONReader(),
#             TimeLocalizer(),
#             TimePrecision(),
#             ExpandFields(),
#             Formatter(),
#         ],
#         config=config["agent"],
#     )

#     # Start TCP Server
=======
    # create the logger
    logger = setup_logger(
        handlers=[
            console_handler,
            debug_file_handler,
            info_file_handler,
        ]
    )
>>>>>>> 610e534d407cd43d85bef99732fc3fd49201d77d

#     server_address = (
#         config["server"]["host"],
#         config["server"]["port"],
#     )

#     server_tcp = SimpleServerTCP(
#         output_buffer=agent._input_buffer,
#         server_address=server_address,
#     )

#     # Create a client
#     node_list = [Node(node) for node in node_config.values()]
#     node_client: NodeClientTCP = NodeClientTCP(node_list, buffer=agent._input_buffer)
    
#     node_client.start(interval=config["node_network"]["node_client"])



<<<<<<< HEAD
# if __name__ == "__main__":
    
#     setup_logging()
#     main()
=======
    from node_client import NodeSwarmClient
    from metrics_agent import load_config
    from network_simple import SimpleServerTCP

    config = load_config("config/application.toml")
    logger = setup_logging()

    # Create a client for the agent to write data to a database
    database_client = FastInfluxDBClient.from_config_file(
        config_file="config/influx_live.toml"
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
        ],
        config=config["agent"],
    )

    # Start TCP Server

    server_address = (
        config["server"]["host"],
        config["server"]["port"],
    )

    server_tcp = SimpleServerTCP(
        output_buffer=agent._input_buffer,
        server_address=server_address,
    )

    # # Set up an Agent to retrieve data from the Arduino nodes
    node_client = NodeSwarmClient(
        buffer=agent._input_buffer,
        update_interval=config["node_client"]["update_interval"],
    )

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
            node_client.request_data_periodically(),
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
>>>>>>> 610e534d407cd43d85bef99732fc3fd49201d77d
