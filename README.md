# Python Class for Sending Data to ObserveInc HTTP Endpoint

Version/Date: 1.3.1 2023-12-29

## Description:

This is a python class for use with other python code to send events to an Observe Inc Datastream via the [API http event endpoint](https://docs.observeinc.com/en/latest/content/data-ingestion/endpoints/http.html).

## Using this Python Class

### Usage: Manual

You will need to put this with any other code and import the class as needed.
1. Instantiate a copy of the `ObserveHttpSender` object
1. Submit JSON dictionary payload(s) via the `post_observation` method.
1. Call the `flush` method to ensure any non batched payload data is flushed to Observe.

### Usage: With pip3

    pip3 install observe_http_sender@git+ssh://git@github.com/observeinc/observe-http-sender-python.git

Once installed you can start python try the following.

#### Python Test:

    from observe_http_sender import ObserveHttpSender 
    help(ObserveHttpSender)

### Getting Started:

The arguments needed to initialize an Observer:

* OBSERVE_URL -- The base Observe customer URL - required (Example: https://154418444508.observeinc.com/)
* OBSERVE_TOKEN -- The configured Datastream API Token - required

#### Python Usage:

    from observe_http_sender import ObserveHttpSender

    # Setup Observer and its logging level.
    observer = ObserveHttpSender(OBSERVE_URL,OBSERVE_TOKEN)
    observer.log.setLevel(logging.INFO)

### Logging

You may use logging by setting up a logger in your code from the `import logging` module.

    logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S %z')
    logger = logging.getLogger(u"OBSERVE_EXAMPLE")
    logger.setLevel(logging.INFO)

You may set a different logging level for the `ObserveHttpSender` class.

    # Setup Observer and its logging level.
    observer = ObserveHttpSender(OBSERVE_CUSTOMER,OBSERVE_TOKEN,OBSERVE_DOMAIN)
    observer.log.setLevel(logging.INFO)
    
# Notes:

* The `check_connectivity` method that is optional but recommended before attempting to submit large amounts of data to Observe. See example.py for use and docstrings on the method for details.
* You MUST call the method `flush` before your code completes to ensure all remaining non batch posted data is sent to Observe.
* Methods `get_pop_empty_fields` and `set_pop_empty_fields`. Defaults to True to remove empty/null fields from payloads to save ingestion cost.
* Methods `get_payload_json_format` and `set_payload_json_format`. Defaults to True to post payload in format `application/json`. False will post the payload in format `text/plain` Set this at instantiation for text payloads.
* Methods `get_post_path` and `set_post_path`. Defaults to None append an optional path segment example `/orca/alerts`. This will show in the Datastream EXTRAS field as `path`.

# Example Usage:

The included python script `example-postcsv.py` takes a csv file with a header row and posts it to Observe in JSON format.

Usage: 

    python3 example-postcsv.py data/sample-authevents.csv

