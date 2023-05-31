"""example-postcsv.py
    Example Python Script to Post a CSV file with header to Observe.

    Usage: 
        python3 example-postcsv.py filename

    Requires:
        1. Available ObserveInc Instance
        2. Configured Observe Datastream and Token.
        3. Installed Observe HTTP Sender python module.

    Reference:
        1. https://docs.observeinc.com/en/latest/content/data-ingestion/datastreams.html
        2. https://docs.observeinc.com/en/latest/content/data-ingestion/endpoints/http.html

"""

# Import standard libraries.
import csv
import time
import logging
import os
import sys

#Import Observe HTTP Sender Class
from observe_http_sender import ObserveHttpSender

def main(filename):

    # Initialize the example script logging config.
    logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S %z')
    logger = logging.getLogger(u"OBSERVE_EXAMPLE")
    logger.setLevel(logging.INFO)

    # Get Observer Values from OS Environment Variables.
    """
    Example to set your Environment Variables in your terminal shell, add the values as appropriate:

    export OBSERVE_CUSTOMER=
    export OBSERVE_TOKEN=
    export OBSERVE_DOMAIN=

    """

    # Retrieve the API Configuration/Credential information from Environment Variables.
    OBSERVE_CUSTOMER = os.getenv('OBSERVE_CUSTOMER')
    OBSERVE_TOKEN = os.getenv('OBSERVE_TOKEN')
    OBSERVE_DOMAIN = os.getenv('OBSERVE_DOMAIN')

    # Check for Observer Configuration Values.
    observer_exception = None

    if OBSERVE_CUSTOMER is None:
        observer_exception = Exception("Observer Customer ID is missing.")
    if OBSERVE_TOKEN is None:
        observer_exception = (Exception("Observer Datastream API Token is missing."))
    if OBSERVE_DOMAIN is None:
        observer_exception = (Exception("Observer Instance Domain is missing."))

    # Raise Exception if Required Values are missing.
    if observer_exception:
        logger.exception(observer_exception)
        raise(observer_exception)

    # Setup Observer and its logging level.
    observer = ObserveHttpSender(OBSERVE_CUSTOMER,OBSERVE_TOKEN,OBSERVE_DOMAIN)
    observer.log.setLevel(logging.INFO)

    # Check Observer for reachability
    observer_reachable =  observer.check_connectivity()
    if observer_reachable is False:
        raise(Exception("Observer Not Reachable: Customer=%s Instance=%s" % (OBSERVE_CUSTOMER,OBSERVE_DOMAIN)))
    
    # Set timestamp and refine the data to post because our input file has no timestamp in it.
    observe_post_time = str(round(time.time(),3))
    metafields = {'timestamp':observe_post_time}

    # Read and Post the file contents
    ioc_list = list()
    with open(filename) as ioc_file:
        # use Dict Reader as the input file is a csv with header row
        csv_reader = csv.DictReader(ioc_file, delimiter=",")
        line_count = 0
        for row in csv_reader:
            # Add the event meta fields before posting to Observe.
            row.update(metafields)
            # Post each row without having to retain full input file in memory.
            try:
                observer.post_observation(row)
            except Exception as e:
                logger.exception(e)

    # Call the required flush to ensure any remaining data that did not fill the full batch size is posted to Observe.
    try:
        observer.flush()
    except Exception as e:
        logger.exception(e)

if __name__ ==  "__main__":

    # Get Arguments
    args = sys.argv

    if len(args) == 1:
        raise(Exception("Filename Required."))
        sys.exit(1)
    
    main(sys.argv[1])