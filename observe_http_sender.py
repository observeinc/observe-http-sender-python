"""observe_http_sender.py
    Observer observation submission class to HTTP endpoint
"""

__version__ = "1.3.0"

import json
import logging
import gzip
import re

import asyncio
from aiohttp import ClientSession

from aiohttp_retry import RetryClient, JitterRetry

# Class for Queue objects.
class _ObserveQueue:
    """
    This is an internal class for Queue objects.
      
    Attributes:
        elements (list): Optional list of elements to place on the queue at initializaiton.
    """
  
    def __init__(self, elements = None):
        """
        The constructor for _Queue class.
  
        Parameters:
           elements (list): Optional list of elements to place on the queue at initializaiton  
        """
          
        if elements is None:
            self.elements = list()
        else:
            self.elements = elements

    def enqueue(self, item):
        """Add an element to the queue.

            Parameters:
                item (dict): dictionary item to add to the queue
            Returns:
                none
        """
        self.elements.append(item)

    def dequeue(self):
        """Remove an element to the queue.

            Parameters:
               none
            Returns:
                dict: dictionary item removed from the queue
        """

        if self.elements:
            return self.elements.pop(0)
      
    def clear(self):
        """Clears the queue of all elements.

            Parameters:
               none
            Returns:
                none
        """
         
        self.elements = list()

    def __str__(self):
        """Outputs current list of elements in the queue.

            Parameters:
               none
            Returns:
                str: string of the list of the elements in the queue.
        """
        return str(self.elements)

    @property
    def size(self):
        """Property returns the number of elements in the queue.
            
            Returns:
                int: length of the queue
        """
        return(len(self.elements))
    
    @property
    def first(self):
        """Property returns the first element in the queue.

            Does not remove the returned item from the queue.
            
            Returns:
                dict: the first element in the queue
        """

        if self.size>=1:
            return self.elements[0]

    @property
    def last(self):   
        """Property returns the last element in the queue.

            Does not remove the returned item from the queue.
            
            Returns:
                dict: the last element in the queue
        """

        if self.size>=1:
            return self.elements[-1]

    @property
    def is_empty(self):
        """Property returns if the queue is empty.
            
            Returns:
                bool: the boolean representing if the queue is empty
        """
        return (self.size == 0)
    
    @property
    def byte_size(self):
        """Property returns the size in bytes of the queue.

            The size is the length of the JSON string representation of the queue elements.
            
            Returns:
                int: the size of the string representation of the elements in the queue
        """
        if not self.is_empty:
            return(len(json.dumps(self.elements,default=str)))
        else:
            return(0)


# Observe HTTP Sender Class
class ObserveHttpSender:
    """
    This is a class for posting JSON dictionary data to the Observe HTTP Endpoint.
      
   Arguments:
        OBSERVE_URL -- The Observe customer URL - required (Example: https://154418444508.observeinc.com/)
        OBSERVE_TOKEN -- The configured Datastream API Token - required

    Functions:
        check_connectivity() returns:(bool) - returns if configured Observe API instance is reachable
        post_observation(dict) returns(none) - posts JSON dictionary to configured Observe HTTP API Endpoint
        flush() returns(none) - required call before exiting your code to flush any remaining batched data
        set_pop_empty_fields(bool) - returns(none) - accepts bool value to control if empty/null fields are removed. Default is True.
        get_pop_empty_fields() - returns(bool) - displays current value controlling removing empty/null fields
        set_payload_json_format(bool) - returns(none) - accepts bool value to control payload format is application/json (True) or text/plain (False). Default is True.
        get_payload_json_format() - returns(bool) - displays current value controlling removing empty/null fields
        set_post_path(string) - returns(none) - accepts string value optional post path segment
        get_post_path() - returns(string) - displays current optional post path value
        set_concurrent_post_limit(int) - returns(none) - accepts value 1-5. Defaults to 5.
        get_concurrent_post_limit() - returns(int) - displays current concurrent http post limit
        set_post_max_byte_size(int) - returns(none) - accepts value 4000-10000. Defaults to 4000.
        get_post_max_byte_size() - returns(int) - displays current http max bytes post size.
 
    
    Example Initialization:
        from observe_http_sender import ObserveHttpSender 
        observer = ObserveHttpSender($OBSERVE_CUSTOMER$,$OBSERVE_DOMAIN$,$OBSERVE_TOKEN$)
    """

    # ASync Web Get Method
    async def _http_get_task(self,work_queue):

        # Use JitteryRetry which is exponential with some randomness.
        retry_options = JitterRetry(attempts=self.http_retries,statuses=self.retry_http_status_codes)

        async with ClientSession() as session:
            retry_client = RetryClient(session,self.http_raise_for_status)

            while not work_queue.empty():
                try:
                    url = await work_queue.get()
                    async with retry_client.get(url,headers=self.observer_headers,retry_options=retry_options) as response:
                        await response.text()   
                    await retry_client.close()
                    return(response.status,response.reason) 
                except Exception as e:
                    self.log.exception(e)

    # ASync Web Post Method
    async def _http_post_task(self,url,work_queue):

       # Use JitteryRetry which is exponential with some randomness.
        retry_options = JitterRetry(attempts=self.http_retries,statuses=self.retry_http_status_codes)

        async with ClientSession() as session:
            retry_client = RetryClient(session,self.http_raise_for_status)

            while not work_queue.empty():
                try:
                    payload = await work_queue.get()
                    if isinstance(payload, list) and len(payload) == 0:
                        continue
                    
                    self.log.debug("Payload JSON Mode:{0}".format(self._payload_mode_json))
                   
                    if self._payload_mode_json:
                         # If True payload is expected to be form application/json
                        payload_string = gzip.compress(json.dumps(payload).encode('utf-8'))
                    else:
                         # If False payload is expected to be form text/plain
                        payload_string = ''
                        for row in payload:
                            payload_string = payload_string+'{0}\n'.format(row)
                        
                    if payload_string:
                        async with retry_client.post(url,headers=self.observer_headers,retry_options=retry_options,data=payload_string) as response:
                            await response.text()               
                        await retry_client.close()
                        if response.status == 400:
                            raise(Exception(response.text))
                except Exception as e:
                    self.log.exception(e)

        return()

    def __init__(self,obsv_url,token):
        """
        The constructor for the ObserveHttpSender class.
  
        Parameters:
            obsv_url (string): Required Observe Customer URL.
            token (string): Required the API Token for the datastream to receive the data.
        """

        self.log = logging.getLogger(u"OBSERVER_HTTP")
        self.log.setLevel(logging.INFO)

        url_regex_pattern = "(?i)https:\/\/(?P<customer_id>\d+)\.(?P<obsv_domain>.+)\.com"

        self.customer_id = re.search(url_regex_pattern, obsv_url).group("customer_id")
        self.observer_instance = re.search(url_regex_pattern, obsv_url).group("obsv_domain")
        self.auth_token = token
  
        if self.customer_id is None:
            raise(Exception("Observer Customer ID is missing."))
        if self.auth_token is None:
            raise(Exception("Observer Datastream API Token is missing."))
        if self.observer_instance is None:
            raise(Exception("Observer Instance Domain is missing."))

        # Set pop empty fields to default True
        self._pop_empty_fields = True

        # Set payload application/json mode
        self._payload_mode_json = True

        # Set optional post path to default /python/default. This appends to the post URL 
        self._post_path = "/python/default"

        # Set HTTP Controls
        self.http_raise_for_status = False
        self.http_retries = 3

        # Set Default batch max size for max bytes for the HTTP Endpoint.
        # Auto flush will occur if next event payload will exceed limit.
        self.max_byte_length = 4000

        # Number of "threads" used to send events to the endpoint (max concurrency).
        self.concurrent_post_limit = 5

        # Create initial queue of payloads.
        self.payload_queue = _ObserveQueue()

        # Create queue of combined payloads for http batch observation posts.
        self.post_queue = _ObserveQueue()

        # Create async queue for http post.
        self.work_queue = asyncio.Queue(maxsize=self.concurrent_post_limit)

        self.log.info("Observer Ready: Customer=%s Instance=%s",self.customer_id, self.observer_instance)

    def __str__(self):
        """Outputs the information about the Observe HTTP Receiver Instance.

            Parameters:
               none
            Returns:
                str: string of the attributes of the configured Observer HTTP receiver.
        """
        return "Observer: Customer={0} Instance={1} Reachable={2} PopEmptyFields={3} PayloadModeJSON={4} ConcurrentPostLimit={5} PostPath={6}".format(self.customer_id, self.observer_instance,self.check_connectivity(),self._pop_empty_fields,self._payload_mode_json,self.concurrent_post_limit,self._post_path)
  
    @property 
    def retry_http_status_codes(self):
        """Property returns HTTP status codes to retry.

            Codes to retry: [408, 500, 502, 503, 504]

            Returns:
                dict: list of http codes to force retry for.
        
        Notes:
            https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
        """

        status_codes = {408}    # 408 Request Timeout
        status_codes.add(429)   # 429 Too Many Retries
        status_codes.add(500)   # 500 Internal Server Error
        status_codes.add(502)   # 502 Bad Gateway
        status_codes.add(503)   # 503 Service Unavailable
        status_codes.add(504)   # 504 Gateway Timeout
    
        return(status_codes)
    
    @property 
    def observer_post_url(self):
        """Property returns Observe API HTTP Endpoint Post URL.
            
            Returns:
                string: formed URL for the Observe API HTTP Post Data endpoint.
        """
        # Check if option path set. Append if present. 
        if self._post_path is None:
            url = "https://%s.collect.%s.com/v1/http" % (self.customer_id, self.observer_instance)
        else:
            url = "https://%s.collect.%s.com/v1/http%s" % (self.customer_id, self.observer_instance,self._post_path)

        return(url)
    
    @property 
    def observer_health_url(self):
        """Property returns Observe API HTTP Health Check URL.
                  
            Returns:
                string: formed URL for the Observe API heath check endpoint.
        """

        url = "https://%s.collect.%s.com/v1/health" % (self.customer_id, self.observer_instance)
        return(url)

    @property
    def observer_headers(self):
        """Property returns the required Observe HTTP Headers."""

        headers = dict()
        headers["Authorization"] = "Bearer %s" % (self.auth_token)
        headers["Content-Encoding"] = "gzip"
        if self._payload_mode_json:
            headers["Content-Type"] = "application/json"
        else:
            headers["Content-Type"] = "text/plain"
        headers["User-Agent"] = "ObserveInc-http-sender/1.0 (Python)"
        return (headers)

    def get_pop_empty_fields(self):
        """Get pop empty fields mode (bool).
        
            Default value is True to save data ingestion cost.

            Parameters:
                none
            Returns:
                bool: True/False control if empty fields are removed from payloads.
        """

        return (self._pop_empty_fields)
    
    def set_post_path(self,value=None):
        """Set optional post path segment value (string).

        This appends the user specified path segment to the HTTP post url. It becomes a `path` value in the `EXTRA` field in the Datastream.
        The post path option is a useful way to group data.

        Parameters:
                value (string): Expected format example `/orca/alerts`
        Returns:
                none

        """

        self._post_path = value
        self.log.info("Observer Post Path Set: post_path={0}".format(value))

    def get_post_path(self):
        """Get the post path segment value (string).
        
            Default value is None
            Value: `/orca/alerts`

            Parameters:
                none
            Returns:
                string: value of optional post path
                
        """

        return (self._payload_mode_json)

    def set_pop_empty_fields(self,value=True):
        """Set pop empty fields mode (bool).

        This mode only works for payload mode (True) application/json. It is ignored for payload mode (False) text/plain.

        Parameters:
                value (bool): Sets if empty/null fields are removed from the payload before posting.
        Returns:
                none

        """

        self._pop_empty_fields = value
        self.log.info("Observer Mode Set: pop_empty_fields={0}".format(value))

    def get_concurrent_post_limit(self):
        """Get payload plain JSON mode (bool).
        
            Default value is True - payload is expected to be a application/json dict.
            Value: False - payload is expected to be text/plain.

            Parameters:
                none
            Returns:
                bool: True/False control if payload is plain JSON dict.
                
        """

        return (self.concurrent_post_limit)
    
    def set_concurrent_post_limit(self,value=5):
        """Set Post Max Byte Size (int).

        Parameters:
                value (int): Sets the max byte size per HTTP post

        Returns:
                none

        """

        if value <= 1:
            value = 1
        if value >= 5:
            value = 5

        self.concurrent_post_limit = value

        self.log.info("Observer Post Max Concurrent limit: concurrent_post_limit={0}".format(value))

    def get_post_max_byte_size(self):
        """Get payload plain JSON mode (bool).
        
            Default value is True - payload is expected to be a application/json dict.
            Value: False - payload is expected to be text/plain.

            Parameters:
                none
            Returns:
                bool: True/False control if payload is plain JSON dict.
                
        """

        return (self.max_byte_size)
    
    def set_post_max_byte_size(self,value=4000):
        """Set Post Max Byte Size (int).

        Parameters:
                value (int): Sets the max byte size per HTTP post

        Returns:
                none

        """

        if value <= 4000:
            value = 4000
        if value >= 10000:
            value = 10000

        self.max_byte_size = value

        self.log.info("Observer Post Max Byte Size: max_byte_size={0}".format(value))
    
    def get_payload_json_format(self):
        """Get Post Max Byte Size (int).
        
            Value: int - valid range: 4000-10000

            Parameters:
                none
            Returns:
                int: max_byte_size
                
        """

        return (self._payload_mode_json)
    
    def set_payload_json_format(self,value=True):
        """Set payload plain JSON mode (bool).

        Parameters:
                value (bool): Sets if empty/null fields are removed from the payload before posting.
                * Value: True - payload is expected to be a application/json dict.
                * Value: False - payload is expected to be text/plain.
        Returns:
                none

        """

        self._payload_mode_json = value
        self.log.info("Observer Mode Set: payload_mode_json={0}".format(value))

    def check_connectivity(self):
        """Checks connectivity to the Observe API.

        Returns:
            bool: Boolean result of if API is reachable
        Notes:
            method will warn on server health codes
        """

        is_available = asyncio.run(self._check_connectivity())

        return(is_available)

    async def _check_connectivity(self):
        """Private ASYNC function to check connectivity to the Observe API.

        Reference:
            https://developer.observeinc.com/
            api endpoint: v1/health

        Returns:
            bool: Boolean result of if API is reachable
        Notes:
            Method will log warnings on server health codes [500,503].
            Internal Method.
        """

        self.log.info("Checking Observer reachability. Customer=%s Instance=%s",self.customer_id, self.observer_instance)

        response = dict() 
        observer_reachable = False
        ACCEPTABLE_STATUS_CODES = [200]
        AUTHENTICATION_ERROR_STATUS_CODES = [400,401,403]
        HEATH_WARNING_STATUS_CODES = [500,503]

        try:

            work_item = dict()
            work_item["url"] = self.observer_health_url

            await self.work_queue.put(self.observer_health_url)
            response = await asyncio.gather(
                asyncio.create_task(self._http_get_task(self.work_queue)),
                )

            response_status_code = "unknown"
            response_text = ""

            if response[0] is None:
                raise(Exception("Unreachable"))
            else:
                try:
                    response_status_code, response_text = response[0]
                except:
                    raise(Exception("Unreachable."))

            if response_status_code==200:
                self.log.info("Observer is reachable. Customer=%s Instance=%s",self.customer_id, self.observer_instance)
                observer_reachable = True
            else:
                if response_status_code in ACCEPTABLE_STATUS_CODES:
                    self.log.info("Observer is reachable. Customer=%s Instance=%s",self.customer_id, self.observer_instance)
                    self.log.warning("Connectivity Check: Customer=%s Instance=%s http_status_code=%s http_message=%s",self.customer_id, self.observer_instance,response_status_code,response_text)
                    observer_reachable = True
                elif response_status_code in AUTHENTICATION_ERROR_STATUS_CODES:
                    self.log.warning("Observer has potential authentication issues. Customer=%s Instance=%s",self.customer_id, self.observer_instance)
                    self.log.error("Connectivity Check: Customer=%s Instance=%s http_status_code=%s http_message=%s",self.customer_id, self.observer_instance,response_status_code,response_text)
                elif response_status_code in HEATH_WARNING_STATUS_CODES:
                    self.log.warning("Observer has potential health issues. Customer=%s Instance=%s",self.customer_id, self.observer_instance)
                    self.log.error("Connectivity Check: Customer=%s Instance=%s http_status_code=%s http_message=%s",self.customer_id, self.observer_instance,response_status_code,response_text)
                else:
                    self.log.warning("Observer is unreachable. Customer=%s Instance=%s",self.customer_id, self.observer_instance)
                    self.log.error("HTTP status_code=%s message=%s Customer=%s Instance=%s",self.customer_id, self.observer_instance, response_status_code,response_text)
        except Exception as e:
            self.log.warn("Observer is unreachable. Customer=%s Instance=%s",self.customer_id, self.observer_instance)
            self.log.exception(e)

        return (observer_reachable)

    async def _post_batch(self):
        """Asyncronously posts the accumulated payloads to Observe.

            Parameters:
                none
            Returns:
                none
            Notes:
                Internal Method.
        """

        if self.post_queue.is_empty:
            self.log.debug("Batch Post: No Payloads to Post.")
            return()

        self.log.debug("Batch Post: Posting to HTTP Endpoint")

        batch_size = self.post_queue.size
        for x in range(batch_size):
            try:
                await self.work_queue.put(self.post_queue.dequeue())
            except Exception as e:
                self.log.exception(e)

        post_tasks = list()
        for x in range(batch_size):
            try:
                post_tasks.append(asyncio.create_task(self._http_post_task(self.observer_post_url,self.work_queue)))
            except Exception as e:
                self.log.exception(e)

        if post_tasks:
            try:
                await asyncio.gather(*post_tasks)
            except Exception as e:
                self.log.exception(e)
   
        return()
    
    def flush(self):
        """Flushes the remaining payloads that were not auto-batch posted to Observe.

            Parameters:
                none
            Returns:
                none
            Notes:
                Always call this method before exiting your code to send any partial batched data.
        """
    
        if not self.payload_queue.is_empty:
            self.log.debug("Final Flush: Posting %s",str(self.payload_queue.size))
            try:
                self.post_queue.enqueue([self.payload_queue.dequeue() for x in range(self.payload_queue.size)])
                if not self.post_queue.is_empty:
                    asyncio.run(self._post_batch())
            except Exception as e:
                self.log.exception(e)

        return()

    def post_observation(self,payload):
        """Places the JSON payload into a batch queue for optimal HTTP Posting to Observe.

        Parameters:
            payload (dict): The JSON dictionary of the data payload.
        Returns:
            none
        Notes:
           Queue will auto flush as needed.
        """

        # Pop empty fields if feature enabled and Payload mode JSON is True.
        if self._pop_empty_fields and self._payload_mode_json:
            payload = {k:payload.get(k) for k,v in payload.items() if v}

        # Convert payload to string of json.
        payloadString = json.dumps(payload,default=str)
        
        # Measure length of the payload string.
        payloadLength = len(payloadString)

        # Check if next payload will exceed limits, post current batch and set next batch to the new payload that exceeded the limit.
        if ((self.payload_queue.byte_size+payloadLength) > self.max_byte_length or (self.max_byte_length - self.payload_queue.byte_size) < payloadLength):
            
            # Move batch to post queue.
            self.post_queue.enqueue([self.payload_queue.dequeue() for x in range(self.payload_queue.size)])

            # If self.concurrent_post_limit batches have accumulated post flush them to Observe.
            if self.post_queue.size >= self.concurrent_post_limit:
                self.log.debug("Auto Flush: Posting the Batch.")
                asyncio.run(self._post_batch())

        # Add new payload to batch accumulation.
        self.payload_queue.enqueue(payload)
