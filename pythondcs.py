from datetime import datetime, date, time, timedelta, timezone
from threading import RLock
import requests, logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

try:
    import ijson, gzip
    IJSONAVAILABLE = True
except ImportError:
    IJSONAVAILABLE = False

class DCSSession:
    """
    The DCSSession class can be used to login and interface with a
    Coherent Research DCS v3+ system using it's built in json based
    web API. You can then download data using class methods which map
    to the DCS API but with some convenient data conversions.
    
    The official API methods supported are detailed here, but all of
    the semantics of the API, including the login cookies are
    handled by the class and its methods:
    
    https://www.coherent-research.co.uk/support/extras/dcsapispec/
    """
    if hasattr(datetime,"fromisoformat"):
        @staticmethod
        def _fromisoformat(isostr):
            """Converts ISO formatted datetime strings to datetime objects
            using built in methods but with added support for "Z" timezones.
            Generally supported by Python 3.7+"""
            return datetime.fromisoformat(isostr.replace('Z', '+00:00', 1))
    else:
        @staticmethod
        def _fromisoformat(isostr):
            """Converts ISO formatted datetime strings to datetime objects
            using string manipulations for Python 3.6 and lower where the built in
            "fromisoformat" method isn't available. This function is slower but
            much faster than "strptime", but not as forgiving if the strings are
            incorrectly formatted.
            Expected format: YYYY*MM*DD*HH*MM*SS[.f][Z|[{+|-}HH*MM]] where * can
            match any single character, and "f" can be up to 6 digits"""
            isostr = isostr.replace('Z', '+00:00', 1)
            strlen = len(isostr)
            tz_pos = (isostr.find("+",19)+1 or isostr.find("-",19)+1 or strlen+1)-1
            if tz_pos == strlen:
                tz = None
            else:
                tz_parts = (
                    int(isostr[tz_pos+1:tz_pos+3]),
                    int(isostr[tz_pos+4:tz_pos+6]),
                )
                if not any(tz_parts):
                    tz = timezone.utc
                else:
                    tz = timezone(
                        (1 if isostr[tz_pos] == "+" else -1)
                        * timedelta(
                            hours=tz_parts[0],
                            minutes=tz_parts[1],
                        )
                    )
            return datetime(
                int(isostr[0:4]),   # Year
                int(isostr[5:7]),   # Month
                int(isostr[8:10]),  # Day
                int(isostr[11:13]), # Hour
                int(isostr[14:16]), # Minute
                int(isostr[17:19]), # Second
                (
                    int(isostr[20:tz_pos].ljust(6,"0"))
                    if strlen > 19 and isostr[19] == "."
                    else 0
                ),  # Microsecond
                tz, # Timezone
            )
    @staticmethod
    def _iterjson_reads(reply):
        """Takes the http response and decodes the json payload by streaming it,
        decompressing it if required, and decoding it into an ijson iterator as
        elements are consumed. Convert timestamps to datetime objects and ensure
        all Decimals are converted back to native floats"""
        # Prepare to decompress on the fly if required
        if reply.headers["content-encoding"] in ("gzip", "deflate"):
            raw = gzip.open(reply.raw)
        else:
            raw = reply.raw
        n=0
        for item in ijson.items(raw, 'item', use_float=True):
            # Convert to datetimes and floats where needed
            item["startTime"] = DCSSession._fromisoformat(item["startTime"])
            yield item  # Yield each item one at a time
            n+=1
        logging.info(f"All {n} readings retreived")
    @staticmethod
    def _json_reads(reply):
        """Takes the http response and decodes the json payload as one object
        Convert timestamps to datetime objects"""
        results = reply.json()
        for item in results:
            # Convert to datetimes
            item["startTime"] = DCSSession._fromisoformat(item["startTime"])
        logging.info(f"All {len(results)} readings retreived")
        return results
    def __enter__(self):
        """Context Manager Enter"""
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        """Context Manager Exit"""
        return None
    def __init__(self, rooturl, username=None, password=None):
        """
        Creates a DCS session with the rooturl and logs in if credentials are
        provided. Returns a DCSSession object for future use.
        """
        # Lock used to limit sessions to 1 transaction at a time to avoid
        # accidental flooding of the server if used within multithreaded loops
        self.lock = RLock()
        self.timeout = (3.05,120)   # Connect and Read timeouts
        self.s = requests.Session()
        self.s.stream = True
        # Attempt up to 5 increasingly delayed retries for recoverable errors
        self.s.mount(rooturl, HTTPAdapter(
            max_retries=Retry(  # Delays between retries: 0, 1, 2, 4, 8 seconds
                total=5, backoff_factor=0.5, status_forcelist=[ 502, 503, 504 ]
            ) ))
        self.rooturl = rooturl + "/api"
        self.username = None
        self.role = None
        if None not in (username, password):
            self.login(username, password)
        else:
            logging.warning("Incomplete credentials given; Please use the login method")
    def login(self, username, password):
        """
        Logs in to DCS server and returns a logged in session for future use.
        An authentication token (cookie) will be stored for this session.
        Must be provided with a username and password.
        """
        subpath = "/account/login/"
        s  = self.s
        if len(s.cookies) > 0 or self.username is not None:
            self.logout()
        try:
            with self.lock:
                reply = s.post(self.rooturl+subpath,
                    json={"username":username,"password":password},
                    timeout=self.timeout
                    )
            reply.raise_for_status()
            result = reply.json()
            self.username = result['username']
            self.role = result['role']
            logging.info(f"Successfully logged in to DCS as '{self.username}' with {self.role} privileges")
        except requests.exceptions.HTTPError as err:
            r = err.response
            logging.error(f"{r.status_code}: {r.reason}, '{r.text}'\n{r.url}")
        self.s = s
    def logout(self):
        """Logs out of the current DCS session."""
        subpath = "/account/logout/"
        with self.lock:
            self.s.post(self.rooturl+subpath, timeout=self.timeout)
        self.username = None
        self.role = None
        logging.info("Logged Out of DCS")
    def __del__(self):
        """Logs out of DCS upon deletion and garbage collection of this object"""
        if self.username is not None:
          self.logout()
    def get_meters(self, id=None):
        """
        Returns a list of all meters defined in DCS, or the one with the given
        id. Returned object will include the registers for each meter.
        This is a direct equivelent to the "Get meters" API call.
        """
        subpath = "/Meters/"
        id = str(int(id)) if id is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+id, timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def get_vms(self, id=None):
        """
        Returns a list of all virtual meters defined in DCS, or the one
        with the given id. Each virtual meter object will contain 1 or more
        register alias objects.
        This is a direct equivelent to the "Get virtual meters" API call.
        """
        subpath = "/VirtualMeters/"
        id = str(int(id)) if id is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+id, timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def get_readings(self, id, isVirtual=False, start=None, end=None,
        decimalPlaces=15, calibrated=True, interpolated=True, useLocalTime=False,
        integrationPeriod="halfhour", source="automatic", iterator=False):
        """
        Given the parameters required by DCS, and a "isVirtual" flag, provides
        the readings as a dictionaries nested within a list or iterator.
        Depending on the isVirtual flag, this is equivelent to the API calls for
        "Get register readings" and "Get virtual meter readings".
        parameters are as required by DCS:
        - "id" - of the register or virtual meter (Required)
        - "isVirtual" - True if id refers to a virtual meter ID, otherwise
            False for a register. (Optional, default False)
        - "start" - a datetime or date object (Optional, default omitted)
        - "end" - a datetime or date object (Optional, default ommitted)
        - "decimalPlaces" - of the returned data 0-15 (Optional, default 15)
        - "calibrated" - totalValues to be calibrated (Optional, default True)
        - "interpolated" - gaps to be linearly filled (Optional, default True)
        - "useLocalTime" - for timestamps (Optional, default False for UTC)
        - "integrationPeriod" - Defaults to "halfHour", or "hour",
            "day", "week", "month" (Optional)
        - "source" - Defaults to "automatic", or "manual" "merged" (Optional)
        - "iterator" - False to return a single potentially larget nested list, or
            True to return an iterator which strems and yields each item.
            if the ijson module is not available, this option does nothing and
            is always equivelent to False.
        Using an iterator (iterator=True with ijson module) will yield one
        reading at a time which may be more memory efficient for large data sets.
        If memory usage is not a concern or you need to retain the data, then
        the iterator=False (default) will simply return one single list of reads.
        In both cases, each element of the list or iterator will consist of a
        dictionary as received by the server with all numbers as ints or
        floats, and dates as timezone aware datetime objects.
        """
        dataparams = {
            'start'             : start,  # can be YYYY-MM-HHTHH:MM:SS
            'end'               : end,  # If None, it wont get parsed to the url
            'decimalPlaces'     : decimalPlaces,  # 0-15 inclusive
            'calibrated'        : calibrated,
            'interpolated'      : interpolated,
            'useLocalTime'      : useLocalTime,  # Keep False for UTC
            'integrationPeriod' : integrationPeriod,  # Enum:"halfHour" "hour" "day" "week" "month"
            'source'            : source  # Enum:"automatic" "manual" "merged"
        }
        if isVirtual:   # Get correct key and url ready
            subpath = "/VirtualMeterReadings/list/"
            dataparams["virtualMeterId"] = int(id)
        else:
            subpath = "/registerReadings/list/"
            dataparams["registerId"] = int(id)
        # Convert to ISO strings assuming datetimes or dates were given
        if isinstance(dataparams["start"], date):
            dataparams["start"] = dataparams["start"].isoformat()
        elif dataparams["start"] is None:
            dataparams["start"] = date.today().isoformat()
        if isinstance(dataparams["end"], date):
            dataparams["end"] = dataparams["end"].isoformat()
        # Actually get the data and stream it into the json iterative decoder
        with self.lock:
            # Stream the response into json decoder for efficiency
            reply = self.s.get(self.rooturl+subpath, params=dataparams,
                timeout=self.timeout)
            reply.raise_for_status()    # Raise exception if not 2xx status
            logging.info(f"Got readings for {'VM' if isVirtual else 'R'}{id}, server response time: {reply.elapsed.total_seconds()}s")
        if iterator and IJSONAVAILABLE:
            # The user must ask for an iterator AND the module must be available
            return DCSSession._iterjson_reads(reply)
        else:
            return DCSSession._json_reads(reply)


class DcsWebApi:
    """
    The DcsWebApi class can be used to login and interface with a
    Coherent Research DCS system using it's built in json based public
    web API. You can then download data using class methods which map
    to the DCS API but with some convenient python data conversions.
    
    The class can be used in a unauthenticated or authenticated regime.

    The official API methods supported are detailed here, but all of
    the semantics of the API, including the login cookies are
    handled by the class and its methods:
    
    https://github.com/coherent-research/dcs-documentation/blob/master/DcsPublicApiDescription.md
    """
    if hasattr(datetime,"fromisoformat"):
        @staticmethod
        def _fromisoformat(isostr):
            """Converts ISO formatted datetime strings to datetime objects
            using built in methods but with added support for "Z" timezones.
            Generally supported by Python 3.7+"""
            return datetime.fromisoformat(isostr.replace('Z', '+00:00', 1))
    else:
        @staticmethod
        def _fromisoformat(isostr):
            """Converts ISO formatted datetime strings to datetime objects
            using string manipulations for Python 3.6 and lower where the built in
            "fromisoformat" method isn't available. This function is slower but
            much faster than "strptime", but not as forgiving if the strings are
            incorrectly formatted.
            Expected format: YYYY*MM*DD*HH*MM*SS[.f][Z|[{+|-}HH*MM]] where * can
            match any single character, and "f" can be up to 6 digits"""
            isostr = isostr.replace('Z', '+00:00', 1)
            strlen = len(isostr)
            tz_pos = (isostr.find("+",19)+1 or isostr.find("-",19)+1 or strlen+1)-1
            if tz_pos == strlen:
                tz = None
            else:
                tz_parts = (
                    int(isostr[tz_pos+1:tz_pos+3]),
                    int(isostr[tz_pos+4:tz_pos+6]),
                )
                if not any(tz_parts):
                    tz = timezone.utc
                else:
                    tz = timezone(
                        (1 if isostr[tz_pos] == "+" else -1)
                        * timedelta(
                            hours=tz_parts[0],
                            minutes=tz_parts[1],
                        )
                    )
            return datetime(
                int(isostr[0:4]),   # Year
                int(isostr[5:7]),   # Month
                int(isostr[8:10]),  # Day
                int(isostr[11:13]), # Hour
                int(isostr[14:16]), # Minute
                int(isostr[17:19]), # Second
                (
                    int(isostr[20:tz_pos].ljust(6,"0"))
                    if strlen > 19 and isostr[19] == "."
                    else 0
                ),  # Microsecond
                tz, # Timezone
            )
    @classmethod
    def _readingsgenerator(cls, parse_events):
        """Provides an iterator of element from the 'readings' object in 'standard' format.
        Converts timestamps to datetime objects and values to floats"""
        n=0
        for item in ijson.items(parse_events,"readings.item", use_float=True):
            item["timestamp"] = cls._fromisoformat(item["timestamp"])
            item["value"] = float(item["value"])
            yield item
            n+=1
        logging.info(f"All {n} readings retreived")
    @classmethod
    def _iterjson_reads(cls, reply):
        """Takes the http response and decodes the json payload by streaming it,
        decompressing it if required, and decoding it into a dictionary with an
        iterator in place of the 'readings' using the _readingsgenerator method.

        Note that any items appearing after the 'readings' object will be lost."""
        if "content-encoding" in reply.headers and reply.headers["content-encoding"] in ("gzip", "deflate"):
            raw = gzip.open(reply.raw)
        else:
            raw = reply.raw
        results = dict()
        parse_events = ijson.parse(raw)
        while True:
            path, name, value = next(parse_events)
            if name == "map_key" and value != "readings":
                path, name, value = next(parse_events)
                results[path] = cls._fromisoformat(value) if path in ("startTime", "endTime") else value
            elif name == "map_key" and value == "readings":
                break
        results["readings"] = cls._readingsgenerator(parse_events)
        return results
    @classmethod
    def _json_reads(cls, reply):
        """Takes the http response and decodes the json payload as one object
        converting timestamps to datetime objects and all readng values to floats"""
        results = reply.json()
        results["startTime"] = cls._fromisoformat(results["startTime"])
        results["endTime"] = cls._fromisoformat(results["endTime"])
        for item in results["readings"]:
            # Convert to datetimes
            item["timestamp"] = cls._fromisoformat(item["timestamp"])
            item["value"] = float(item["value"])
        logging.info(f"All {len(results['readings'])} readings retreived")
        return results
    @classmethod
    def _raise_for_status(cls, reply):
        """Raises :class:`HTTPError`, if one occurred with the human readable error message included"""
        if 400 <= reply.status_code < 500:
            http_error_msg = f"\n{reply.status_code} Client Error: {reply.reason} for url: {reply.url}"
            try:
                payload = reply.json()
                http_error_msg += f"\n{ chr(10).join( ' : '.join(item) for item in payload.items() ) }"
            except requests.models.complexjson.JSONDecodeError:
                pass
            raise requests.exceptions.HTTPError(http_error_msg, response=reply)
        else:
            reply.raise_for_status()    # Fallback on the requests library exception
    def __enter__(self):
        """Context Manager Enter"""
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        """Context Manager Exit"""
        return None
    def __init__(self, rooturl, username=None, password=None):
        """
        Creates a Public API Session object with the rooturl and logs in if
        credentials are provided. Returns this object for future use.
        """
        # Lock used to limit sessions to 1 transaction at a time to avoid
        # accidental flooding of the server if used within multithreaded loops
        self.lock = RLock()
        self.timeout = (3.05,120)   # Connect and Read timeouts
        self.s = requests.Session()
        self.s.stream = True
        # Attempt up to 5 increasingly delayed retries for recoverable errors
        self.s.mount(rooturl, HTTPAdapter(
            max_retries=Retry(  # Delays between retries: 0, 1, 2, 4, 8 seconds
                total=5, backoff_factor=0.5, status_forcelist=[ 502, 503, 504 ]
            ) ))
        self.rooturl = rooturl.rstrip(" /")
        self.username = None
        self.role = None
        if None not in (username, password):
            self.signin(username, password)
        else:
            logging.warning("Incomplete credentials given - Unauthenticated mode will be used; Please use the signin method for Authenticated mode")
    def status(self):
        """
        Gets the Status of the API
        """
        subpath = "/status"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath, timeout=self.timeout)
        self._raise_for_status(reply)
        return reply.json()
    def signin(self, username, password):
        """
        Signs in to the DCS server for the current Session object.
        An authentication token (cookie) will be stored for this session.
        Must be provided with a username and password.
        Returns None.
        """
        subpath = "/authentication/signin"
        if len(self.s.cookies) > 0 or self.username is not None:
            self.signout()
        try:
            with self.lock:
                reply = self.s.post(self.rooturl+subpath,
                    json={"username":username,"password":password},
                    timeout=self.timeout
                    )
            self._raise_for_status(reply)
            result = reply.json()
            self.username = result['username']
            self.role = result['role']
            logging.info(f"Successfully signed in to DCS as '{self.username}' with {self.role} privileges")
        except requests.exceptions.HTTPError as err:
            r = err.response
            logging.error(f"{r.status_code}: {r.reason}, '{r.text}'\n{r.url}")
    def signout(self):
        """
        Signs out of the current session and expires the authentication cookie.
        Returns None.
        """
        subpath = "/authentication/signout"
        with self.lock:
            self.s.post(self.rooturl+subpath, timeout=self.timeout)
        self.username = None
        self.role = None
        logging.info("Signed Out of DCS")
    def __del__(self):
        """Signs out of DCS upon deletion and garbage collection of this object"""
        if self.username is not None:#
            self.signout()
    def meters(self, iterator=False):
        """
        Returns a list of available meters defined in DCS including the registers for each meter.
        What is available depends on permissions.
        Structure is approximately a List containing Dicts for each meter
        where Registers are another List containing Dicts for each register.

        """
        subpath = "/public/meters" if self.username is None else "/meters"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath, timeout=self.timeout)
        self._raise_for_status(reply)
        if iterator and IJSONAVAILABLE:
            # The user must ask for an iterator AND the module must be available
            if "content-encoding" in reply.headers and reply.headers["content-encoding"] in ("gzip", "deflate"):
                raw = gzip.open(reply.raw)
            else:
                raw = reply.raw
            return ijson.items(raw,"item", use_float=True)
        else:
            return reply.json()
    def virtualmeters(self, iterator=False):
        """
        Returns a list of available virtual meters defined in DCS. Each virtual meter object will contain 1 or more register alias objects.
        What is available depends on permissions.
        Structure is approximately a List containing Dicts for each virtual meter
        where register aliases are another List containing Dict for each alias.
        """
        subpath = "/public/virtualMeters" if self.username is None else "/virtualMeters"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath, timeout=self.timeout)
        self._raise_for_status(reply)
        if iterator and IJSONAVAILABLE:
            # The user must ask for an iterator AND the module must be available
            if "content-encoding" in reply.headers and reply.headers["content-encoding"] in ("gzip", "deflate"):
                raw = gzip.open(reply.raw)
            else:
                raw = reply.raw
            return ijson.items(raw,"item", use_float=True)
        else:
            return reply.json()
    def readings(self, id, startTime=None, endTime=None, periodCount=None,
        calibrated=True, interpolated=True, periodType="halfhour", iterator=False):
        """
        Returns a list or iterator of readings for the specified register or virtual meter and timespan.
        Structure is approximately a Dict containing header information with a nested list of readings
        where readings are a list/iterator containing a Dict for each reading.

        If very large queries are needed, or server limits are met, use the 'largereadings' method.

        Using an iterator (iterator=True with ijson module) will yield one
        reading at a time which may be more memory efficient for large data sets
        but the values are not retained after consumption.
        If memory usage is not a concern or you need to retain the data, then
        the iterator=False (default) will simply return one single list of reads.
        In both cases, each element of the list or iterator will consist of a
        dictionary with values as floats and dates as timezone aware datetime objects.

        It is possible for the floats to represent positive and negative infinities or nan.

        Parameters are as required by DCS:
        - "id" - string of the register or virtual meter prepended by R or VM (Required)
        - "startTime" - datetime or date object (Optional, see note)
        - "endTime" - datetime or date object (Optional, see note)
        - "periodCount" - integer number of periodTypes (Optional, see note)
        - "calibrated" - boolean for whether values should be calibrated (Optional, default True)
        - "interpolated" - boolean for whether gaps should be linearly filled (Optional, default True)
        - "periodType" - string of "halfHour", or "hour",
            "day", "week", "month" (Optional, default halfhour)
        - "iterator" - False to return a single potentially larget nested list, or
            True to return an iterator which streams and yields each item.
            if the ijson module is not available, this option does nothing and
            is always equivelent to False.
        Note: The timespan covered by the request can be specified by including any 2 of
        startTime, endTime or periodCount. It is an error to specify anything other than 2.
        """
        if (startTime,endTime,periodCount).count(None) != 1:
            raise TypeError("Only two parameters are permitted from startTime, endTime and periodCount")
        dataparams = {
            'id'                : id,           # String, such as "R123" or "VM456"
            'format'            : "standard",   # Currently only "standard" is supported
            'startTime'         : startTime,    # if None, it wont get arsed to the url
            'endTime'           : endTime,      # If None, it wont get parsed to the url
            'periodCount'       : periodCount,  # If None, it wont get parsed to the url
            'calibrated'        : calibrated,   # Boolean
            'interpolated'      : interpolated, # Boolean
            'periodType'        : periodType,   # Enum: "halfHour" "hour" "day" "week" "month"
        }
        subpath = "/public/readings" if self.username is None else "/readings"
        # Convert to ISO strings assuming datetimes or dates were given
        if isinstance(dataparams["startTime"], datetime):
            dataparams["startTime"] = dataparams["startTime"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z", 1)
        elif isinstance(dataparams["startTime"], date):
            dataparams["startTime"] = dataparams["startTime"].isoformat() + "T00:00:00Z"
        if isinstance(dataparams["endTime"], datetime):
            dataparams["endTime"] = dataparams["endTime"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z", 1)
        elif isinstance(dataparams["endTime"], date):
            dataparams["endTime"] = dataparams["endTime"].isoformat() + "T00:00:00Z"
        # Actually get the data and stream it into the json iterative decoder
        with self.lock:
            # Stream the response into json decoder for efficiency
            reply = self.s.get(self.rooturl+subpath, params=dataparams,
                timeout=self.timeout)
        self._raise_for_status(reply)
        logging.info(f"Got readings for {id}, server response time: {reply.elapsed.total_seconds()}s")
        if iterator and IJSONAVAILABLE:
            # The user must ask for an iterator AND the module must be available
            return self._iterjson_reads(reply)
        else:
            return self._json_reads(reply)
    def largereadings(self, *args, maxwindow=timedelta(days=365), periodCount=None,
        startTime=None, endTime=None, iterator=False, periodType="halfHour", **kwargs):
        """
        Returns a list or iterator of readings for the specified register or virtual meter and timespan.
        Structure is approximately a Dict containing header information with a nested list of readings
        where readings are a list/iterator containing a Dict for each reading.

        A potentially very large query will be chunked into numerous smaller transactions
        using the 'readings' method based on a maxwindow size (a timedelta defaulting
        to about 12 months) and provides the result as if one single transaction was completed;
        either a very large list of readings or a single iterator over all readings from all
        constituent underlying transactions.

        This method utilises the simpler 'readings' method and aside from maxwindow,
        all other arguments are as per 'readings' and are passed through, however,
        a startTime and endTime must be provided explicitly (not using periodCount).

        Using an iterator (iterator=True with ijson module) will yield one
        reading at a time which may be more memory efficient for large data sets
        but the values are not retained after consumption.
        If memory usage is not a concern or you need to retain the data, then
        the iterator=False (default) will simply return one single list of reads.
        In both cases, each element of the list or iterator will consist of a
        dictionary with values as floats and dates as timezone aware datetime objects.

        It is possible for the floats to represent positive and negative infinities or nan.
        """
        periodTimedelta = {
                "halfhour"  : timedelta(minutes=30),
                "hour"      : timedelta(hours=1),
                "day"       : timedelta(days=1),
                "week"      : timedelta(days=7),
                "month"     : timedelta(days=31),
            }
        periodType = periodType.lower()
        ptd = periodTimedelta[periodType]
        if maxwindow < ptd:
            raise TypeError("Max window is smaller than the periodType")
        if None in (startTime, endTime):
            raise TypeError("You must explicitly provide a startTime and endTime if you want batching")
        if isinstance(startTime, date):
            startTime = datetime.combine(startTime, time(), timezone.utc)
        elif isinstance(startTime, datetime):
            startTime = startTime.astimezone(timezone.utc)
        else:
            raise TypeError("The startTime isn't a 'date' or 'datetime' instance")
        if isinstance(endTime, date):
            endTime = datetime.combine(endTime, time(), timezone.utc)
        elif isinstance(endTime, datetime):
            endTime = endTime.astimezone(timezone.utc)
        else:
            raise TypeError("The endTime isn't a 'date' or 'datetime' instance")
        if endTime < startTime: # Swap dates if reversed
            startTime, endTime = endTime, startTime
        if startTime == endTime:
            raise TypeError("The startTime and endTime are the same")
        elif (
            (periodType == "halfhour" and not ( # Check its a clean half hour
            endTime.microsecond == endTime.second == startTime.microsecond == startTime.second == 0 and
            startTime.minute in (0,30) and endTime.minute in (0,30)
            )) or
            (periodType == "hour" and not ( # Check its a clean hour
            endTime.microsecond == endTime.second == endTime.minute == startTime.microsecond == startTime.second == startTime.minute == 0
            )) or
            (periodType == "day" and not ( # Check its a clean day
            endTime.microsecond == endTime.second == endTime.minute == endTime.hour == startTime.microsecond == startTime.second == startTime.minute == startTime.hour == 0
            )) or
            (periodType == "week" and not ( # Check its a clean week starting on Monday and ending on Sunday
            endTime.microsecond == endTime.second == endTime.minute == endTime.hour == startTime.microsecond == startTime.second == startTime.minute == startTime.hour == 0 and
            (startTime.weekday(), endTime.weekday()) == (0,6)
            )) or
            (periodType == "month" and not ( # Check its a clean month starting on the 1st and ending on whatever the last day of the month is for that month and year
            endTime.microsecond == endTime.second == endTime.minute == endTime.hour == startTime.microsecond == startTime.second == startTime.minute == startTime.hour == 0 and
            startTime.day == 1 and endTime.day == ((endTime.replace(month=endTime.month+1, day=1) if endTime.month < 12 else endTime.replace(year=endTime.year+1, month=1, day=1)) - timedelta(days=1)).day
            )) ):
            raise TypeError("The startTime and endTime must be aligned with the periodType")
        maxwindow=abs(maxwindow)  # Strip negative durations and dont go too small
        if maxwindow < timedelta(days=1):
            maxwindow = timedelta(days=1)
        reqwindow = endTime - startTime # Requested window/duration
        logging.info(f"{reqwindow} requested and the maximum limit is {maxwindow}")
        if reqwindow <= maxwindow: # If the period is smaller than max, use directly
            logging.info("Only 1 transaction is needed")
            return self.readings(*args, startTime=startTime, endTime=endTime, periodType=periodType, iterator=iterator, **kwargs)
        else:   # If the period is larger than max, then break it down
            if periodType == "month":
                reqperiods = (endTime.year-startTime.year)*12 + (endTime.month-startTime.month) + 1 # Requested duration counted in months
            else:
                reqperiods = reqwindow // ptd # Requested duration in periods
            maxperiods = maxwindow // ptd # Maximum duration in periods
            d = 2   # Start by dividing into 2 intervals, since 1 was tested above
            while True:
                # Divide into incrementally more/smaller peices and check
                i, r = divmod(reqperiods, d)
                # Once the peices are small enough, stop.
                # If there is no remainder, take i, otherwise the remainders will
                # be added to other peices so add 1 and check that instead.
                if (i+1 if r else i) <= maxperiods: break
                d += 1
            # Make a list of HH sample sizes, with the remainders added onto the
            # first sets. Such as 11, 11, 10 for a total of 32.
            periodsBlocks = [i+1]*r + [i]*(d-r)
            logging.info(f"{len(periodsBlocks)} transactions will be used")
            Intervals=[]
            IntervalStart = startTime   # The first starttime is the original start
            if periodType == "month":
                for i in periodsBlocks:
                    # Add calculated number of half hours on to the start time
                    IntervalEnd = IntervalStart + i * ptd
                    IntervalEnd = IntervalEnd.replace(day=1) - timedelta(days=1)
                    # Define each sample window  and start the next one after the last
                    Intervals.append({"startTime":IntervalStart,"endTime":IntervalEnd})
                    IntervalStart = IntervalEnd + timedelta(days=1)
            else:
                for i in periodsBlocks:
                    # Add calculated number of half hours on to the start time
                    IntervalEnd = IntervalStart + i * ptd
                    # Define each sample window  and start the next one after the last
                    Intervals.append({"startTime":IntervalStart,"endTime":IntervalEnd})
                    IntervalStart = IntervalEnd
        if iterator:
            result = {
                "startTime" : Intervals[0]["startTime"],
                "endTime"   : Intervals[-1]["endTime"],
                }
            iterresults = ( self.readings(*args, periodType=periodType, iterator=iterator, periodCount=None, **chunk, **kwargs) for chunk in Intervals )
            firstchunk = next(iterresults)
            for item in firstchunk:
                if item not in ("startTime", "endTime", "readings"):
                    result[item] = firstchunk[item]
                elif item == "readings":
                    def concatreadings(firstone, otherones):
                        yield from firstone["readings"]
                        for chunk in iterresults:
                            yield from chunk["readings"]
                    result["readings"] = concatreadings(firstchunk,iterresults)
            return result
        else:
            # Gather results
            result = {"readings":[]}
            for chunk in Intervals:
                chunkresult = self.readings(*args, periodType=periodType, iterator=iterator, **chunk, **kwargs)
                for item in chunkresult:
                    if item not in result or item == "endTime":
                        result[item] = chunkresult[item]
                    elif item == "readings":
                        result["readings"].extend(chunkresult["readings"])
            return result
