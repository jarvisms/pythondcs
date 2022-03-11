# This is an Extention module which can be used in place of the standard module
# All features from the standard module will be inherited and added to
from pythondcs import *

def macint_to_hex(MacInt):
    """Converts integers to hex MAC address"""
    assert 0 <= MacInt <= 0xFFFFFFFFFFFF, "Integer out of range"
    return ":".join([format(MacInt,"012X")[x:x+2] for x in range(0,12,2)])

def machex_to_int(MacHex):
    """Converts hex MAC address to integers"""
    assert 12 <= len(MacHex) <= 17, "String of unexpected size"
    return int(MacHex.replace(":","").lower(), 16)

def get_meters_from_group(group, meters=[]):
    """
    Produces a flat list of meters from a possibly nested group.
    Given the output of DCSSession.get_meter_tree(), the result should be
    equivelent to that of DCSSession.get_meters() after both were sorted.
    This is useful for getting a flat list from a particular point in the tree.
    """
    if group["hasMeters"]:
        meters.extend(group["meters"])
    for subgroup in group["meterGroups"]:
        meters=get_meters_from_group(subgroup,meters)
    return meters

class DCSSession():
    """
    The DCSSession class can be used to login and interface with a
    Coherent Research DCS v3+ system using it's built in json based
    web API. You can then download data using class methods which map
    to the DCS API but with some convenient data conversions.
    
    This extention module builds upon the officially supported methods.
    Nothing defined here is officially documented or supported.
    Therefore it should be used at your own risk.
    
    Use the pythondcs standard module for the officially supported features
    which are also the most commonly required.
    
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
    def get_idcs(self, macAddress=None):
        """Get a detailed list of all IDCs known to the server
        or the one specified by macAddress (as an unsigned integer)"""
        subpath = "/Idcs/"
        macAddress = str(int(macAddress)) if macAddress is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+macAddress,
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def get_idc_settings(self, macAddress):
        """Retreive the IDC settings from the IDC with the given macAddress
        (as an unsigned integer)"""
        subpath = "/Idcs/settings/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(macAddress)),
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def update_idc_settings(self, macAddress, settings):
        """Update the IDC settings for the IDC with the given macAddress
        (as an unsigned integer). setting of the same form as get_idc_settings()
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised."""
        subpath = "/idcs/settings/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath,
                json={"macAddress":macAddress, "idcSettings":settings},
                timeout=self.timeout,
            )
        reply.raise_for_status()
    def get_modbus_devices_by_idc(self, macAddress):
        """Get details of all Modbus Devices under the IDC with the given
        macAddress (as an unsigned integer)"""
        subpath = "/ModbusDevices/byIdc/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(macAddress)),
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def get_modbus_device_by_id(self, id):
        """Get Modbus Device with the given id (as an unsigned integer)"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(id)),
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def update_modbus_device(self, device):
        """Updates a modbus device defined by 'device' with those parameters.
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised.
        Then returns the resulting modbus device (like get_modbus_device_by_id)"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath, json=device,
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def add_modbus_device(self, device):
        """Adds the modbus device defined by 'device' with those parameters.
        Parameters are 'address':<int>, 'description':<str>, 'serialNumber':<str>
        'deviceType':'pulseCounter'|'radioReceiver', 'macAddress':<int> 
        Then returns the resulting modbus device (like get_modbus_device_by_id)"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath, json=device,
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def command_modbus_device(self, id, command):
        """Executes 'command' on the modbus device with 'id'
        Only accepts commands: "testComms", "setup", "resetBuffer", and only
        Pulse Counters or Radio Receivers will accept "setup" or "resetBuffer"
        """
        subpath = "/ModbusDevices/command/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath,
                json={"id": int(id), "action":command}, timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def delete_modbus_device(self, id):
        """Deletes the modbus device with the given 'id'."""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.delete(self.rooturl+subpath+str(int(id)),
                timeout=self.timeout)
        reply.raise_for_status()
        logging.info("Modbus Device Deleted Successfully")
    def get_meter_tree(self,id=0,recursively=True,groupsOnly=False,withoutRegister=False):
        """Gets potentially all Meter Groups ("Folders"), Meter, Registers, and
        Virtual Meters in the various sub-groups.
        Parameters:
        - A group "id" of 0 (default) is taken as the root, otherwise the tree is
          walked from that point down
        - If "recursively" is false, only immediate children of the given group id
          are retreived, but if it's True (default), then all children,
          grand-children, and so on are retreived.
        - If "groupsOnly" is True, only the groups themselves are retreived, but if
          True (default), then all Meters and VirtualMeters are given too
        - If "withoutRegister" is True, then only Meters are given, otherwise all
          of the assciated registers are given too (default)
        All parameters are optional, as omissions are equivelent to the default
        If no parameters are given, (i.e. all defaults) then every item of the
        tree is retreived
        """
        subpath = "/MeterGroups/"
        with self.lock:
            reply = self.s.get(
                self.rooturl+subpath+str(id),
                params = {
                    "recursively":recursively,
                    "groupsOnly":groupsOnly,
                    "withoutRegister":withoutRegister,
                },
                timeout=self.timeout
            )
        reply.raise_for_status()
        return reply.json()
    def get_calibration_reads(self,registerId, startIndex=0, maxCount=2**31-1):
        """Retreive a list of calibration readings for the given registerId"""
        subpath = "/CalibrationReadings/"
        with self.lock:
            reply = self.s.get(
                self.rooturl+subpath,
                params = {
                    "registerId":registerId,
                    "startIndex":startIndex,
                    "maxCount":maxCount,
                },
                timeout=self.timeout,
                )
        reply.raise_for_status()
        # Just get relevent parts of the object returned
        result = reply.json()["calibrationReadings"]
        # Convert the datetime strings to real datetime objects which are tz aware
        for item in result:
            item["timestamp"] = DCSSession._fromisoformat(
                item["timestamp"]).replace(tzinfo=timezone.utc)
            item["startTime"] = DCSSession._fromisoformat(
                item["startTime"]).replace(tzinfo=timezone.utc)
        return result
    def get_meters_by_idc(self, macAddress):
        """Returns a list of all meters defined in DCS (excluding registers)
        under the IDC with the given macAddress (as an unsigned integer)"""
        subpath = "/Meters/byIdc/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(macAddress),
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def update_meter(self, settings):
        """Updates a meter defined by 'settings' with those parameters.
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised.
        Returns equivelent of get_meters(id)"""
        subpath = "/Meters/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath, json=settings,
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def get_metertypes(self, id=None):
        """Returns a list of all Meter Types defined in DCS or the one given by
        the given id. Returned object will include Register Types."""
        subpath = "/MeterTypes/"
        id = str(int(id)) if id is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+id, timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def add_registers(self, meterId, registerTypeIds):
        """Add new registers of the given type ids (list) to the meter with given id"""
        subpath = "/Registers/add/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath,
                json={ "meterId":int(meterId),
                    "registerTypeIds":tuple(registerTypeIds) },
                    timeout=self.timeout,
                )
        reply.raise_for_status()
        logging.info("Registers Added Successfully")
    def import_metereddata(self, filedata):
        """Import metered data CSV format file with contents provided in filedata"""
        subpath = "/registerReadings/import/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath,
                json={ "dataImportPluginId":1, "data":filedata },
                    timeout=self.timeout,
                )
        reply.raise_for_status()
        logging.info("Data Imported Successfully")
    def get_mega_readings(self, *args, maxwindow=timedelta(days=549),
        start=None, end=None, iterator=False, **kwargs):
        """Breaks up a potentially very large get_readings transaction into numerous
        smaller ones based on a maxwindow size (a timedelta defaulting to about 18
        months) and provides the result as if one single transaction was completed;
        either a very large list of readings or a single iterator over all readings
        from all constituent underlying transactions. Aside from maxwindow, all
        arguments are as per get_readings and are passed through"""
        if start is None:
            start = date.today()
        if end is None:
            end = start + timedelta(days=1)
        if end < start: # Swap dates if reversed
            start, end = end, start
        elif start == end:
            end = start + timedelta(minutes=30)
        maxwindow=abs(maxwindow)  # Strip negative durations and dont go too small
        if maxwindow < timedelta(days=1):
            maxwindow = timedelta(days=1)
        reqwindow = end - start # Requested window/duration
        logging.info(f"{reqwindow} requested and the maximum limit is {maxwindow}")
        if reqwindow <= maxwindow: # If the period is smaller than max, use directly
            logging.info("Only 1 transaction is needed")
            return self.get_readings(*args, start=start, end=end, iterator=iterator, **kwargs)
        else:   # If the period is larger than max, then break it down
            reqHHs = reqwindow // timedelta(minutes=30) # Requested duration in HalfHours
            maxHHs = maxwindow // timedelta(minutes=30) # Maximum duration in Halfhours
            d = 2   # Start by dividing into 2 intervals, since 1 was tested above
            while True:
                # Divide into incrementally more/smaller peices and check
                i, r = divmod(reqHHs, d)
                # Once the peices are small enough, stop.
                # If there is no remainder, take i, otherwise the remainders will
                # be added to other peices so add 1 and check that instead.
                if (i+1 if r else i) <= maxHHs: break
                d += 1
            # Make a list of HH sample sizes, with the remainders added onto the
            # first sets. Such as 11, 11, 10 for a total of 32.
            HHBlocks = [i+1]*r + [i]*(d-r)
            logging.info(f"{len(HHBlocks)} transactions will be used")
            Intervals=[]
            IntervalStart = start   # The first starttime is the original start
            for i in HHBlocks:
                # Add calculated number of half hours on to the start time
                IntervalEnd = IntervalStart + i * timedelta(minutes=30)
                # Define each sample window  and start the next one after the last
                Intervals.append({"start":IntervalStart,"end":IntervalEnd})
                IntervalStart = IntervalEnd
        # Create a generator which concatenates the readings from each transaction
        concat = (reading for chunk in Intervals
            for reading in self.get_readings(*args, iterator=iterator, **chunk, **kwargs))
        # Return this generator if an iterator was requested, else give a list
        return concat if iterator else list(concat)
    def get_users(self, id=None):
        """Get a detailed list of all users configured on the server
        or the one specified by id (a UUID like string)"""
        subpath = "/users/"
        id = str(id) if id is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+id,
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
    def update_user(self, user):
        """Updates a user defined by the 'user' dictionary with
        the parameters within. A read-modify-write process is advised.
        Returns the user details (like an element from get_users)
        although the DCS server appears to send the old details
        and not confirmation of the new details."""
        subpath = "/users/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath, json=user,
                timeout=self.timeout)
        reply.raise_for_status()
        return reply.json()
