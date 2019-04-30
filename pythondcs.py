from datetime import datetime, date, timedelta, timezone
from threading import RLock
import requests

try:
    import ijson, gzip
    IJSONAVAILABLE = True
except ImportError:
    IJSONAVAILABLE = False

def _iterjson_reads(reply):
    """Takes the http response and decodes the json payload by streaming it,
    decompressing it if required, and decoding it into an ijson iterator as
    elements are consumed. Convert timestapms to datetime objects and ensure
    all Decimals are converted back to native floats"""
    # Prepare to decompress on the fly if required
    if reply.headers["content-encoding"] in ("gzip", "deflate"):
        raw = gzip.open(reply.raw)
    else:
        raw = reply.raw
    n=0
    for item in ijson.items(raw, 'item'):
        # Convert to datetimes and floats where needed
        item["startTime"] = datetime.strptime(item["startTime"],"%Y-%m-%dT%H:%M:%S%z")
        if type(item["totalValue"]) == ijson.common.decimal.Decimal:
            item["totalValue"] = float(item["totalValue"])
        if type(item["periodValue"]) == ijson.common.decimal.Decimal:
            item["periodValue"] = float(item["periodValue"])
        yield item  # Yield each item one at a time
        n+=1
    print(f"All {n} readings retreived")

def _json_reads(reply):
    """Takes the http response and decodes the json payload as one object
    Convert timestamps to datetime objects"""
    results = reply.json()
    for item in results:
        # Convert to datetimes
        item["startTime"] = datetime.strptime(item["startTime"],"%Y-%m-%dT%H:%M:%S%z")
    print(f"All {len(results)} readings retreived")
    return results

def macint_to_hex(MacInt):
    """Converts integers to hex MAC address"""
    assert 0 <= MacInt <= 0xFFFFFFFFFFFF, "Integer out of range"
    return ":".join([format(MacInt,"012X")[x:x+2] for x in range(0,12,2)])

def machex_to_int(MacHex):
    """Converts hex MAC address to integers"""
    assert 12 <= len(MacHex) <= 17, "String of unexpected size"
    return int(MacHex.replace(":","").lower(), 16)

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
        self.s = requests.Session()
        self.s.stream = True
        self.rooturl = rooturl + "/api"
        self.username = None
        self.appVersion = None
        self.role = None
        if None not in (username, password):
            self.login(username, password)
        else:
            print("Incomplete credentials given; Please use the login method")
    def login(self, username, password):
        """
        Logs in to DCS server and returns a logged in session for future use.
        An authentication token (cookie) will be stored for this session.
        Must be provided with a username and password.
        """
        subpath = "/account/login/"
        s  = self.s
        if len(s.cookies) > 0:
            self.logout()
        try:
            with self.lock:
                reply = s.post(self.rooturl+subpath,
                    json={"username":username,"password":password}
                    )
            reply.raise_for_status()
            result = reply.json()
            self.username = result['username']
            self.role = result['role']
            print(f"Successfully logged in to DCS as '{self.username}' with {self.role} privileges")
        except requests.exceptions.HTTPError as err:
            r = err.response
            print(f"{r.status_code}: {r.reason}, '{r.text}'\n{r.url}")
        self.s = s
    def logout(self):
        """Logs out of the current DCS session."""
        subpath = "/account/logout/"
        with self.lock:
            self.s.post(self.rooturl+subpath)
        self.username = None
        self.appVersion = None
        self.role = None
        print("Logged Out of DCS")
    def __del__(self):
        """Logs out of DCS upon deletion and garbage collection of this object"""
        self.logout()
    def get_meters(self, id=None):
        """
        Returns a list of all meters defined in DCS, or the one with the given
        id. Returned object will includ the registers for each meter.
        This is a direct equivelent to the "Get meters" API call.
        """
        subpath = "/Meters/"
        id = str(int(id)) if id is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+id)
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
            reply = self.s.get(self.rooturl+subpath+id)
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
        if type(dataparams["start"]) in (datetime, date):
            dataparams["start"] = dataparams["start"].isoformat()
        elif dataparams["start"] is None:
            dataparams["start"] = date.today().isoformat()
        if type(dataparams["end"]) in (datetime, date):
            dataparams["end"] = dataparams["end"].isoformat()
        # Actually get the data and stream it into the json iterative decoder
        with self.lock:
            # Stream the response into json decoder for efficiency
            reply = self.s.get(self.rooturl+subpath, params=dataparams)
            reply.raise_for_status()    # Raise exception if not 2xx status
            print(f"Got readings for {'VM' if isVirtual else 'R'}{id}, server response time: {reply.elapsed.total_seconds()}s")
        if iterator and IJSONAVAILABLE:
            # The user must ask for an iterator AND the module must be available
            return _iterjson_reads(reply)
        else:
            return _json_reads(reply)
    def get_idcs(self, macAddress=None):
        """Get a detailed list of all IDCs known to the server
        or the one specified by macAddress (as an unsigned integer)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/Idcs/"
        macAddress = str(int(macAddress)) if macAddress is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+macAddress)
        reply.raise_for_status()
        return reply.json()
    def get_idc_settings(self, macAddress):
        """Retreive the IDC settings from the IDC with the given macAddress
        (as an unsigned integer)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/Idcs/settings/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(macAddress)))
        reply.raise_for_status()
        return reply.json()
    def update_idc_settings(self, macAddress, settings):
        """Update the IDC settings for the IDC with the given macAddress
        (as an unsigned integer). setting of the same form as get_idc_settings()
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised.
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/idcs/settings/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath,
                json={"macAddress":macAddress, "idcSettings":settings}
            )
        reply.raise_for_status()
    def get_modbus_devices_by_idc(self, macAddress):
        """Get details of all Modbus Devices under the IDC with the given
        macAddress (as an unsigned integer)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/ModbusDevices/byIdc/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(macAddress)))
        reply.raise_for_status()
        return reply.json()
    def get_modbus_device_by_id(self, id):
        """Get Modbus Device with the given id (as an unsigned integer)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(id)))
        reply.raise_for_status()
        return reply.json()
    def update_modbus_device(self, device):
        """Updates a modbus device defined by 'device' with those parameters.
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised.
        Then returns the resulting modbus device (like get_modbus_device_by_id)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath, json=device)
        reply.raise_for_status()
        return reply.json()
    def add_modbus_device(self, device):
        """Adds the modbus device defined by 'device' with those parameters.
        Parameters are 'address':<int>, 'description':<str>, 'serialNumber':<str>
        'deviceType':'pulseCounter'|'radioReceiver', 'macAddress':<int> 
        Then returns the resulting modbus device (like get_modbus_device_by_id)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath, json=device)
        reply.raise_for_status()
        return reply.json()
    def command_modbus_device(self, id, command):
        """Executes 'command' on the modbus device with 'id'
        Only accepts commands: "testComms", "setup", "resetBuffer", and only
        Pulse Counters or Radio Receivers will accept "setup" or "resetBuffer"
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/ModbusDevices/command/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath,
                json={"id": int(id), "action":command})
        reply.raise_for_status()
        return reply.json()
    def delete_modbus_device(self, id):
        """Deletes the modbus device with the given 'id'.
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.delete(self.rooturl+subpath++str(int(id)))
        reply.raise_for_status()
        print("Modbus Device Deleted Successfully")
    def get_meter_tree(self,id=0,recursively=True,groupsOnly=False,withoutRegister=False):
        """Gets potentially all Meter Groups ("Folders"), Meter, Registers, and
        Virtual Meters in the various sub-groups.
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK
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
                }
            )
        reply.raise_for_status()
        return reply.json()
    def get_calibration_reads(self,registerId):
        """Retreive a list of all calibration readings for the given registerId
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/CalibrationReadings/"
        with self.lock:
            reply = self.s.get(
                self.rooturl+subpath,
                params = {
                    "registerId":registerId,
                    "startIndex":0,
                    "maxCount":2**31-1,
                })
        reply.raise_for_status()
        # Just get relevent parts of the object returned
        result = reply.json()["calibrationReadings"]
        # Convert the datetime strings to real datetime objects which are tz aware
        for item in result:
            item["timestamp"] = datetime.strptime(
                item["timestamp"],"%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            item["startTime"] = datetime.strptime(
                item["startTime"],"%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return result
    def get_meters_by_idc(self, macAddress):
        """Returns a list of all meters defined in DCS (excluding registers)
        under the IDC with the given macAddress (as an unsigned integer)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/Meters/byIdc/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(macAddress))
        reply.raise_for_status()
        return reply.json()
    def update_meter(self, settings):
        """Updates a meter defined by 'settings' with those parameters.
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised.
        Returns equivelent of get_meters(id)
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/Meters/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath, json=settings)
        reply.raise_for_status()
        return reply.json()
    def get_metertypes(self, id=None):
        """Returns a list of all Meter Types defined in DCS or the one given by
        the given id. Returned object will include Register Types.
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/MeterTypes/"
        id = str(int(id)) if id is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+id)
        reply.raise_for_status()
        return reply.json()
    def add_registers(self, meterId, registerTypeIds):
        """Add new registers of the given type ids (list) to the meter with given id
        NOT OFFICIALLY DOCUMENTED - USE AT YOUR OWN RISK"""
        subpath = "/Registers/add/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath,
                json={ "meterId":int(meterId),
                    "registerTypeIds":tuple(registerTypeIds) }
                )
        reply.raise_for_status()
        print("Registers Added Successfully")