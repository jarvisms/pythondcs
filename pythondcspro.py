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
        meters=flatten(subgroup,meters)
    return meters

class DCSSession(DCSSession):
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
        print("Modbus Device Deleted Successfully")
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
        print("Registers Added Successfully")
    def import_metereddata(self, filedata):
        """Import metered data CSV format file with contents provided in filedata"""
        subpath = "/registerReadings/import/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath,
                json={ "dataImportPluginId":1, "data":filedata },
                    timeout=self.timeout,
                )
        reply.raise_for_status()
        print("Data Imported Successfully")
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
        print(f"{reqwindow} requested and the maximum limit is {maxwindow}")
        if reqwindow <= maxwindow: # If the period is smaller than max, use directly
            print("Only 1 transaction is needed")
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
            print(f"{len(HHBlocks)} transactions will be used")
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
