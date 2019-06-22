# This is an Extention module which can be used in place of the standard module
# All features from the standard module will be inherited and added to
from pythondcs import *

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
            reply = self.s.get(self.rooturl+subpath+macAddress)
        reply.raise_for_status()
        return reply.json()
    def get_idc_settings(self, macAddress):
        """Retreive the IDC settings from the IDC with the given macAddress
        (as an unsigned integer)"""
        subpath = "/Idcs/settings/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(macAddress)))
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
                json={"macAddress":macAddress, "idcSettings":settings}
            )
        reply.raise_for_status()
    def get_modbus_devices_by_idc(self, macAddress):
        """Get details of all Modbus Devices under the IDC with the given
        macAddress (as an unsigned integer)"""
        subpath = "/ModbusDevices/byIdc/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(macAddress)))
        reply.raise_for_status()
        return reply.json()
    def get_modbus_device_by_id(self, id):
        """Get Modbus Device with the given id (as an unsigned integer)"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(int(id)))
        reply.raise_for_status()
        return reply.json()
    def update_modbus_device(self, device):
        """Updates a modbus device defined by 'device' with those parameters.
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised.
        Then returns the resulting modbus device (like get_modbus_device_by_id)"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath, json=device)
        reply.raise_for_status()
        return reply.json()
    def add_modbus_device(self, device):
        """Adds the modbus device defined by 'device' with those parameters.
        Parameters are 'address':<int>, 'description':<str>, 'serialNumber':<str>
        'deviceType':'pulseCounter'|'radioReceiver', 'macAddress':<int> 
        Then returns the resulting modbus device (like get_modbus_device_by_id)"""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath, json=device)
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
                json={"id": int(id), "action":command})
        reply.raise_for_status()
        return reply.json()
    def delete_modbus_device(self, id):
        """Deletes the modbus device with the given 'id'."""
        subpath = "/ModbusDevices/"
        with self.lock:
            reply = self.s.delete(self.rooturl+subpath+str(int(id)))
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
                }
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
                })
        reply.raise_for_status()
        # Just get relevent parts of the object returned
        result = reply.json()["calibrationReadings"]
        # Convert the datetime strings to real datetime objects which are tz aware
        for item in result:
            try:
                item["timestamp"] = datetime.strptime(
                    item["timestamp"],"%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                item["timestamp"] = datetime.strptime(
                    item["timestamp"],"%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
            try:
                item["startTime"] = datetime.strptime(
                    item["startTime"],"%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                item["startTime"] = datetime.strptime(
                    item["startTime"],"%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
        return result
    def get_meters_by_idc(self, macAddress):
        """Returns a list of all meters defined in DCS (excluding registers)
        under the IDC with the given macAddress (as an unsigned integer)"""
        subpath = "/Meters/byIdc/"
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+str(macAddress))
        reply.raise_for_status()
        return reply.json()
    def update_meter(self, settings):
        """Updates a meter defined by 'settings' with those parameters.
        Any missing parameters will be defaulted to zero/blank so this
        could be destructive. A read-modify-write process is advised.
        Returns equivelent of get_meters(id)"""
        subpath = "/Meters/"
        with self.lock:
            reply = self.s.put(self.rooturl+subpath, json=settings)
        reply.raise_for_status()
        return reply.json()
    def get_metertypes(self, id=None):
        """Returns a list of all Meter Types defined in DCS or the one given by
        the given id. Returned object will include Register Types."""
        subpath = "/MeterTypes/"
        id = str(int(id)) if id is not None else ""
        with self.lock:
            reply = self.s.get(self.rooturl+subpath+id)
        reply.raise_for_status()
        return reply.json()
    def add_registers(self, meterId, registerTypeIds):
        """Add new registers of the given type ids (list) to the meter with given id"""
        subpath = "/Registers/add/"
        with self.lock:
            reply = self.s.post(self.rooturl+subpath,
                json={ "meterId":int(meterId),
                    "registerTypeIds":tuple(registerTypeIds) }
                )
        reply.raise_for_status()
        print("Registers Added Successfully")
