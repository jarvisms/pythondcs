# pythondcs

The pythondcs module provides a convenient way to allow a Python application to access data within a [Coherent Research](http://coherent-research.co.uk/) DCS v3+ remote metering server via its built in web API using json formatting. This may be to download meter reading data to validate invoices, big data analytics, or simply to dump into a file for consumption by some other system. Whatever the purpose, this module handles the link to DCS and returns data in standard Python data types, including those found within the standard [datetime](https://docs.python.org/3/library/datetime.html) library.

## Getting Started

This module is written in pure Python and should work on Python 3.6 and up.

### Prerequisites

You must of course have access to a Coherent DCS server system and have a valid username and password to access it. It's assumed that you are familiar with how meter data is structured within DCS and understand the concepts and terminology used. If this is not the case, please refer to the DCS User Guide for your server, or speak to your DCS System Administrator.

The only external module required is the [`python-requests`](http://docs.python-requests.org/) library. If installing `pyhondcs` via pip, this will be installed for you.

For efficient handling of larger data sets, you can optionally use the [`ijson`](https://github.com/isagalaev/ijson) which is recommended if you envisage accessing large amounts of data in each transaction (such as years of halfhourly data at a time) as this will provide memory efficient iterators instead of lists. However, you may choose to omit the `ijson` module if you wish if you only plan to grab small amounts of data in each transaction, or if you don't mind the memory burden of very large lists with nested dictionaries. The `ijson` module is also availble via pip.

### Installing

The `pythondcs` package is available via pip, which will also install the `requests` prerequisite for you if you do not already have this.

At a command line (not in python), such as Windows cmd, or Linux/Mac shell/terminal:

```
pip install pythondcs
```

If you wish to ensure you have the latest version, then run `pip install --upgrade pythondcs` instead.

Once installed, `pythondcs` should be importable from within a python interpreter and available for use in your scripts.

```
import pythondcs
```

You are now ready to connect to a DCS server.

## Usage

The DCSSession class provides the methods/functions which allow you to login a session and obtain data. The methods provided are essentially just wrappers for the DCS Web API. For further details, please see the official [Coherent Research DCS Web API specification](https://www.coherent-research.co.uk/support/extras/dcsapispec/).

### Logging in to a session

To create a DCS Session, create an object using the URL to your server, and provide your username and password. The URL to use would be the same as for the normal user interface.
```
dcs = pythondcs.DCSSession("https://url-of-dcs/", "myUsername", "MySuperSecurePassword")
```
This will then return: `Successfully logged in to DCS as 'myUsername' with Viewer privileges`

This session object will obtain and store your authentication cookie for the lifetime of the object.

You may alternatively just provide the URL which will create an un-authenticated DCSSession object and the `login` method can be used directly later.
```
dcs = pythondcs.DCSSession("https://url-of-dcs/")
dcs.login("myUsername", "MySuperSecurePassword")
```

### Getting a list of Meters or Virtual Meters

Getting a list of meters or virtual meters is as simple as a call to the `get_meters` or `get_vms` method

Meters:
It is possible to fetch a list of all meters, or one specific one.
```
listofallmeters = dcs.get_meters()
singlemeter = dcs.get_meters(405)
```
The data returned in both cases will look much like the pretty representation here:
```
[
  {
    "connectionMethod": "tcp",
    "deviceId": "",
    "id": 405,
    "name": "Sample meter",
    "remoteAddress": "10.8.222.99:5000",
    "serialNumber": "R1100729",
    "status": "online",
    "registers": [
      {
        "address": "130",
        "id": 733,
        "isInstantaneous": false,
        "name": "Active Energy (import)",
        "scaleFactor": "",
        "defaultScaleFactor": "1",
        "unit": "kWh"
      }
    ],
  },
  ...
  ...
}]
```

Virtual Meters:
It is possible to fetch a list of all virtual meters, or one specific one.
```
listofallvms = dcs.get_vms()
singlevm =  dcs.get_vms(9)
```
The data returned in both cases will look much like the pretty representation here:
```
[
   {
    "id": 9,
    "name": "Sample Virtual Meter",
    "expression": "R1+R2+R3",
    "decimalPlaces": 2,    
    "isInstantaneous": false,
    "unit": "kWh",
    "registerAliases": [
      {
        "alias": "R1",
        "registerId": 1156,
        "registerName": ""
      },
      {
        "alias": "R2",
        "registerId": 1164,
        "registerName": ""
      },
      {
        "alias": "R3",
        "registerId": 1168,
        "registerName": ""
      }
    ]
  },
  ...
  ...
}]
```
For more details on the output, please see the [DCS Web API Spec](https://www.coherent-research.co.uk/support/extras/dcsapispec/#meters-and-virtual-meters-get-meters).

The important ID numbers you'll want for getting readings are the **id** of the **registers** under the Meters, such as _130_ in the example above, and ***NOT*** 405, or the **virtual meters** itself which is _9_ in the example above. These numbers can also be found within the DCS front end interface (the one for humans!) from the "Registers" tab when viewing meter data, or directly from the list of Virtual Meters Be sure you don't use the Meter ID by accident.

### Getting Readings Data

This is likely the most important feature and the reason you are using this module.
The same method has been provided to access Register Data and Virtual Meter Data even though the underlying API calls are different. The parameters are essentially the same and result set have the same structure and so these have a single method within this module for convenience and consistency.

```
results = dcs.get_readings(id, isVirtual, start, end, decimalPlaces, calibrated, interpolated, useLocalTime, integrationPeriod, source, iterator)
```
The only required item is **id**. The other options are optional and relate to options also available on the Web front end of DCS.
-  **id** - of the register (*not* the meter.) or virtual meter (Required).
-  **isVirtual** - True if id refers to a virtual meter ID, otherwise False for a register. (Optional, default False)
-  **start** - a python [datetime](https://docs.python.org/3/library/datetime.html#datetime-objects) or [date](https://docs.python.org/3/library/datetime.html#date-objects) object (Optional, default omitted), such as `datetime.date(2019,1,1)`. If ommitted, the default date used is defined by the server
-  **end** - a datetime or date object (Optional, default ommitted), as above. If ommitted, the default date used is defined by the server.
-  **decimalPlaces** - of the returned data 0-15 (Optional, default 15)
-  **calibrated** - totalValues to be calibrated (Optional, default True)
-  **interpolated** - gaps to be linearly filled (Optional, default True)
-  **useLocalTime** - for timestamps (Optional, default False for UTC). Returned data will be timezone aware regardless.
-  **integrationPeriod** - Defaults to "halfHour", or "hour", "day", "week", "month" (Optional)
-  **source** - Defaults to "automatic", or "manual" "merged" (Optional)
-  **iterator** - False (default) give the results as one large list, True will return a generator where each reading can be consumed one at a time (Recommended)

If the start and end dates are omitted, the server defaults are used, which is generally a start date of today with an end date 1 day later. In almost all cases, these would be specified.

The results will be structures as follows:
```
[
  {
    "id": 26167735,
    "startTime": datetime.datetime(2019, 2, 1, 0, 0, tzinfo=datetime.timezone.utc),
    "duration": 30,
    "totalValue": 10161.5,
    "periodValue": 11,
    "isGenerated": false,
    "isInterpolated": false
  },
  {
    "id": 26167736,
    "startTime": datetime.datetime(2019, 2, 1, 0, 30, tzinfo=datetime.timezone.utc),
    "duration": 30,
    "totalValue": 10172.5,
    "periodValue": 5.3,
    "isGenerated": false,
    "isInterpolated": false
  },
  ...
  ...
]
```
If the **iterator** option is used when `ijson` is installed, each dictionary element within what would otherwise be the "list" will be yielded by a generator function so you may embed this into a `for` loop:
```
for item in results:
    # Do something with each item individually as they arrive
```

For the **iterator** option to work, the `ijson` module must be available, otherwise this has no effect.
Using an iterator (`iterator=True` with `ijson` module) will yield one reading at a time which may be more memory efficient for extremely large data sets (i.e. multiple years of half hourly data etc.), particularly if, for example, you just want to calculate an average without retaining all the data. However, if memory usage is not a concern or you need to retain the data for more complicated manipulations, then `iterator=False` (default) will simply return one single list of reads. This may potentially be very large. In both cases, each element of the list or iterator will consist of a dictionary as received by the server with all numbers as integers or floats, and timestamps as timezone aware datetime objects.

For more information on the meaning of each item, particularly the `isGenerated` and `isInterpolated` flags, please refer back to the [DCS Web API Spec](https://www.coherent-research.co.uk/support/extras/dcsapispec/#metered-data-get-register-readings), however if you are familiar with the DCS web front end, you'll probably have some familiarity with these fields. Generally the `id` field can be discarded.

### Logout

When you have finished, it's good practice to logout of the session so as not to leave a dormant/orphaned authenticated session running on the server, or authentication cookies stored within your application memory. You need not do this if you are using the DCSSession object as a context manager within a `with` block.
```
dcs.logout()
```
The logout method will not delete the DCSSession object and the `login` method may be used straight after. This can be used to login again or change credentials during execution.

### Usage as a Context Manager

The DCSSession class can be used as a context manager for more compact code with the real work being done within the `with` block. This will handle logging in and out for you as the block is entered and exited.
Example:

```
with pythondcs.DCSSession("https://url-of-dcs/", "myUsername", "MySuperSecurePassword") as dcs:
    # Do stuff with dcs
    # Do more stuff dcs
```

## Basic Example
```
import datetime
import pythondcs
dcs = pythondcs.DCSSession("https://url-of-dcs/", "myUsername", "MySuperSecurePassword")
listofreadings = dcs.get_readings(123, start=datetime.date(2019,1,1), end=datetime.date(2019,1,31))
dcs.logout()
```
In this simple example, the appropriate modules are imported, including `datetime` which will ensure all dates and times are correctly handled. The script then logs in, and downloads readings for register ID 123 for January 2019. This will default to calibrated, interpolated halfhourly automatic data up to 15 decimal places long given in UTC/GMT as would be the default as these settings are ommitted. The session is then logged out.

## Elaborate Example
```
from datetime import date
from pythondcs import DCSSession
with DCSSession("https://url-of-dcs/", "myUsername", "MySuperSecurePassword") as dcs:
    listofvms = dcs.get_vms()
    for vm in listofvms:
        if vm["name"] == "Virtual Meter of Interest"
            idofinterest = vm["id"]
            break
    maxdemand = max(item["periodValue"] for item in dcs.get_readings(idofinterest, True, date(2019,1,1), date.today(), iterator=True))
```
In this example, slightly more condensed namespaces are used, and a context manager is used to create an authenticated session which is then used to get a list of all virtual meters. This list (containing dictionaries) is then looped through to search for the first one with the name `"Virtual Meter of Interest"` (assuming at least one exists on the server) at which point the ID number is retained and loop broken. This is then used to efficiently (using a generator comprehension with `iterator=True` option) find the maximum halfhour demand value for that virtual meter between new year and the current day. The authenticated session is then automatically logged out upon leaving the `with` block.

### Exceptions

Any exceptions raised by the underlying API call will be propagated to the caller and so it is for the higher level application to deal with them. This is most likely to be from providing an invalid id number when getting readings for example. The only place this does not happen is with logging in where the error message from the server will be returned, or logging out where exceptions are simply ignored. If an exception occurs during login (i.e. invalid credentials), the DCSSession object will still be provided in an un-authenticated state where the `login` method can be called again directly.

### Concurrent Transactions

This module has not specifically been designed to be thread-safe, but will probably work in multi-threaded environments just fine. There is however a thread-lock which deliberately limits each instance of a DCSSession object to a single concurrent transaction at a time (irrespective of number of threads which may be trying to work with it). This is primarily to protect the DCS server itself from being overwhelmed with concurrent transactions. However there is no limit to the rate at which consecutive transactions can occur. Therefore care must be taken not to overwhelm the server with numerous small but fast requests - including invalid ones raising errors. Concurrent transactions are still possible with multiple DCSSession objects or, of course, multi-process environments.

### Other functions

Additional functions are available but they are not documented in the [DCS Web API Spec](https://www.coherent-research.co.uk/support/extras/dcsapispec/) and so they are not fully documented here as they are not officially supported for use by third parties. The functions provided have been reversed engineered from analysis of how the front end user interface works and so they are to be used at your own risk and their behaviour may change at any time. These are available within the `pythondcspro` module which is supplied as part of this project. This module essentially duplicates the standard functionality and so can be used in place of `pythondcs`, but with additional features, some of which can modify the DCS database and so are to be used at your own risk. Please see the source code inline comments within this file for further details.

## Author

**Mark Jarvis** - [LinkedIn](https://www.linkedin.com/in/marksjarvis/) | [GitHub](https://github.com/jarvisms) | [PyPi](https://pypi.org/user/jarvism/)

I'm employed by [University of Warwick Estates Office, Energy & Sustainability Team](https://warwick.ac.uk/about/environment) as a Sustainability Engineer and as part of this role I am responsible for managing the University's several thousand meters and remote metering infrastructure based on [Coherent Research's](http://coherent-research.co.uk/) equipment and DCS Software platform. While this module will inevitably be used within my work to cleanse and analyse data, and may benefit other users within or collaborating with the University for research projects, this module was written exclusively as a personal project since I'm not employed as a software developer!

## Contributions & Feature requests

For bugs, or feature requests, please contact me via GitHub or raise an [issue](https://github.com/jarvisms/pythondcs/issues).

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](https://github.com/jarvisms/pythondcs/blob/master/LICENSE) file for details

## Acknowledgements

* Thanks to [Coherent Research](http://coherent-research.co.uk/) for documentation and on going technical support.
