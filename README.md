# pythondcs

The pythondcs module provides a convenient way to allow a Python application to access data within a [Coherent Research](http://coherent-research.co.uk/) DCS v3+ remote metering server via the DCS Web API. This may be to download meter reading data to validate invoices, big data analytics, or simply to dump into a file for consumption by some other system. Whatever the purpose, this module handles the link to DCS and returns data in standard Python data types, including those found within the standard [datetime](https://docs.python.org/3/library/datetime.html) library.

## Getting Started

This module is written in pure Python and should work on Python 3.6 and up.

### Prerequisites

You must of course have access to a Coherent DCS server system and have a valid username and password to access it. It's assumed that you are familiar with how meter data is structured within DCS and understand the concepts and terminology used. If this is not the case, please refer to the DCS User Guide for your server, or speak to your DCS System Administrator.

The only external module required is the [`python-requests`](http://docs.python-requests.org/) library. If installing `pyhondcs` via pip, this will be installed for you.

For efficient handling of larger data sets, you can optionally use the [`ijson`](https://github.com/isagalaev/ijson) library which is recommended if you envisage accessing large amounts of data in each transaction (such as years of halfhourly data at a time) as this will provide memory efficient iterators instead of lists. However, you may choose to omit the `ijson` module if you wish if you only plan to grab small amounts of data in each transaction, or if you don't mind the memory burden of very large lists with nested dictionaries. The `ijson` module is also availble via pip.

### Installing

The `pythondcs` package is available via pip, which will also install the `requests` prerequisite for you if you do not already have this. As mentioned above, `ijson` can optionally be used and is recomended.

At a command line (not in python), such as Windows cmd, or Linux/Mac shell/terminal:

```
pip install pythondcs
```
or
```
pip install pythondcs ijson
```

If you wish to ensure you have the latest version, then run `pip install --upgrade pythondcs` instead.

Once installed, `pythondcs` should be importable from within a python interpreter and available for use in your scripts.

```
import pythondcs
```

You are now ready to connect to a DCS server.

## Usage

The DcsWebApi class provides the methods/functions which allow you to login a session and obtain data. The methods provided are essentially just wrappers for the DCS Web API. For further details, please see the official [Coherent Research DCS Web API specification](https://github.com/coherent-research/dcs-documentation/blob/master/DcsPublicApiDescription.md).

### Signing in to a session

To create a DcsWebApi Session, create an object using the URL to your server, and provide your username and password. The URL is likely to be similar to that used for normal web access, but will be different - speak to your DCS System Administrator for the Web API URL to use.
```
dcs = pythondcs.DcsWebApi("https://url-of-dcs-web-api/", "myUsername", "MySuperSecurePassword")
```
This will then return: `Successfully logged in to DCS as 'myUsername' with Viewer privileges`

This session object will obtain and store your authentication cookie for the lifetime of the object.

You may alternatively just provide the URL which will create an un-authenticated DcsWebApi object and the `signin` method can be used directly later.
```
dcs = pythondcs.DcsWebApi("https://url-of-dcs-web-api/")
dcs.signin("myUsername", "MySuperSecurePassword")
```

If you don't provide credentials, the session object can be used in un-authenticated mode if your server allows. In this mode, only data that is publicically accessible can be accessed.

If your authentication cookie expires, subsequent requests may return an error to that effect, in which case you can simply use the `signin` method again.

### Getting a list of Meters or Virtual Meters

Getting a list of meters or virtual meters is as simple as a call to the `meters` or `virtualmeters` method. This will provde a list containing dictionaries describing the various attributes of the virtual meter or meter, including it's registers.

It is assumed as a user of DCS that you have an appreciation of what these mean, but in summary, Meters represent the devices being monitored and within these Registers represent the particular meter reading "channel" or measurement that is being logged. There can be multiple registers for the same meter and they can be cumulative (meter readings) or instantaneous parameters - such as import kwh (cumulative), export kwh (cumulative), voltage (instantaneous), current (instantaneous). Virtual meters, as the name implies, are not real but instead produce data based on data from other registers (one or more) based on an expression/formula, and so the resulting data is presented in a similar way to a register.

Most of the data provided by the `meters` or `virtualmeters` methods are essentially for information only, with exception to the ID numbers for registers and virtual meters as this can later be used to identify the them for reading retreival.

Meters:
It is possible to fetch a list of all meters that your credentials are allowed to access, and the registers within.
```
listofallmeters = dcs.meters()
```
The data returned will look much like the pretty representation here, where the relevent ID for obtaining readings has been marked:
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
        "id": 733,    ## This ID is important for obtaining readings
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
Note that the order each element is given in the dictionaries can change.

Virtual Meters:
It is possible to fetch a list of all virtual meters that your credentials are allowed to access.
```
listofallvms = dcs.virtualmeters()
```
The data returned will look much like the pretty representation here, where the relevent ID for obtaining readings has been marked:
```
[
   {
    "id": 9,    ## This ID is important for obtaining readings
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
Note that the order each element is given in the dictionaries can change.

For more details on the output, please see the [DCS Web API Spec](https://github.com/coherent-research/dcs-documentation/blob/master/DcsPublicApiDescription.md#get-virtual-meters).

The important ID numbers you'll want for getting readings are the **id** of the **registers** under the Meters, such as _733_ in the example above, and ***NOT*** 405, or the **virtual meters** itself which is _9_ in the example above. These numbers can also be found within the DCS front end interface (the one for humans!) from the "Registers" tab when viewing meter data, or directly from the list of Virtual Meters Be sure you don't use the Meter ID by accident.



### Getting Readings Data

This is likely the most important feature and the reason you are using this module.
Two methods have been provided to access Register Data and Virtual Meter Data; `readings` will simply provide data from a single transaction, while `largereadings` will divide a large query (i.e. for a large time span) into multiple smaller transactions depending on a maximum window size. This may be useful where your server has query restrictions set up preventing you from requesting data for large time spans.

Both cases will essentially behave the same using the same core parameters and should provide the same output. For smaller requests where multiple transactions are not needed, `largereadings` will use a single `readings` transaction and so its possible to exclusively use `largereadings` for all data requests without issue.

 To discriminate between the two sources of data, the respective Register IDs or Virtual Meter IDs are prepended by "R" or "VM" respectively and given as strings - for example, `"R130"` or `"VM9"` as in the examples above.

```
small_results = dcs.readings(id, startTime, endTime, periodCount, calibrated, interpolated, periodType, iterator)
results = dcs.leargereadings(id, startTime, endTime, periodCount, calibrated, interpolated, periodType, iterator, maxwindow=timedelta(days=365))
```
This will return an object containing a list or iterator of readings for the specified register or virtual meter and timespan. The structure is approximately a python dictionary containing header information with a nested list/iterator containing a dictionaries for each reading. Readings will have a timezone aware datetime in UTC, the reading value as a float (typically the Total Value, or instantaneous value) and an integer status flag.

Using an iterator (`iterator=True` with `ijson` module) will yield one reading at a time which may be more memory efficient for large data sets but the values are not retained after consumption. If memory usage is not a concern or you need to retain the data, then the `iterator=False` (default) will simply return one single list of reads. In both cases, each element of the list or iterator will consist of a dictionary with values as floats and dates as timezone aware datetime objects.

It is possible for the floats to represent positive and negative infinities or nan.

Parameters are as required by DCS:
- **id** - string of the register or virtual meter prepended by R or VM (Required)
- **startTime** - a python [datetime](https://docs.python.org/3/library/datetime.html#datetime-objects) or [date](https://docs.python.org/3/library/datetime.html#date-objects) object, such as `datetime.date(2019,1,1)`. (Required, see note)
- **endTime** - a datetime or date object as above. (Required, see note)
- **periodCount** - (`readings` only) integer number of periodTypes (Optional, see note)
- **calibrated** - boolean for whether values should be calibrated (Optional, default True)
- **interpolated** - boolean for whether gaps should be linearly filled (Optional, default True)
- **periodType** - string of "halfHour", or "hour", "day", "week", "month" defining the granularity of the data (Optional, default halfhour)
- **iterator** - boolean where False returns a single potentially large nested list, or True to return an iterator which streams and yields each reading as required. if the ijson module is not available, this option does nothing and is always equivelent to False.
- **maxwindow** - (`largereadings` only) [timedelta](https://docs.python.org/3/library/datetime.html#timedelta-objects) representing the largest time period a single query may span before being broken into smaller transactions (Optional, default datetime.timedelta(days=365))

Note: When using `readings`, the timespan covered by the request can be specified by including any 2 of startTime, endTime or periodCount. It is an error to specify anything other than 2. However, when using `largereadings`, periodCount cannot be used and you must explicitly provide the startTime and endTime. In both cases, if a datetime is provided which is timezone aware, this will be converted to UTC before being sent to the server. If it is naive it will be assumed to mean UTC (regardless of daylight savings in your region), and a plain date object will be assumed to be represent midnight UTC at the start of that date. See python documentation on [timezone aware and naive object](https://docs.python.org/3/library/datetime.html#aware-and-naive-objects).

Example, using the meter and register from earlier and default values:

```
results = dcs.leargereadings("R733, startTime=datetime(2021,9,20,18), endTime=datetime(2021,9,20,20))
```

The results will be structures as follows:
```
{
 "endTime": datetime.datetime(2021, 9, 20, 20, 0, tzinfo=datetime.timezone.utc),
 "name": "Sample meter: Active Energy (import)",
 "periodType": "halfHour",
 "startTime": datetime.datetime(2021, 9, 20, 18, 0, tzinfo=datetime.timezone.utc),
 "duration": 0,
 "unit": "kWh",
 "readings":
    [
      {
        "status": 0,
        "timestamp": datetime.datetime(2021, 9, 20, 18, 0, tzinfo=datetime.timezone.utc),
        "value": 9.04
      },
      {
        "status": 1,
        "timestamp": datetime.datetime(2021, 9, 20, 18, 30, tzinfo=datetime.timezone.utc),
        "value": 18.04
      },
      {
        "status": 1,
        "timestamp": datetime.datetime(2021, 9, 20, 19, 0, tzinfo=datetime.timezone.utc),
        "value": 43.08
      },
      {
        "status": 0,
        "timestamp": datetime.datetime(2021, 9, 20, 19, 30, tzinfo=datetime.timezone.utc),
        "Value": 59.0
      }
    ]
}
```
Note that the order each element is given in the dictionaries can change, but the nested `readings` list/iterator should always be last.

For more information on the status flags, please refer back to the [DCS Web API Spec](https://github.com/coherent-research/dcs-documentation/blob/master/DcsPublicApiDescription.md#response-4).

If the **iterator** option is used when `ijson` is installed, each dictionary element within what would otherwise be the "list" will be yielded by a generator function so you may embed this into a `for` loop and consume each element as it arrives rather than wait for the entire list to arrive (and consume memory):
```
for item in results:
    # Do something with each item individually as they arrive
```

For the **iterator** option to work, the `ijson` module must be installed, otherwise this has no effect.
Using an iterator (`iterator=True` with `ijson` module) will yield one reading at a time which may be more memory efficient for extremely large data sets (i.e. multiple years of half hourly data etc.), particularly if, for example, you just want to calculate an average in pure python without retaining all the data for later use. However, if memory usage is not a concern or you need to retain and work on the data as native python objects (rther than a pandas DataFrame for example), then `iterator=False` (default) will simply return one single list of reads. This may potentially be very large. In both cases, each element of the list or iterator will consist of a dictionary as received by the server with all numbers as floats, and timestamps as timezone aware datetime objects. If you are immediately loading the data into some other data structure, for example a pandas DataFrame, or numpy Arrays or even performing SQL insertions etc. without retaining the original python object, it is recomended that you install the `ijson` module and use `iterator=True` as this will improve performance, reduce latency and reduce memory use.

### Signout

When you have finished, it's good practice to signout of the session so as not to leave a dormant/orphaned authenticated session running on the server, or authentication cookies stored within your application memory. You need not do this if you are using the DcsWebApi object as a context manager within a `with` block.
```
dcs.signout()
```
The singout method will not delete the DCSSession object and the `login` method may be used straight after. This can be used to login again or change credentials during execution.

### Usage as a Context Manager

The DcsWebApi class can be used as a context manager for more compact code with the real work being done within the `with` block. This will handle signing in and out for you as the block is entered and exited.
Example:

```
with pythondcs.DcsWebApi("https://url-of-dcs-web-api/", "myUsername", "MySuperSecurePassword") as dcs:
    # Do stuff with dcs
    # Do more stuff dcs

# Block signs out automatically
```

## Basic Example
```
import datetime
import pythondcs
dcs = pythondcs.DcsWebApi("https://url-of-dcs-web-api/", "myUsername", "MySuperSecurePassword")
listofreadings = dcs.readings("R123", startTime=datetime.date(2019,1,1), endTime=datetime.date(2019,1,31))
dcs.logout()
```
In this simple example, the appropriate modules are imported, including `datetime` to allow the start and end times to be provided correctly. The script then signs in, and downloads readings for register ID 123 for January 2019. This will default to calibrated, interpolated halfhourly given in UTC/GMT as would be the default as these parameters are ommitted. The session is then logged out.

## Elaborate Example
```
from datetime import date
from pythondcs import DcsWebApi
with DcsWebApi("https://url-of-dcs-web-api/", "myUsername", "MySuperSecurePassword") as dcs:
    listofvms = dcs.virtualmeters()
    for vm in listofvms:
        if vm["name"] == "Virtual Meter of Interest"
            idofinterest = vm["id"]
            break
    maxdemand = max(item["value"] for item in dcs.readings(idofinterest, date(2019,1,1), date.today(), iterator=True))
```
In this example, slightly more condensed namespaces are used, and a context manager is used to create an authenticated session which is then used to get a list of all virtual meters. This list (containing dictionaries) is then looped through to search for the first one with the name `"Virtual Meter of Interest"` (assuming this exists on the server) at which point the ID number is retained and loop broken. This is then used to efficiently (using a generator comprehension with `iterator=True` option) find the maximum halfhour demand value for that virtual meter between new year 2019 and the current day, given that the 'value' in each case is the usage in that period for Virtual Meters (This wouldn't be the case with Registers as the value represents the meter reading itself). The authenticated session is then automatically logged out upon leaving the `with` block.

### Exceptions

Any exceptions raised by the underlying API call will be propagated to the caller and so it is for the higher level application to deal with them. This is most likely to be from providing an invalid or unauthorised register or virtual meter id number when getting readings for example. The only place this does not happen is with logging in where the error message from the server will be returned, or logging out where exceptions are simply ignored. If an exception occurs during login (i.e. invalid credentials), the DcsWebApi object will still be provided in an un-authenticated state where the `signing` method can be called again directly.

### Concurrent Transactions

This module has not specifically been designed to be thread-safe, but will probably work in multi-threaded environments just fine. There is however a thread-lock which deliberately limits each instance of a DCSSession object to a single concurrent transaction at a time (irrespective of number of threads which may be trying to work with it). This is primarily to protect the DCS server itself from being overwhelmed with concurrent transactions. Concurrent transactions are still possible with multiple DcsWebApi objects or, of course, multi-process environments.

There is no limit to the rate at which consecutive transactions can occur other than what may be enforced by the DCS server via HTTP 429 statuses and X-Rate-Limit headers. If the rate limit is reached, the DcsWebApi method will simply wait for the time recommended by the server to retry and so this may be seen as a delayed response. The rate limiting in this case is imposed by the server and potentially triggered by and impacting on all users so care must be taken not to overwhelm the server with excessive/unnecessary small but fast requests - including invalid ones raising errors.

### Other functions

Additional functions are available in the `pythondcspro` module using the API driving the User Interface, but this is not officiall supported by Coherent for third party use and so they are not fully documented and subject to breaking changes with differing versions of DCS. The functions provided have been reversed engineered from analysis of how the front end user interface works and so they are to be used at your own risk. The `pythondcspro` module which is supplied as part of this project. This contains similar methods with similar functionality but with different names, parameters and outputs formats and so they are not directly interchangable. Additional methods can modify the DCS database and so are to be used at your own risk. Please see the source code inline comments within this file for further details. This is recomended for advanced usage only and you take full responsibilty if your data is inadvertently corrupted or destroyed!

## Author

**Mark Jarvis** - [LinkedIn](https://www.linkedin.com/in/marksjarvis/) | [GitHub](https://github.com/jarvisms) | [PyPi](https://pypi.org/user/jarvism/)

I'm employed by [University of Warwick Estates Office, Energy & Sustainability Team](https://warwick.ac.uk/about/environment) as a Sustainability Engineer and as part of this role I am responsible for managing the University's several thousand meters and remote metering infrastructure based on [Coherent Research's](http://coherent-research.co.uk/) equipment and DCS Software platform. While this module will inevitably be used within my work to cleanse, analyse and transfer data between other software platforms and may benefit other users within or collaborating with the University for research projects, this module was written **exclusively** as a personal project since I'm not employed as a software developer!

## Contributions & Feature requests

For bugs, or feature requests, please contact me via GitHub or raise an [issue](https://github.com/jarvisms/pythondcs/issues).

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](https://github.com/jarvisms/pythondcs/blob/master/LICENSE) file for details

## Acknowledgements

* Thanks to [Coherent Research](http://coherent-research.co.uk/) for documentation and on going technical support.
