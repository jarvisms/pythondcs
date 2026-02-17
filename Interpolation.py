from datetime import date, timedelta

def interpolate(startAnchor, endAnchor, duration):
    """
    Given with a start reading and an end reading as anchors (Dictionaries with timestamps and values i.e. total values) and the duration (i.e. the periodType),
    this will yield estimated readings at the expected timestamps and provide period values and statuses of 1.
    The very first item is included (startAnchor), but the last is excluded (endAnchor)
    Note: This assumes the duration is fixed for each interval and that the start and end are integer number of intervals apart.
          Therefore this will not handle monthly data due to differing sized months.
          No type or sanity checking is done.
    """
    tDelta = endAnchor['timestamp'] - startAnchor['timestamp']
    vDelta = endAnchor['value'] - startAnchor['value']
    if duration != "monthly":   # Halfhourly, Hourly, Daily, Weekly - in all cases, the period duration is fixed
        periodCount = tDelta / duration         # Should be an integer number
        periodValue = duration*vDelta/tDelta    # This will be the same throughout
        for n in range( int(periodCount) ):     # Includes 0th, excludes last
            # targetTimestamp is now, without looking ahead for periodValues
            sinceStart = n*duration     # Time since start
            targetTimestamp = startAnchor['timestamp'] + sinceStart
            targetReading = startAnchor['value'] + vDelta * sinceStart / tDelta
            yield {"timestamp": targetTimestamp, "value": targetReading, "status": 1, "periodValue": periodValue}
    else:   # if this is Monthly data
        targetTimestamp = startAnchor['timestamp']
        targetReading = startAnchor['value']
        periodValue = 0
        while True:
            nextTimestamp = targetTimestamp.replace(month = targetTimestamp.month+1) if targetTimestamp.month < 12 else targetTimestamp.replace(month = 1, year = targetTimestamp.year+1)
            targetReading = targetReading + periodValue # new targetReading is the last targetReading + the last periodValue
            periodValue = vDelta * (nextTimestamp - targetTimestamp) / tDelta
            yield {"timestamp": targetTimestamp, "value": targetReading, "status": 1, "periodValue": periodValue}
            targetTimestamp = nextTimestamp
            if targetTimestamp >= endAnchor['timestamp']:
                break

def perioddata(totaldata, duration):
    """
    Given the readings array, this will yield period values for each interval. Note that the last one will be None
    If there appears to be a gap in the data, interpolated values will be provided instead
    Timestamps mark the START of the period
    """
    totaldata = iter(totaldata) # Make an iterator
    before = next(totaldata)    # Grab the first value from it seperately
    for after in totaldata:     # Continue from the next one
        if after["timestamp"] - before["timestamp"] == duration:    # If the next value has the expected timestamp given the duration
            yield { **before, 'periodValue': (after['value'] - before['value']) }  # periodValue is the simple difference
        elif duration == "monthly" and timedelta(days=28) <= (after["timestamp"] - before["timestamp"]) <= timedelta(days=31):  # If its monthly and the next timestamp is within a month
            yield { **before, 'periodValue': (after['value'] - before['value']) }  # periodValue is the simple difference
        else:   # If there is a gap in the data
            yield from interpolate(before, after, duration)
        before = after  # Ready for the next round
    yield { **after, 'periodValue': None }  # Since we don't have the next totalValue, just provide None


readings = [
    {
        'timestamp': date(2021,12,31),
        'value': 0.05,
        'status': 0
    },
    {
        'timestamp': date(2022,1,1),
        'value': 0.1,
        'status': 0
    },
    {
        'timestamp': date(2022,1,7),
        'value': 0.7,
        'status': 0
    },
    {
        'timestamp': date(2022,1,8),
        'value': 1.0,
        'status': 0
    },
]

r = perioddata(readings,timedelta(days=1))
for a in r:
    print(a)


readings = [
    {
        'timestamp': date(2019,10,1),
        'value': 0.5,
        'status': 0
    },
    {
        'timestamp': date(2019,11,1),
        'value': 1.0,
        'status': 0
    },
    {
        'timestamp': date(2020,2,1),
        'value': 4.0,
        'status': 0
    },
    {
        'timestamp': date(2020,5,1),
        'value': 7.0,
        'status': 0
    },
]

r = perioddata(readings,"monthly")
for a in r:
    print(a)