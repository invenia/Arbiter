def timedelta_to_seconds(delta):
    """
    Takes a timedelta and returns the total
    seconds as a float with microsecond resolution.

    Needed for 2.6 compatibility cause 2.6 doesn't support total_seconds()
    """
    try:
        return delta.total_seconds()
    except AttributeError:
        return (
            delta.microseconds + (
                delta.seconds + delta.days * 24 * 3600
            ) * 10 ** 6
        ) / float(10 ** 6)
