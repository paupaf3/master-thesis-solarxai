from datetime import datetime
from zoneinfo import ZoneInfo
import math


def _localize_time(ts: datetime, tz_name: str | None):
    """Return datetime localized to the plant timezone and its UTC offset (minutes)."""
    if tz_name is None:
        offset_minutes = ts.utcoffset().total_seconds() / 60 if ts.tzinfo else 0
        return ts, offset_minutes

    tz = ZoneInfo(tz_name)
    if ts.tzinfo is None:
        ts_local = ts.replace(tzinfo=tz)
    else:
        ts_local = ts.astimezone(tz)
    offset_minutes = ts_local.utcoffset().total_seconds() / 60 if ts_local.utcoffset() else 0
    return ts_local, offset_minutes


def calculate_poa_irradiance(ts: datetime, lat: float = 41.61954318671897,
                             lon: float | None = None, tz_name: str | None = None) -> float:
    """
    Simple POA irradiance estimate with solar position from latitude/longitude and timezone.
    Uses equation of time for true solar time; clamps to 0 when sun below horizon.
    """
    ts_local, offset_minutes = _localize_time(ts, tz_name)

    day_of_year = ts_local.timetuple().tm_yday
    local_minutes = ts_local.hour * 60 + ts_local.minute + ts_local.second / 60

    # Equation of time (minutes)
    b = math.radians(360 * (day_of_year - 81) / 365)
    eot = 9.87 * math.sin(2 * b) - 7.53 * math.cos(b) - 1.5 * math.sin(b)

    lon_correction = 4 * lon if lon is not None else 0
    time_offset = eot + lon_correction - offset_minutes
    true_solar_minutes = local_minutes + time_offset
    solar_hour = true_solar_minutes / 60

    declination = 23.45 * math.sin(math.radians(360 / 365 * (284 + day_of_year)))
    hour_angle = 15 * (solar_hour - 12)

    zenith_angle = math.degrees(math.acos(
        math.sin(math.radians(lat)) * math.sin(math.radians(declination)) +
        math.cos(math.radians(lat)) * math.cos(math.radians(declination)) * math.cos(math.radians(hour_angle))
    ))

    if zenith_angle > 90:
        return 0.0

    max_irradiance = 1000
    air_mass = 1 / math.cos(math.radians(zenith_angle))
    poa = max_irradiance * math.exp(-0.1 * (air_mass - 1))
    return poa