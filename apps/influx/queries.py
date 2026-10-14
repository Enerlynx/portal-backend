# apps/influx/queries.py
# All InfluxDB Flux queries live here.
# Views call these functions — no Flux anywhere else in the codebase.
# Each function returns clean Python dicts, no InfluxDB objects leak out.

from datetime import datetime, timezone, timedelta

from rest_framework import fields
from .client import get_influx_client
import os
import re

INFLUX_ORG = os.getenv('INFLUX_ORG')

# CEA (Central Electricity Authority) all-India weighted average grid
# emission factor, FY 2024-25, CO2 Baseline Database User Guide V21.0
# (Nov 2025). Update when CEA publishes a new edition.
CO2_AVOIDED_FACTOR_KG_PER_KWH = 0.71

MIN_POA_KWH_M2_FOR_PR = 0.3

# Determine when to show device is offline
STALE_AFTER_SECONDS = 180

# STALE_AFTER_SECONDS, tighter than TIMELINE_GAP_SECONDS (300, faults timeline).
INVERTER_STATUS_LOOKBACK_SECONDS = 240

# For Generation start stop
GENERATION_STOP_GAP_SECONDS = 120

# Minimum active power to consider generating
EXPORT_THRESHOLD_KW = -1.0

# REGISTER-mode inverters: ignore energy_today_kw points in the first minutes
# after IST midnight — the register resets ~00:01, and a point carrying
# yesterday's final value must never be read as today's.
INVERTER_TODAY_REGISTER_GRACE_SECONDS = 300

# REGISTER-mode inverters: inverter clocks drift vs IST — some reset their
# daily register a minute or two BEFORE IST midnight. End the day's register
# read this many seconds early so last() is the day's total, not the reset 0.
INVERTER_TODAY_REGISTER_END_GUARD_SECONDS = 900

# -------- Inverter status mapping ----------
# Canonical inverter_status codes (single-value, per handoff spec):
#   0=Stopped (normal night state, NOT a fault), 1=Running, 2=Standby,
#   4=Warning, 8=Fault. Multi-bit combos aren't emitted yet → 'Unknown'.
_INVERTER_STATUS_LABELS = {0: 'Stopped', 1: 'Running', 2: 'Standby', 4: 'Warning', 8: 'Fault'}

# Fixed output buckets for the plant-overview state tally (online subset only).
# Absent/unrecognized status → 'other', so the six always sum to `online`.
_INVERTER_STATE_KEYS = {0: 'stopped', 1: 'running', 2: 'standby', 4: 'warning', 8: 'fault'}

# Standard inverter faults: (bit of `fault_code`, code, label, category).
# Mirrors GENERIC_FAULT_BITS + CATEGORY_MEMBERS in the gateway Lua - keep in
# lockstep. Never renumber a bit; only append new ones.
_INVERTER_FAULTS = [
    (0,  'grid_overvoltage',  'Grid overvoltage',  'grid'),
    (1,  'grid_undervoltage', 'Grid undervoltage', 'grid'),
    (2,  'grid_frequency',    'Grid frequency',    'grid'),
    (3,  'grid_fault',        'Grid fault',        'grid'),
    (4,  'dc_overvoltage',    'DC overvoltage',    'electrical'),
    (5,  'ac_overcurrent',    'AC overcurrent',    'electrical'),
    (6,  'ground_fault',      'Ground fault',      'electrical'),
    (7,  'leakage_current',   'Leakage current',   'electrical'),
    (8,  'arc_fault',         'Arc fault',         'electrical'),
    (9,  'over_temperature',  'Over temperature',  'thermal'),
    (10, 'insulation_fault',  'Insulation fault',  'electrical'),
    (11, 'internal_fault',    'Internal fault',    'hardware'),
    (12, 'generic_fault',     'Unspecified fault', 'generic'),
]


def _inverter_status_label(code):
    return _INVERTER_STATUS_LABELS.get(code, 'Unknown')


# ── Timezone Helper ────────────────────────────────────────────────────────────

def get_ist_midnight_utc():
    """
    Returns today's IST midnight as a UTC datetime string for Flux range().
    IST = UTC+5:30, so IST midnight = UTC previous day 18:30.
    eg: IST 2026-06-03 00:00 = UTC 2026-06-02 18:30
    """
    ist          = timezone(timedelta(hours=5, minutes=30))
    now_ist      = datetime.now(ist)
    midnight_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_utc = midnight_ist.astimezone(timezone.utc)
    return midnight_utc.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_n_days_ago_midnight_utc(n):
    """
    Returns IST midnight n days ago as UTC string.
    Used for the 7-day daily energy query.
    """
    ist          = timezone(timedelta(hours=5, minutes=30))
    now_ist      = datetime.now(ist)
    past_ist     = now_ist - timedelta(days=n)
    midnight_ist = past_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_utc = midnight_ist.astimezone(timezone.utc)
    return midnight_utc.strftime('%Y-%m-%dT%H:%M:%SZ')


# Helper for checking if data is live :
def _is_fresh(record_time):
    """True if a record's timestamp is within the staleness threshold."""
    if record_time is None:
        return False
    age = (datetime.now(timezone.utc) - record_time).total_seconds()
    return age <= STALE_AFTER_SECONDS


def _is_status_recent(record_time):
    """
    True if an inverter_status point is within the (wider) status lookback
    window. Used to keep a flaky inverter's last-known state instead of nulling
    it when it skips a status cycle. Callers must still confirm the inverter is
    online before trusting it — offline always wins (no stuck last state).
    """
    if record_time is None:
        return False
    age = (datetime.now(timezone.utc) - record_time).total_seconds()
    return age <= INVERTER_STATUS_LOOKBACK_SECONDS


def _last_fresh_value(results, field):
    """
    The 'Now' value for a trend stat: the most recent non-null reading for
    `field`, but ONLY if that reading is fresh (its timestamp within
    STALE_AFTER_SECONDS of now). Returns None when the latest point is stale —
    i.e. the device is offline, or the chart is a past date. Stops "Now" from
    showing a value hours after the data actually stopped.

    Walks newest-first, so it's O(1) whenever the series is live (common case).
    `results` is time-sorted ascending; each point carries an ISO-8601 'time'
    string (UTC, +00:00) from the trend query. max/min/mean stay ungated —
    those describe the day and remain true while the device is offline.
    """
    for point in reversed(results):
        val = point.get(field)
        if val is None:
            continue
        point_time = datetime.fromisoformat(point['time'])
        return round(val, 2) if _is_fresh(point_time) else None
    return None
# ── Overview Queries ───────────────────────────────────────────────────────────

def _query_inverter_snapshot(query_api, bucket, site_id, inverter_ids):
    """
    Internal: fetches latest values for all inverters.
    Returns { 'inverter1': { 'ac_active_power_kw': 35.0, ... }, ... }
    """
    device_filter = ' or '.join(
        [f'r.device == "{d}"' for d in inverter_ids]
    )

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) =>
                r._field == "ac_active_power_kw" or
                r._field == "energy_daily_kwh"    or
                r._field == "grid_frequency_hz"         or
                r._field == "ac_power_factor"           or
                r._field == "internal_temp_c" or
                r._field == "inverter_efficiency_pct"   or
                r._field == "ac_reactive_power_kvar"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    tables      = query_api.query(flux, org=INFLUX_ORG)
    device_data = {}
    device_times = {}

    for table in tables:
        for record in table.records:
            time = record.get_time()
            if not _is_fresh(time):
                continue

            device = record.values.get('device')
            field  = record.get_field()
            value  = record.get_value()

            if device not in device_data:
                device_data[device] = {}
            device_data[device][field] = value

            if device not in device_times or time > device_times[device]:
                device_times[device] = time

    return device_data, device_times


def _query_meter_snapshot(query_api, bucket, site_id, meter_id):
    """
    Internal: fetches latest values from the plant meter.
    Returns { 'current_phase_a': 3.14, 'voltage_line_ab_v': 10.9, ... }
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) =>
                r._field == "current_phase_a"        or
                r._field == "current_phase_b"        or
                r._field == "current_phase_c"        or
                r._field == "voltage_line_ab_v"      or
                r._field == "voltage_line_bc_v"      or
                r._field == "voltage_line_ca_v"      or
                r._field == "reactive_power_total_kvar"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    tables      = query_api.query(flux, org=INFLUX_ORG)
    meter_data  = {}

    for table in tables:
        for record in table.records:
            if not _is_fresh(record.get_time()):
                continue
            meter_data[record.get_field()] = record.get_value()

    return meter_data


def _query_power_trend(query_api, bucket, site_id, inverter_ids, interval_minutes):
    """
    Internal: fetches today's power trend aggregated by interval.
    Returns [ { 'time': '...', 'total_kw': 104.0 }, ... ]
    """
    device_filter = ' or '.join(
        [f'r.device == "{d}"' for d in inverter_ids]
    )
    start = get_ist_midnight_utc()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => r._field == "ac_active_power_kw")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: {interval_minutes}m, fn: mean, createEmpty: false)
            |> pivot(rowKey: ["_time"], columnKey: ["device"], valueColumn: "_value")
    '''

    tables  = query_api.query(flux, org=INFLUX_ORG)
    results = []

    for table in tables:
        for record in table.records:
            total = sum(
                record.values.get(d) or 0.0
                for d in inverter_ids
            )
            results.append({
                'time':     record.get_time().isoformat(),
                'total_kw': round(total, 2),
            })

    results.sort(key=lambda x: x['time'])
    return results


def _query_breaker_live(query_api, bucket, site_id, device_id):
    """
    Main breaker + service status via Grafana-parity logic.

    Fetches di_01 (breaker on), di_02 (breaker off), di_04 (breaker trip),
    and di_03 (in service) in one pivoted query, then decodes each signal on
    the record's OWN timestamp — a point older than STALE_AFTER_SECONDS is
    treated as offline, exactly like Grafana's date.sub(now(), 15m) check.

    Two independent signals, surfaced as two fields:
        breaker_status : 'on' | 'trip' | 'off' | 'offline' | None
        service_status : 'in_service' | 'out_of_service' | 'offline' | None

    Precedence on breaker_status matches Grafana: trip wins over on.

    Returns {} only if no data at all in the window (device never seen).
    """
    flux = f'''
        import "date"

        from(bucket: "{bucket}")
            |> range(start: -15m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) =>
                r._field == "di_01" or
                r._field == "di_02" or
                r._field == "di_03" or
                r._field == "di_04"
            )
            |> last()
            |> pivot(rowKey: ["device", "_time"], columnKey: ["_field"], valueColumn: "_value")
            |> map(fn: (r) => ({{
                _time: r._time,
                breaker_code:
                  if r._time < date.sub(from: now(), d: {STALE_AFTER_SECONDS}s) then 3
                  else if exists r.di_04 and int(v: r.di_04) == 1 then 2
                  else if exists r.di_01 and int(v: r.di_01) == 1 then 1
                  else 0,
                service_code:
                  if r._time < date.sub(from: now(), d: {STALE_AFTER_SECONDS}s) then 2
                  else if exists r.di_03 and int(v: r.di_03) == 1 then 1
                  else 0,
            }}))
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    breaker_code = None
    service_code = None
    for table in tables:
        for record in table.records:
            breaker_code = int(record.values.get('breaker_code'))
            service_code = int(record.values.get('service_code'))

    if breaker_code is None:
        return {}

    breaker_map = {0: 'off', 1: 'on', 2: 'trip', 3: 'offline'}
    service_map = {0: 'out_of_service', 1: 'in_service', 2: 'offline'}

    return {
        'breaker_status': breaker_map.get(breaker_code),
        'service_status': service_map.get(service_code),
    }


def get_dashboard_overview(bucket, site_id, inverter_ids, meter_id, interval_minutes=5):
    """
    Main dashboard overview — single function, three internal queries.
    Returns everything the plant overview page needs in one call.

    Args:
        bucket           : InfluxDB bucket (from Customer.influx_bucket)
        site_id          : site tag (from Site.influx_site_id)
        inverter_ids     : list of inverter device tags
        meter_id         : plant meter device tag
        interval_minutes : power trend aggregation window

    Returns a single dict with plant, grid, inverters and power_trend.
    """
    client    = get_influx_client()
    query_api = client.query_api()

    try:
        # Fire all three queries
        inverter_data, inverter_times = _query_inverter_snapshot(
            query_api, bucket, site_id, inverter_ids
        )
        meter_data = _query_meter_snapshot(
            query_api, bucket, site_id, meter_id
        )
        power_trend = _query_power_trend(
            query_api, bucket, site_id, inverter_ids, interval_minutes
        )

        client.close()

        # ── Aggregate inverter values ──────────────────────────────────────────
        total_active_power  = 0.0
        total_daily_gen     = 0.0
        grid_freq           = None
        power_factor        = None
        last_updated        = None

        inverter_list = []

        for device_id in inverter_ids:
            fields = inverter_data.get(device_id, {})
            t      = inverter_times.get(device_id)

            total_active_power += fields.get('ac_active_power_kw', 0.0)
            total_daily_gen    += fields.get('energy_daily_kwh', 0.0)

            if grid_freq is None:
                grid_freq    = fields.get('grid_frequency_hz')
            if power_factor is None:
                power_factor = fields.get('ac_power_factor')
            if last_updated is None or (t and t > last_updated):
                last_updated = t

            inverter_list.append({
                'device_id':       device_id,
                'active_power_kw': round(fields.get('ac_active_power_kw', 0.0), 2),
                'daily_gen_kwh':   round(fields.get('energy_daily_kwh', 0.0), 3),
                'internal_temp_c':    round(fields.get('internal_temp_c', 0.0), 1),
                'inverter_efficiency_pct':      round(fields.get('inverter_efficiency_pct', 0.0), 1),
                'status':          'online' if fields else 'offline',
                'last_updated':    t.isoformat() if t else None,
            })

        # ── Assemble final response ────────────────────────────────────────────
        return {
            'last_updated': last_updated.isoformat() if last_updated else None,

            'plant': {
                'active_power_kw':    round(total_active_power, 2),
                'reactive_power_kvar': round(meter_data.get('reactive_power_total_kvar', 0.0), 2),
                'energy_today_kwh':   round(total_daily_gen, 3),
                'frequency_hz':       round(grid_freq, 2) if grid_freq else None,
                'power_factor':       round(power_factor, 2) if power_factor else None,
            },

            'grid': {
                'current_a':    round(meter_data.get('current_phase_a', 0.0), 2),
                'current_b':    round(meter_data.get('current_phase_b', 0.0), 2),
                'current_c':    round(meter_data.get('current_phase_c', 0.0), 2),
                'voltage_ab':   round(meter_data.get('voltage_line_ab_v', 0.0), 3),
                'voltage_bc':   round(meter_data.get('voltage_line_bc_v', 0.0), 3),
                'voltage_ca':   round(meter_data.get('voltage_line_ca_v', 0.0), 3),
            },

            'inverters':    inverter_list,
            'power_trend':  power_trend,
        }

    except Exception as e:
        client.close()
        raise Exception(f'Dashboard overview query failed: {str(e)}')


# ── Daily Energy (Separate Endpoint) ──────────────────────────────────────────

def get_daily_energy(bucket, site_id, meter_id, days=7):
    """
    Returns daily generation for the last N days.
    Uses meter1 energy_active_export_kwh — calculates last - first per day.
    Matches exactly the Grafana query logic.
    IST timezone aware.

    Returns:
    [
        { 'date': '2026-05-31', 'energy_kwh': 6250.0 },
        { 'date': '2026-06-01', 'energy_kwh': 6290.0 },
        ...
    ]
    """
    client    = get_influx_client()
    query_api = client.query_api()

    start = get_n_days_ago_midnight_utc(days - 1)
    ist_offset = '5h30m'

    # Matches the Grafana query exactly:
    # daily = last(energy_export) - first(energy_export) per day
    flux = f'''
        import "date"
        import "timezone"

        option location = timezone.fixed(offset: {ist_offset})

        day_first = from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "energy_active_export_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: 1d, fn: first, createEmpty: false, timeSrc: "_start")
            |> map(fn: (r) => ({{r with _field: "v_first"}}))

        day_last = from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "energy_active_export_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: 1d, fn: last, createEmpty: false, timeSrc: "_start")
            |> map(fn: (r) => ({{r with _field: "v_last"}}))

        union(tables: [day_first, day_last])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> map(fn: (r) => ({{
                _time  : r._time,
                _value : r.v_last - r.v_first,
                _field : "daily_generation_kwh"
            }}))
            |> keep(columns: ["_time", "_value", "_field"])
    '''

    try:
        tables  = query_api.query(flux, org=INFLUX_ORG)
        client.close()

        results = []
        for table in tables:
            for record in table.records:
                # Convert UTC time to IST date for display
                utc_time  = record.get_time()
                ist_tz    = timezone(timedelta(hours=5, minutes=30))
                ist_time  = utc_time.astimezone(ist_tz)

                results.append({
                    'date':       ist_time.strftime('%Y-%m-%d'),
                    'energy_kwh': round(record.get_value(), 2),
                })

        results.sort(key=lambda x: x['date'])
        return results

    except Exception as e:
        client.close()
        raise Exception(f'Daily energy query failed: {str(e)}')


# ── System Health (unchanged from before) ─────────────────────────────────────

def get_system_health(bucket, site_id, device_id, range_hours=3):
    client    = get_influx_client()
    query_api = client.query_api()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -{range_hours}h)
            |> filter(fn: (r) => r._measurement == "system_health")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) =>
                r._field == "cpu_percent" or
                r._field == "cpu_temp"    or
                r._field == "ram_percent" or
                r._field == "heartbeat"
            )
    '''

    try:
        tables  = query_api.query(flux, org=INFLUX_ORG)
        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    'time':  record.get_time().isoformat(),
                    'field': record.get_field(),
                    'value': record.get_value(),
                })
        client.close()
        return results
    except Exception as e:
        client.close()
        raise Exception(f'System health query failed: {str(e)}')


def get_latest_system_health(bucket, site_id, device_id):
    client    = get_influx_client()
    query_api = client.query_api()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "system_health")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> last()
    '''

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)
        result = {}
        for table in tables:
            for record in table.records:
                result[record.get_field()] = record.get_value()
                result['last_updated']     = record.get_time().isoformat()
        client.close()
        return result
    except Exception as e:
        client.close()
        raise Exception(f'System health latest query failed: {str(e)}')
    

# ── Plant Overview Queries ─────────────────────────────────────────────────────

def _query_meter_today_energy(query_api, bucket, site_id, meter_id):
    """
    Internal: calculates today's generation from meter1.
    Uses last - first of energy_active_export_kwh since IST midnight.
    """
    start = get_ist_midnight_utc()

    flux_first = f'''
        from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "energy_active_export_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> first()
    '''

    flux_last = f'''
        from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "energy_active_export_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    first_val = None
    last_val  = None

    tables = query_api.query(flux_first, org=INFLUX_ORG)
    for table in tables:
        for record in table.records:
            first_val = record.get_value()

    tables = query_api.query(flux_last, org=INFLUX_ORG)
    for table in tables:
        for record in table.records:
            last_val = record.get_value()

    if first_val is not None and last_val is not None:
        return round(last_val - first_val, 2)
    return 0.0


def _query_meter_live(query_api, bucket, site_id, meter_id):
    """
    Internal: fetches all live meter1 fields in one query.
    Returns flat dict of field → value.
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -6h)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) =>
                r._field == "active_power_total_kw"     or
                r._field == "reactive_power_total_kvar" or
                r._field == "voltage_line_ab_v"         or
                r._field == "voltage_line_bc_v"         or
                r._field == "voltage_line_ca_v"         or
                r._field == "current_phase_a"           or
                r._field == "current_phase_b"           or
                r._field == "current_phase_c"           or
                r._field == "grid_frequency_hz"         or
                r._field == "power_factor_total"        or
                r._field == "energy_active_export_kwh"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    tables     = query_api.query(flux, org=INFLUX_ORG)
    meter_data = {}      # fresh only — instantaneous fields
    meter_last = {}      # last-known regardless of freshness — for cumulative counters
    meter_time = None

    for table in tables:
        for record in table.records:
            t     = record.get_time()
            field = record.get_field()
            value = record.get_value()

            if meter_time is None or t > meter_time:
                meter_time = t          # last-seen, regardless of freshness

            meter_last[field] = value   # always keep last-known
            if _is_fresh(t):
                meter_data[field] = value  # fresh subset

    return meter_data, meter_time, meter_last


def _query_data_logger(query_api, bucket, site_id):
    """
    Internal: data-logger heartbeat for one site.
      online    = last heartbeat within STALE_AFTER_SECONDS (120s).
      last_seen = most recent heartbeat within -3h, regardless of freshness, so an
                  offline logger still reports when it last checked in.
    Presence-only: the value (always 1) is ignored, only recency matters. Different
    measurement ("device_info") and device ("data_logger") from solar_data, so it's
    its own query rather than folded into the meter/inverter reads.
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -3h)
            |> filter(fn: (r) => r._measurement == "device_info")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "data_logger")
            |> filter(fn: (r) => r._field == "heartbeat")
            |> last()
    '''

    tables      = query_api.query(flux, org=INFLUX_ORG)
    logger_time = None
    for table in tables:
        for record in table.records:
            t = record.get_time()
            if logger_time is None or (t and t > logger_time):
                logger_time = t

    return _is_fresh(logger_time), logger_time


def _query_inverter_status(query_api, bucket, site_id, inverter_ids):
    """
    Internal: per-inverter live snapshot for the plant overview.
      device_data  — fresh only (ac_active_power_kw, dc_input_power_kw,
                     inverter_status); drives online/offline and the
                     instantaneous values.
      device_last  — last-known energy_total_kwh, UNGATED, so the lifetime
                     counter persists while the inverter is offline (same
                     fresh/last-known split as get_inverter_overview).
    Window is -6h (not -10m) so an offline inverter's last counter is still in
    range for the ungated capture; the 120s freshness gate still protects the
    instantaneous fields regardless of window.
    """
    device_filter = ' or '.join(
        [f'r.device == "{d}"' for d in inverter_ids]
    )

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -6h)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) =>
                r._field == "ac_active_power_kw" or
                r._field == "dc_input_power_kw" or
                r._field == "energy_total_kwh" or
                r._field == "fault_code" or
                r._field == "inverter_status"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    tables       = query_api.query(flux, org=INFLUX_ORG)
    device_data  = {}
    device_last  = {}
    device_status = {}
    device_times = {}

    for table in tables:
        for record in table.records:
            device = record.values.get('device')
            field  = record.get_field()
            value  = record.get_value()
            time   = record.get_time()

            # Lifetime counter: keep last-known regardless of age (persists offline).
            if field == 'energy_total_kwh':
                device_last.setdefault(device, {})[field] = value

            if field == 'inverter_status':
                if _is_status_recent(time):
                    device_status[device] = (value, time)
                continue

            # Instantaneous fields: fresh only.
            if not _is_fresh(time):
                continue

            if device not in device_data:
                device_data[device] = {}
            device_data[device][field] = value

            if device not in device_times or time > device_times[device]:
                device_times[device] = time

    return device_data, device_last, device_status, device_times


def _query_plant_power_trend(query_api, bucket, site_id, meter_id, start, end, interval_minutes):
    """
    Internal: fetches meter1 active_power_total_kw trend.
    start/end are UTC timestamp strings.
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "active_power_total_kw")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: {interval_minutes}m, fn: mean, createEmpty: false)
    '''

    tables  = query_api.query(flux, org=INFLUX_ORG)
    results = []

    for table in tables:
        for record in table.records:
            raw = record.get_value() or 0.0
            results.append({
                'time':     record.get_time().isoformat(),
                'active_power_total_kw': round(-raw, 2) if raw < 0 else 0.0,
            })

    results.sort(key=lambda x: x['time'])
    return results


def _query_irradiance_trend(query_api, bucket, site_id, device_id, start, end, interval_minutes):
    """
    Internal: fetches weather station irradiation_inclined_wm2 trend.
    Same shape/aggregation pattern as _query_plant_power_trend so the two
    series line up on the same interval buckets.
    Returns { time_iso: value } for merging onto the power trend by time.
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) => r._field == "irradiation_inclined_wm2")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: {interval_minutes}m, fn: mean, createEmpty: false)
    '''

    tables        = query_api.query(flux, org=INFLUX_ORG)
    irradiance_map = {}

    for table in tables:
        for record in table.records:
            irradiance_map[record.get_time().isoformat()] = round(record.get_value() or 0.0, 2)

    return irradiance_map

def _query_electrical_trend(query_api, bucket, site_id, meter_id, start, end, interval_minutes):
    """
    Internal: fetches meter1 voltage/current/frequency trend for a whole
    day, aggregated every interval_minutes.
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) =>
                r._field == "voltage_line_ab_v" or
                r._field == "voltage_line_bc_v" or
                r._field == "voltage_line_ca_v" or
                r._field == "current_phase_a"   or
                r._field == "current_phase_b"   or
                r._field == "current_phase_c"   or
                r._field == "grid_frequency_hz"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: {interval_minutes}m, fn: mean, createEmpty: false)
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    points_by_time = {}

    for table in tables:
        for record in table.records:
            t = record.get_time().isoformat()
            field = record.get_field()
            points_by_time.setdefault(t, {'time': t})
            points_by_time[t][field] = round(record.get_value() or 0.0, 2)

    field_names = [
        'voltage_line_ab_v', 'voltage_line_bc_v', 'voltage_line_ca_v',
        'current_phase_a', 'current_phase_b', 'current_phase_c',
        'grid_frequency_hz',
    ]
    results = []
    for t in sorted(points_by_time.keys()):
        point = points_by_time[t]
        for field in field_names:
            point.setdefault(field, 0.0)
        results.append(point)

    return results


def get_plant_electrical_trend(bucket, site_id, meter_id, date_str=None, interval_minutes=5):
    """
    meter1 voltage/current/frequency trend for a selected date.
    Separate endpoint/graph from power+irradiance — min/max/last stats,
    not mean/max/last.
    """
    start_str, end_str = _resolve_ist_date_range(date_str)

    client    = get_influx_client()
    query_api = client.query_api()

    try:
        results = _query_electrical_trend(
            query_api, bucket, site_id, meter_id,
            start_str, end_str, interval_minutes
        )
        client.close()

        field_names = [
            'voltage_line_ab_v', 'voltage_line_bc_v', 'voltage_line_ca_v',
            'current_phase_a', 'current_phase_b', 'current_phase_c',
            'grid_frequency_hz',
        ]

        return {
            'data':  results,
            'stats': {
                field: _trend_stats_minmax(results, field)
                for field in field_names
            },
        }

    except Exception as e:
        client.close()
        raise Exception(f'Plant electrical trend query failed: {str(e)}')


def _trend_stats_minmax(results, field):
    """
    Internal: min/max/last over an already-fetched trend series.
    Used for electrical parameters (voltage/current/frequency) where sags
    and spikes matter more than the average — same convention as Grafana.
    """
    values = [point[field] for point in results]

    if not values:
        return {'min': 0.0, 'max': 0.0, 'last': 0.0}

    return {
        'min':  round(min(values), 2),
        'max':  round(max(values), 2),
        'last': _last_fresh_value(results, field),
    }


def _resolve_ist_date_range(date_str):
    """
    Internal: resolves a 'YYYY-MM-DD' (or None for today) into UTC start/end
    strings for a Flux range() over that full IST day.
    Shared by every whole-day trend query (power, electrical, etc) so the
    date/today/past-day logic lives in exactly one place.
    Returns (start_str, end_str) as '%Y-%m-%dT%H:%M:%SZ' UTC strings.
    """
    ist = timezone(timedelta(hours=5, minutes=30))

    if date_str:
        try:
            requested_date = datetime.strptime(date_str, '%Y-%m-%d')
            requested_date = requested_date.replace(tzinfo=ist)
        except ValueError:
            raise Exception(f'Invalid date format: {date_str}. Use YYYY-MM-DD.')
    else:
        requested_date = datetime.now(ist).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    start_ist = requested_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_ist.astimezone(timezone.utc)

    now_ist   = datetime.now(ist)
    today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

    if start_ist.date() == today_ist.date():
        end_utc = datetime.now(timezone.utc)
    else:
        end_ist = start_ist + timedelta(days=1)
        end_utc = end_ist.astimezone(timezone.utc)

    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str   = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    return start_str, end_str


def get_plant_overview(bucket, site_id, inverter_ids, meter_id, weather_device_id=None, dido_device_id=None, dc_capacity_kw=None, ac_capacity_kw=None, meter_energy_offset_kwh=0.0, daily_generation_target_kwh=None, transformer_device_id=None, target_cuf_pct=None,  grid_meter_id=None, meter_site_id=None, grid_site_id=None, register_inverter_ids=None):
    """
    Plant overview — single function, four or five internal queries.
    Returns everything for the plant overview page stat cards,
    grid values, inverter status table, device summary and weather snapshot.

    weather_device_id is optional — sites without an active weather station
    pass None and the weather query is skipped entirely (no wasted call).
    Weather never fails the endpoint: offline/missing station returns zeros
    with status 'offline', same as any other offline device on this page.

    Returns:
    {
        last_updated,
        plant: {
            active_power_kw,
            energy_today_kwh,
            frequency_hz,
            power_factor,
        },
        grid: {
            voltage_ab, voltage_bc, voltage_ca,
            current_a, current_b, current_c,
        },
        inverters: [ { name, device_id, active_power_kw, daily_gen_kwh,
                       total_gen_kwh, status, inverter_status, last_updated } ],
        device_summary: { total, online, offline },
        weather: {
            irradiation_inclined_wm2, ambient_temp_c, module_temp_c, status,
        },
    }
    """
    client    = get_influx_client()
    query_api = client.query_api()

    try:
        # site_id = the PLANT (inverters, weather, transformer, logger all live
        # here). The reference and grid meters may live on a SUBSTATION whose
        # site tag differs — meter1 is not unique across sites, so each meter
        # carries its own site tag. Default both to the plant when unset.
        if meter_site_id is None:
            meter_site_id = site_id
        if grid_meter_id is None:
            grid_meter_id = meter_id
            grid_site_id  = meter_site_id
        if grid_site_id is None:
            grid_site_id = site_id

        ref_live, ref_time, ref_last = _query_meter_live(query_api, bucket, meter_site_id, meter_id)
        if grid_meter_id == meter_id and grid_site_id == meter_site_id:
            grid_live, grid_time = ref_live, ref_time
        else:
            grid_live, grid_time, _grid_last = _query_meter_live(query_api, bucket, grid_site_id, grid_meter_id)
        meter_is_live = bool(grid_live)

        meter_is_live = bool(grid_live)          # grid meter liveness drives meter.status
        energy_today = _query_meter_today_energy(query_api, bucket, meter_site_id, meter_id)
        inv_data, inv_last, inv_status, inv_times = _query_inverter_status(query_api, bucket, site_id, inverter_ids)
        inv_today_by_device = _query_inverters_today_energy(
            query_api, bucket, site_id, inverter_ids, register_ids=register_inverter_ids
        )

        weather_fields = {}
        weather_time   = None
        poa_wh_m2      = 0.0
        if weather_device_id:
            weather_fields, weather_time = _query_weather_live(query_api, bucket, site_id, weather_device_id)
            poa_wh_m2      = _query_poa_irradiation(
                query_api, bucket, site_id, weather_device_id, get_ist_midnight_utc()
            )

        transformer_fields = {}
        transformer_time   = None
        if transformer_device_id:
            transformer_fields, transformer_time = _query_transformer_live(
                query_api, bucket, site_id, transformer_device_id
            )

        # Build the weather block only when the site actually has a station.
        # None = no station (frontend hides the section); an object with
        # status:'offline' = station exists but not live right now (still render).
        weather_block = None
        if weather_device_id:
            weather_block = {
                'irradiation_inclined_wm2': round(weather_fields.get('irradiation_inclined_wm2', 0.0), 2),
                'ambient_temp_c':           round(weather_fields.get('ambient_temp_c', 0.0), 2),
                'module_temp_c':            round(weather_fields.get('module_temp_c', 0.0), 2),
                'status':                   'online' if weather_fields else 'offline',
                'last_updated':             weather_time.isoformat() if weather_time else None,
            }
        transformer_block = None
        if transformer_device_id:
            transformer_block = {
                'transformer_oil_temp_c': (
                    round(transformer_fields['transformer_oil_temp_c'], 2)
                    if 'transformer_oil_temp_c' in transformer_fields else None
                ),
                'transformer_winding_temp_c': (
                    round(transformer_fields['transformer_winding_temp_c'], 2)
                    if 'transformer_winding_temp_c' in transformer_fields else None
                ),
                'status':       'online' if transformer_fields else 'offline',
                'last_updated': transformer_time.isoformat() if transformer_time else None,
            }

        breaker_fields = {}
        if dido_device_id:
            breaker_fields = _query_breaker_live(query_api, bucket, site_id, dido_device_id)

        generation_window = _query_plant_generation_window(query_api, bucket, meter_site_id, meter_id)

        logger_online, logger_time = _query_data_logger(query_api, bucket, site_id)

        client.close()

        # Build inverter list
        inverter_list = []
        online_count  = 0
        total_dc_power = 0.0
        state_counts  = {'running': 0, 'stopped': 0, 'standby': 0,
                         'warning': 0, 'fault': 0, 'other': 0}

        for device_id in inverter_ids:
            fields    = inv_data.get(device_id, {})
            last_flds = inv_last.get(device_id, {})
            t         = inv_times.get(device_id)
            is_online = bool(fields)

            # Device state (orthogonal to online/offline): last-known status from
            # the wider window, surfaced ONLY when the inverter is online — a flaky
            # inverter that skipped a status cycle keeps its state; an offline one
            # reports None (offline wins).
            inverter_status = None
            if is_online:
                status_rec = inv_status.get(device_id)
                if status_rec is not None:
                    code = int(round(status_rec[0]))
                    inverter_status = {'code': code, 'label': _inverter_status_label(code)}

            if is_online:
                online_count += 1
                total_dc_power += fields.get('dc_input_power_kw', 0.0)
                # State tally — online subset only (offline reports no state;
                # covered by device_summary.offline). Absent/unrecognized → 'other'.
                key = _INVERTER_STATE_KEYS.get(
                    inverter_status['code'] if inverter_status else None, 'other'
                )
                state_counts[key] += 1

            # Live faults: decoded from a FRESH fault_code only, no history.
            # None = offline or fault_code not published (site not mapped yet);
            # [] = reporting, no fault. Independent of inverter_status.
            faults = None
            if is_online and fields.get('fault_code') is not None:
                word = int(round(fields['fault_code']))
                faults = [
                    {'code': code, 'label': label, 'category': category}
                    for bit, code, label, category in _INVERTER_FAULTS
                    if (word >> bit) & 1
                ]

            # e_today: calculated last−first (not the retired energy_daily_kwh).
            # e_total: last-known lifetime, ungated → persists while offline.
            daily_gen = abs(inv_today_by_device.get(device_id, 0.0))
            total_gen = abs(last_flds.get('energy_total_kwh', 0.0))

            inverter_list.append({
                'device_id':       device_id,
                'active_power_kw': round(fields.get('ac_active_power_kw', 0.0), 2),
                'daily_gen_kwh':   round(daily_gen, 3),
                'total_gen_kwh':   round(total_gen, 2),
                'status':          'online' if is_online else 'offline',
                'inverter_status': inverter_status,
                'faults':          faults,
                'last_updated':    t.isoformat() if t else None,
            })

        total_inverters = len(inverter_ids)
        poa_kwh_m2      = round(poa_wh_m2 / 1000.0, 4)
        # Plant Overview only: raw meter value is negative during export (normal
        # generation). We only want to show 0 when the meter is drawing power
        # (importing, i.e. raw value positive) rather than a negative number.
        raw_active_power = ref_live.get('active_power_total_kw', 0.0)
        if raw_active_power > 0:
            active_power_kw = 0.0
        else:
            active_power_kw = round(raw_active_power * -1, 2)

        performance_ratio_pct = None
        if bool(ref_live) and dc_capacity_kw and poa_kwh_m2 >= MIN_POA_KWH_M2_FOR_PR and weather_fields:
            performance_ratio_pct = round(
                (energy_today / (float(dc_capacity_kw) * poa_kwh_m2)) * 100, 2
            )

        cuf_pct = None
        if ac_capacity_kw:
            cuf_pct = round(
                (energy_today / (float(ac_capacity_kw) * 24)) * 100, 2
            )

        co2_avoided_today_kg = round(energy_today * CO2_AVOIDED_FACTOR_KG_PER_KWH, 2)

        return {
            'last_updated': max(
                (t for t in inv_times.values() if t),
                default=None
            ),

            'plant': {
                'active_power_kw':  active_power_kw,
                'energy_today_kwh': energy_today,
                'frequency_hz':     round(grid_live.get('grid_frequency_hz', 0.0), 2),
                'power_factor':     round(grid_live.get('power_factor_total', 0.0), 2),
                'reactive_power_kvar': round(grid_live.get('reactive_power_total_kvar', 0.0), 2),
                'dc_capacity_kw':   float(dc_capacity_kw) if dc_capacity_kw else None,
                'ac_capacity_kw':   float(ac_capacity_kw) if ac_capacity_kw else None,
                'daily_generation_target_kwh': float(daily_generation_target_kwh) if daily_generation_target_kwh else None,
                'target_cuf_pct': float(target_cuf_pct) if target_cuf_pct else None,
                'energy_active_export_kwh': round(
                    ref_last['energy_active_export_kwh'] + meter_energy_offset_kwh, 2
                ) if 'energy_active_export_kwh' in ref_last else 0.0,
            },

            'grid': {
                'voltage_ab': round(grid_live.get('voltage_line_ab_v', 0.0), 3),
                'voltage_bc': round(grid_live.get('voltage_line_bc_v', 0.0), 3),
                'voltage_ca': round(grid_live.get('voltage_line_ca_v', 0.0), 3),
                'current_a':  round(grid_live.get('current_phase_a', 0.0), 2),
                'current_b':  round(grid_live.get('current_phase_b', 0.0), 2),
                'current_c':  round(grid_live.get('current_phase_c', 0.0), 2),
            },
            'inverters': inverter_list,

            'device_summary': {
                'total':   total_inverters,
                'online':  online_count,
                'offline': total_inverters - online_count,
                'states':  state_counts,
            },

            'meter': {
                'status':       'online' if meter_is_live else 'offline',
                'last_updated': grid_time.isoformat() if grid_time else None,
            },

            'data_logger': {
                'status':    'online' if logger_online else 'offline',
                'last_seen': logger_time.isoformat() if logger_time else None,
            },

            'weather': weather_block,

            'transformer': transformer_block,

            'performance': {
                'performance_ratio_pct':       performance_ratio_pct,
                'cuf_pct':                     cuf_pct,
                'poa_irradiation_kwh_m2':      poa_kwh_m2,
                'dc_power_total_kw':           round(total_dc_power, 2),
                'co2_avoided_today_kg':   co2_avoided_today_kg,
            },
            
            'breaker_status': breaker_fields.get('breaker_status'),
            'service_status': breaker_fields.get('service_status'),
            'generation_window': generation_window,
        }

    except Exception as e:
        client.close()
        raise Exception(f'Plant overview query failed: {str(e)}')

def _trend_stats(results, field):
    """
    Internal: max/mean/last over an already-fetched trend series.
    No Influx call — pure Python over the list get_plant_power_trend already
    has in memory, so this costs nothing extra against the call budget.
    results must be time-sorted (both trend queries already sort by time).
    """
    values = [point[field] for point in results if point[field] is not None]

    if not values:
        return {'max': 0.0, 'mean': 0.0, 'last': 0.0}

    return {
        'max':  round(max(values), 2),
        'mean': round(sum(values) / len(values), 2),
        'last': _last_fresh_value(results, field),
    }


def get_plant_power_trend(bucket, site_id, meter_id, weather_device_id=None, date_str=None, interval_minutes=5, meter_site_id=None):
    """
    Plant power trend for a selected date.
    date_str: 'YYYY-MM-DD' in IST. Defaults to today if not provided.
    Returns meter1 active_power_total_kw aggregated every interval_minutes.

    Returns:
    [
        { 'time': '2026-06-03T04:30:00Z', 'power_kw': 0.0 },
        { 'time': '2026-06-03T05:00:00Z', 'power_kw': 104.5 },
        ...
    ]
    """
    start_str, end_str = _resolve_ist_date_range(date_str)

    # site_id = plant (weather/irradiance lives here). The reference meter may
    # be on a substation whose Influx site tag differs — meter1 is not unique
    # across sites. Default to the plant when the caller doesn't override.
    if meter_site_id is None:
        meter_site_id = site_id

    client    = get_influx_client()
    query_api = client.query_api()

    try:
        power_rows = _query_plant_power_trend(
            query_api, bucket, meter_site_id, meter_id,
            start_str, end_str, interval_minutes
        )
        power_map = {row['time']: row['active_power_total_kw'] for row in power_rows}

        irradiance_map = {}
        if weather_device_id:
            irradiance_map = _query_irradiance_trend(
            query_api, bucket, site_id, weather_device_id,
            start_str, end_str, interval_minutes
            )

        client.close()

        # Time axis is the UNION of both series, not just the meter. Otherwise a
        # dead meter truncates the whole chart even while the weather station is
        # live (and vice-versa). Buckets with no reading stay None so the frontend
        # draws a gap instead of a fake zero.
        all_times = sorted(set(power_map) | set(irradiance_map))
        results = [
            {
                'time':                     t,
                'active_power_total_kw':    power_map.get(t),
                'irradiation_inclined_wm2': irradiance_map.get(t),
            }
            for t in all_times
        ]

        return {
            'data':  results,
            'stats': {
                'active_power_total_kw':    _trend_stats(results, 'active_power_total_kw'),
                'irradiation_inclined_wm2': _trend_stats(results, 'irradiation_inclined_wm2'),
            },
        }

    except Exception as e:
        client.close()
        raise Exception(f'Plant power trend query failed: {str(e)}')
    

# ── Inverter Overview Queries ──────────────────────────────────────────────────


def _query_inverters_today_energy(query_api, bucket, site_id, inverter_ids, register_ids=None):
    """
    Internal: today's generation per inverter. Per-device source (Device.energy_today_source):
      COUNTER  → last − first of energy_total_kwh since IST midnight.
      REGISTER → last() of the inverter's own energy_today_kw since
                 IST midnight + INVERTER_TODAY_REGISTER_GRACE_SECONDS.
    Mixed sites are fine — each group runs only if it has devices.

    Returns { device_id: energy_kwh_today }. Missing devices are omitted
    (caller should .get(id, 0.0)).
    """
    register_ids = set(register_ids or [])
    counter_ids  = [d for d in inverter_ids if d not in register_ids]
    reg_ids      = [d for d in inverter_ids if d in register_ids]

    start  = get_ist_midnight_utc()
    result = {}

    # ── COUNTER inverters: last − first of the lifetime counter ──
    if counter_ids:
        device_filter = ' or '.join([f'r.device == "{d}"' for d in counter_ids])

        flux_first = f'''
            from(bucket: "{bucket}")
                |> range(start: {start})
                |> filter(fn: (r) => r._measurement == "solar_data")
                |> filter(fn: (r) => r.site == "{site_id}")
                |> filter(fn: (r) => {device_filter})
                |> filter(fn: (r) => r._field == "energy_total_kwh")
                |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
                |> group(columns: ["device"])
                |> first()
        '''

        flux_last = f'''
            from(bucket: "{bucket}")
                |> range(start: {start})
                |> filter(fn: (r) => r._measurement == "solar_data")
                |> filter(fn: (r) => r.site == "{site_id}")
                |> filter(fn: (r) => {device_filter})
                |> filter(fn: (r) => r._field == "energy_total_kwh")
                |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
                |> group(columns: ["device"])
                |> last()
        '''

        first_vals = {}
        last_vals  = {}

        for table in query_api.query(flux_first, org=INFLUX_ORG):
            for record in table.records:
                first_vals[record.values.get('device')] = record.get_value()

        for table in query_api.query(flux_last, org=INFLUX_ORG):
            for record in table.records:
                last_vals[record.values.get('device')] = record.get_value()

        for device_id in counter_ids:
            f = first_vals.get(device_id)
            l = last_vals.get(device_id)
            if f is not None and l is not None:
                result[device_id] = round(l - f, 3)

    # ── REGISTER inverters: the inverter's own energy_today_kw ──
    if reg_ids:
        reg_start_dt = (
            datetime.strptime(start, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            + timedelta(seconds=INVERTER_TODAY_REGISTER_GRACE_SECONDS)
        )
        # Inside the grace window the range would start in the future (Flux
        # errors) — skip; register inverters report no value until it passes.
        if datetime.now(timezone.utc) > reg_start_dt:
            reg_start     = reg_start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            device_filter = ' or '.join([f'r.device == "{d}"' for d in reg_ids])

            flux_reg = f'''
                from(bucket: "{bucket}")
                    |> range(start: {reg_start})
                    |> filter(fn: (r) => r._measurement == "solar_data")
                    |> filter(fn: (r) => r.site == "{site_id}")
                    |> filter(fn: (r) => {device_filter})
                    |> filter(fn: (r) => r._field == "energy_today_kw")
                    |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
                    |> group(columns: ["device"])
                    |> last()
            '''

            for table in query_api.query(flux_reg, org=INFLUX_ORG):
                for record in table.records:
                    device = record.values.get('device')
                    value  = record.get_value()
                    if device is not None and value is not None:
                        result[device] = round(value, 3)

    return result


def get_inverter_overview(bucket, site_id, inverter_ids, weather_device_id=None, dc_capacity_kw=None, ac_capacity_kw=None, register_inverter_ids=None):
    """
    Fetches all live inverter data in one query.
    Returns summary (totals) + per inverter breakdown.

    Fields fetched per inverter (live snapshot):
        ac_active_power_kw, energy_total_kwh,
        grid_frequency_hz, ac_power_factor, ac_reactive_power_kvar,
        inverter_efficiency_pct, inverter_status

    Daily energy is NOT read from energy_daily_kwh (unreliable/self-resetting,
    and absent entirely on newer sites). Derived instead as last-minus-first
    of energy_total_kwh since IST midnight, same pattern as the meter.

    PR requires the weather station to be live right now, not just to have
    reported earlier today — poa_irradiation_kwh_m2 is a cumulative integral
    that freezes the moment the station goes offline, while energy keeps
    climbing. Without a freshness check here, a dead station mid-day inflates
    PR past 100% instead of correctly reporting it as unavailable.
    """
    client    = get_influx_client()
    query_api = client.query_api()

    device_filter = ' or '.join(
        [f'r.device == "{d}"' for d in inverter_ids]
    )

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -6h)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) =>
                r._field == "ac_active_power_kw"      or
                r._field == "energy_total_kwh"         or
                r._field == "grid_frequency_hz"        or
                r._field == "ac_power_factor"          or
                r._field == "ac_reactive_power_kvar"   or
                r._field == "inverter_efficiency_pct"  or
                r._field == "inverter_status"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    try:
        tables       = query_api.query(flux, org=INFLUX_ORG)

        poa_kwh_m2     = 0.0
        weather_is_live = False
        if weather_device_id:
            poa_wh_m2  = _query_poa_irradiation(
                query_api, bucket, site_id, weather_device_id, get_ist_midnight_utc()
            )
            poa_kwh_m2 = round(poa_wh_m2 / 1000.0, 4)

            weather_fields, _ = _query_weather_live(query_api, bucket, site_id, weather_device_id)
            weather_is_live = bool(weather_fields)

        daily_energy_by_device = _query_inverters_today_energy(
            query_api, bucket, site_id, inverter_ids, register_ids=register_inverter_ids
        )


        client.close()

        # Collect per device.
        # device_data: fresh only — instantaneous fields, drives online/offline.
        # device_last: last-known ungated — the cumulative energy_total_kwh counter,
        # so Energy Total survives an outage instead of collapsing to 0 (same
        # last-known pattern as the meter).
        device_data  = {}
        device_last  = {}
        device_status = {}
        device_times = {}

        for table in tables:
            for record in table.records:
                device = record.values.get('device')
                field  = record.get_field()
                value  = record.get_value()
                time   = record.get_time()

                # last() yields one point per (device, field) over the 24h window
                # — that's the last-known counter value; keep it regardless of age.
                if field == 'energy_total_kwh':
                    device_last.setdefault(device, {})[field] = value

                # inverter_status: wider window, kept out of device_data (see plant overview).
                if field == 'inverter_status':
                    if _is_status_recent(time):
                        device_status[device] = (value, time)
                    continue

                if not _is_fresh(time):
                    continue

                if device not in device_data:
                    device_data[device] = {}
                device_data[device][field] = value

                if device not in device_times or time > device_times[device]:
                    device_times[device] = time

        # Build per inverter list and totals
        inverter_list        = []
        total_active_power   = 0.0
        total_daily_gen      = 0.0
        online_count         = 0
        state_counts         = {'running': 0, 'stopped': 0, 'standby': 0,
            'warning': 0, 'fault': 0, 'other': 0}

        dc_capacity_per_inverter = (
            float(dc_capacity_kw) / len(inverter_ids)
            if dc_capacity_kw and len(inverter_ids) > 0 else None
        )

        pr_ready = weather_is_live and poa_kwh_m2 >= MIN_POA_KWH_M2_FOR_PR

        for device_id in inverter_ids:
            fields    = device_data.get(device_id, {})
            last_fields = device_last.get(device_id, {})
            t         = device_times.get(device_id)
            is_online = bool(fields)

            inverter_status = None
            if is_online:
                status_rec = device_status.get(device_id)
                if status_rec is not None:
                    code = int(round(status_rec[0]))
                    inverter_status = {'code': code, 'label': _inverter_status_label(code)}

            if is_online:
                online_count += 1
                # State tally — online subset only; sums to online_count.
                key = _INVERTER_STATE_KEYS.get(
                    inverter_status['code'] if inverter_status else None, 'other'
                )
                state_counts[key] += 1

            active_power = abs(round(fields.get('ac_active_power_kw', 0.0), 2))
            daily_gen    = abs(daily_energy_by_device.get(device_id, 0.0))

            total_active_power += active_power
            total_daily_gen    += daily_gen

            inverter_pr_pct = None
            if dc_capacity_per_inverter and pr_ready:
                inverter_pr_pct = round(
                    (daily_gen / (dc_capacity_per_inverter * poa_kwh_m2)) * 100, 2
                )

            inverter_list.append({
                'device_id':               device_id,
                'ac_active_power_kw':      active_power,
                'energy_daily_kwh':        round(daily_gen, 3),
                'energy_total_kwh':        (
                    abs(round(last_fields['energy_total_kwh'], 2))
                    if 'energy_total_kwh' in last_fields else 0.0
                ),
                'ac_reactive_power_kvar':  abs(round(fields.get('ac_reactive_power_kvar', 0.0), 2)),
                'ac_power_factor':         round(fields.get('ac_power_factor', 0.0), 2),
                'grid_frequency_hz':       round(fields.get('grid_frequency_hz', 0.0), 2),
                'inverter_efficiency_pct': round(fields.get('inverter_efficiency_pct', 0.0), 1),
                'performance_ratio_pct':   inverter_pr_pct,
                'status':                  'online' if is_online else 'offline',
                'inverter_status':         inverter_status,
                'last_updated':            t.isoformat() if t else None,
            })

        fleet_pr_pct = None
        if dc_capacity_kw and pr_ready:
            fleet_pr_pct = round(
                (total_daily_gen / (float(dc_capacity_kw) * poa_kwh_m2)) * 100, 2
            )

        return {
            'summary': {
                'total_ac_active_power_kw': round(total_active_power, 2),
                'total_energy_daily_kwh':   round(total_daily_gen, 3),
                'online_count':             online_count,
                'total_count':              len(inverter_ids),
                'states':                   state_counts,
                'performance_ratio_pct':    fleet_pr_pct,
                'poa_irradiation_kwh_m2':   poa_kwh_m2,
                'dc_capacity_kw':           float(dc_capacity_kw) if dc_capacity_kw else None,
                'ac_capacity_kw':           float(ac_capacity_kw) if ac_capacity_kw else None,
            },
            'inverters': inverter_list,
        }

    except Exception as e:
        client.close()
        raise Exception(f'Inverter overview query failed: {str(e)}')
    

def get_inverter_power_trend(bucket, site_id, inverter_ids, weather_device_id=None, date_str=None, interval_minutes=5):
    """
    Inverter power trend for a selected date. Sums ac_active_power_kw across all
    inverters per bucket and overlays weather-station irradiance — same dual-series
    shape, keys and stats as get_plant_power_trend.
    date_str: 'YYYY-MM-DD' in IST. Defaults to today if not provided.

    weather_device_id optional — None skips the irradiance query. Time axis is the
    UNION of both series, so an offline station never truncates the power trace
    (and vice-versa); absent buckets stay None for the frontend to gap.
    """
    ist = timezone(timedelta(hours=5, minutes=30))

    if date_str:
        try:
            requested_date = datetime.strptime(date_str, '%Y-%m-%d')
            requested_date = requested_date.replace(tzinfo=ist)
        except ValueError:
            raise Exception(f'Invalid date format: {date_str}. Use YYYY-MM-DD.')
    else:
        requested_date = datetime.now(ist).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    start_ist = requested_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_ist.astimezone(timezone.utc)

    now_ist   = datetime.now(ist)
    today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

    if start_ist.date() == today_ist.date():
        end_utc = datetime.now(timezone.utc)
    else:
        end_ist = start_ist + timedelta(days=1)
        end_utc = end_ist.astimezone(timezone.utc)

    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str   = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    device_filter = ' or '.join(
        [f'r.device == "{d}"' for d in inverter_ids]
    )

    client    = get_influx_client()
    query_api = client.query_api()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => r._field == "ac_active_power_kw")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: {interval_minutes}m, fn: mean, createEmpty: false)
            |> pivot(rowKey: ["_time"], columnKey: ["device"], valueColumn: "_value")
    '''

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)

        power_map = {}
        for table in tables:
            for record in table.records:
                total = sum(
                    abs(record.values.get(d) or 0.0)
                    for d in inverter_ids
                )
                power_map[record.get_time().isoformat()] = round(total, 2)

        # Irradiance query MUST run before client.close() (closed-client queries
        # return silent empties).
        irradiance_map = {}
        if weather_device_id:
            irradiance_map = _query_irradiance_trend(
                query_api, bucket, site_id, weather_device_id,
                start_str, end_str, interval_minutes
            )

        client.close()

        all_times = sorted(set(power_map) | set(irradiance_map))
        results = [
            {
                'time':                     t,
                'active_power_total_kw':    power_map.get(t),
                'irradiation_inclined_wm2': irradiance_map.get(t),
            }
            for t in all_times
        ]

        return {
            'data':  results,
            'stats': {
                'active_power_total_kw':    _trend_stats(results, 'active_power_total_kw'),
                'irradiation_inclined_wm2': _trend_stats(results, 'irradiation_inclined_wm2'),
            },
        }

    except Exception as e:
        client.close()
        raise Exception(f'Inverter power trend query failed: {str(e)}')


# ── Inverter Detail Page Queries ──────────────────────────────────────────────

def get_inverter_detail(bucket, site_id, device_id, weather_device_id=None, dc_capacity_per_inverter=None, energy_today_register=False):
    """
    Daily energy is derived (last-minus-first of energy_total_kwh since IST
    midnight), not read from energy_daily_kwh — see get_inverter_overview.

    PR requires the weather station to be live right now, not just to have
    reported earlier today — poa_irradiation_kwh_m2 is a cumulative integral
    that freezes the moment the station goes offline, while energy keeps
    climbing. Without a freshness check here, a dead station mid-day inflates
    PR past 100% instead of correctly reporting it as unavailable.
    """
    client    = get_influx_client()
    query_api = client.query_api()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -6h)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) =>
                r._field == "ac_active_power_kw"      or
                r._field == "energy_total_kwh"         or
                r._field == "ac_power_factor"         or
                r._field == "inverter_efficiency_pct" or
                r._field == "grid_frequency_hz"       or
                r._field == "ac_reactive_power_kvar"  or
                r._field == "internal_temp_c"         or
                r._field == "grid_voltage_ab_v"       or
                r._field == "grid_voltage_bc_v"       or
                r._field == "grid_voltage_ca_v"       or
                r._field == "dc_input_voltage_v"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)

        poa_kwh_m2      = 0.0
        weather_is_live = False
        if weather_device_id:
            poa_wh_m2  = _query_poa_irradiation(
                query_api, bucket, site_id, weather_device_id, get_ist_midnight_utc()
            )
            poa_kwh_m2 = round(poa_wh_m2 / 1000.0, 4)

            weather_fields, _ = _query_weather_live(query_api, bucket, site_id, weather_device_id)
            weather_is_live = bool(weather_fields)

        daily_energy_by_device = _query_inverters_today_energy(
            query_api, bucket, site_id, [device_id],
            register_ids=[device_id] if energy_today_register else None,
        )

        client.close()

        fields      = {}      # fresh only — instantaneous fields
        last_fields = {}      # last-known ungated — cumulative counter
        last_time   = None

        for table in tables:
            for record in table.records:
                field = record.get_field()
                value = record.get_value()
                t     = record.get_time()

                # energy_total_kwh: keep last-known so Energy Total survives an
                # outage instead of dropping to 0.
                if field == 'energy_total_kwh':
                    last_fields[field] = value

                if not _is_fresh(t):
                    continue

                fields[field] = value
                if last_time is None or t > last_time:
                    last_time = t

        is_online    = bool(fields)
        active_power = abs(round(fields.get('ac_active_power_kw', 0.0), 2))
        daily_gen    = abs(daily_energy_by_device.get(device_id, 0.0))

        performance_ratio_pct = None
        if dc_capacity_per_inverter and weather_is_live and poa_kwh_m2 >= MIN_POA_KWH_M2_FOR_PR:
            performance_ratio_pct = round(
                (daily_gen / (dc_capacity_per_inverter * poa_kwh_m2)) * 100, 2
            )

        return {
            'device_id':               device_id,
            'ac_active_power_kw':      active_power,
            'energy_daily_kwh':        round(daily_gen, 3),
            'energy_total_kwh':        (
                abs(round(last_fields['energy_total_kwh'], 2))
                if 'energy_total_kwh' in last_fields else 0.0
            ),
            'ac_power_factor':         round(fields.get('ac_power_factor', 0.0), 2),
            'inverter_efficiency_pct': round(fields.get('inverter_efficiency_pct', 0.0), 1),
            'performance_ratio_pct':   performance_ratio_pct,
            'poa_irradiation_kwh_m2':  poa_kwh_m2,
            'grid_frequency_hz':       round(fields.get('grid_frequency_hz', 0.0), 2),
            'ac_reactive_power_kvar':  abs(round(fields.get('ac_reactive_power_kvar', 0.0), 2)),
            'internal_temp_c':         round(fields.get('internal_temp_c', 0.0), 1),
            'grid_voltage_ab_v':       round(fields.get('grid_voltage_ab_v', 0.0), 1),
            'grid_voltage_bc_v':       round(fields.get('grid_voltage_bc_v', 0.0), 1),
            'grid_voltage_ca_v':       round(fields.get('grid_voltage_ca_v', 0.0), 1),
            'dc_input_voltage_v':      round(fields.get('dc_input_voltage_v', 0.0), 2),
            'status':                  'online' if is_online else 'offline',
            'last_updated':            last_time.isoformat() if last_time else None,
        }

    except Exception as e:
        client.close()
        raise Exception(f'Inverter detail query failed: {str(e)}')  

def get_inverter_detail_power_trend(bucket, site_id, device_id, date_str=None, interval_minutes=5):
    """
    Powers both the 'DC Input vs Active Power' and 'Active vs Reactive Power' charts
    on the inverter detail page — one device, one date, one query, three fields.
    """
    ist = timezone(timedelta(hours=5, minutes=30))

    if date_str:
        try:
            requested_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=ist)
        except ValueError:
            raise Exception(f'Invalid date format: {date_str}. Use YYYY-MM-DD.')
    else:
        requested_date = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)

    start_ist = requested_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_ist.astimezone(timezone.utc)

    now_ist   = datetime.now(ist)
    today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

    if start_ist.date() == today_ist.date():
        end_utc = datetime.now(timezone.utc)
    else:
        end_utc = (start_ist + timedelta(days=1)).astimezone(timezone.utc)

    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str   = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    client    = get_influx_client()
    query_api = client.query_api()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) =>
                r._field == "dc_input_power_kw"     or
                r._field == "ac_active_power_kw"    or
                r._field == "ac_reactive_power_kvar"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: {interval_minutes}m, fn: mean, createEmpty: false)
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    try:
        tables  = query_api.query(flux, org=INFLUX_ORG)
        client.close()

        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    'time':                   record.get_time().isoformat(),
                    'dc_input_power_kw':      abs(round(record.values.get('dc_input_power_kw') or 0.0, 2)),
                    'ac_active_power_kw':     abs(round(record.values.get('ac_active_power_kw') or 0.0, 2)),
                    'ac_reactive_power_kvar': abs(round(record.values.get('ac_reactive_power_kvar') or 0.0, 2)),
                })

        results.sort(key=lambda x: x['time'])
        return results

    except Exception as e:
        client.close()
        raise Exception(f'Inverter detail power trend query failed: {str(e)}')
    

def get_inverter_daily_energy(bucket, site_id, device_id, days=7):
    """
    One inverter's own daily generation for the last N days.
    Derived as last - first of energy_total_kwh (lifetime counter) per IST day,
    same pattern as the meter's get_daily_energy. Replaces the old energy_daily_kwh
    max() approach, which is empty on inverters that don't populate that field.
    The most recent day is partial (generation so far today).
    """
    client    = get_influx_client()
    query_api = client.query_api()

    start = get_n_days_ago_midnight_utc(days - 1)
    ist_offset = '5h30m'

    flux = f'''
        import "timezone"

        option location = timezone.fixed(offset: {ist_offset})

        day_first = from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) => r._field == "energy_total_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: 1d, fn: first, createEmpty: false, timeSrc: "_start")
            |> map(fn: (r) => ({{r with _field: "v_first"}}))

        day_last = from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) => r._field == "energy_total_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: 1d, fn: last, createEmpty: false, timeSrc: "_start")
            |> map(fn: (r) => ({{r with _field: "v_last"}}))

        union(tables: [day_first, day_last])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> map(fn: (r) => ({{
                _time  : r._time,
                _value : r.v_last - r.v_first,
                _field : "daily_generation_kwh"
            }}))
            |> keep(columns: ["_time", "_value", "_field"])
    '''

    try:
        tables  = query_api.query(flux, org=INFLUX_ORG)
        client.close()

        results = []
        ist_tz  = timezone(timedelta(hours=5, minutes=30))

        for table in tables:
            for record in table.records:
                ist_time = record.get_time().astimezone(ist_tz)
                results.append({
                    'date':       ist_time.strftime('%Y-%m-%d'),
                    'energy_kwh': round(record.get_value(), 2),
                })

        results.sort(key=lambda x: x['date'])
        return results

    except Exception as e:
        client.close()
        raise Exception(f'Inverter daily energy query failed: {str(e)}')    

def get_all_inverters_pv_strings(bucket, site_id, inverter_ids):
    """
    Fetches PV string currents for every inverter at a site in a single query.
    Returns { 'inverter1': { '01': 11.40, '02': 0.0, ... }, 'inverter2': {...}, ... }
    Stale readings (older than STALE_AFTER_SECONDS) are dropped rather than
    shown as a last-known value, matching the same rule as every other
    live snapshot in this file.
    """
    client    = get_influx_client()
    query_api = client.query_api()

    device_filter = ' or '.join(
        [f'r.device == "{d}"' for d in inverter_ids]
    )

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => r._field =~ /^pv_string_\\d+_current_a$/)
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)
        client.close()

        device_pv_data = {d: {} for d in inverter_ids}

        for table in tables:
            for record in table.records:
                if not _is_fresh(record.get_time()):
                    continue

                device = record.values.get('device')
                match  = re.match(r'^pv_string_(\d+)_current_a$', record.get_field())
                if match and device in device_pv_data:
                    device_pv_data[device][match.group(1)] = record.get_value()

        return device_pv_data

    except Exception as e:
        client.close()
        raise Exception(f'PV strings query failed: {str(e)}')

    # ── Meter Overview Query ───────────────────────────────────────────────────────

def get_meter_overview(bucket, sites_with_meters):
    """
    Fetches live data for all meters across one or more sites
    (main site + optional substation).

    Args:
        bucket             : InfluxDB bucket (from Customer.influx_bucket)
        sites_with_meters  : list of dicts, one per site to query:
            [
                {
                    'influx_site_id': 'siteA',
                    'location': 'MAIN',
                    'meters': [
                        {'influx_device_id': 'meter1', 'name': 'Main Meter', 'pk': 1},
                        ...
                    ]
                },
                {
                    'influx_site_id': 'siteA-gss',
                    'location': 'SUBSTATION',
                    'meters': [...]
                }
            ]

    Returns a flat list of meter dicts, each tagged with its location and
    Postgres pk so the frontend never has to worry about influx_device_id
    collisions between main site and substation.
    """
    client    = get_influx_client()
    query_api = client.query_api()

    all_meters_result = []

    try:
        for site_group in sites_with_meters:
            site_id = site_group['influx_site_id']
            site_type = site_group['site_type']
            meters   = site_group['meters']

            if not meters:
                continue

            device_filter = ' or '.join(
                [f'r.device == "{m["influx_device_id"]}"' for m in meters]
            )

            flux = f'''
                from(bucket: "{bucket}")
                    |> range(start: -6h)
                    |> filter(fn: (r) => r._measurement == "solar_data")
                    |> filter(fn: (r) => r.site == "{site_id}")
                    |> filter(fn: (r) => {device_filter})
                    |> filter(fn: (r) =>
                        r._field == "active_power_total_kw"          or
                        r._field == "reactive_power_total_kvar"      or
                        r._field == "apparent_power_total_kva"       or
                        r._field == "energy_active_export_kwh"       or
                        r._field == "energy_active_import_kwh"       or
                        r._field == "energy_active_net_kwh"          or
                        r._field == "energy_reactive_export_kvarh"   or
                        r._field == "energy_reactive_import_kvarh"   or
                        r._field == "voltage_line_ab_v"              or
                        r._field == "voltage_line_bc_v"              or
                        r._field == "voltage_line_ca_v"              or
                        r._field == "current_phase_a"                or
                        r._field == "current_phase_b"                or
                        r._field == "current_phase_c"                or
                        r._field == "grid_frequency_hz"              or
                        r._field == "power_factor_total"
                    )
                    |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
                    |> last()
            '''

            tables       = query_api.query(flux, org=INFLUX_ORG)
            device_data  = {}
            device_last  = {}
            device_times = {}

            for table in tables:
                for record in table.records:
                    device = record.values.get('device')
                    field  = record.get_field()
                    value  = record.get_value()
                    time   = record.get_time()

                    # energy_active_export_kwh (Total): keep last-known ungated so
                    # it survives an outage instead of collapsing to 0. last() gives
                    # one point per (device, field) over the 24h window.
                    if field == 'energy_active_export_kwh':
                        device_last.setdefault(device, {})[field] = value

                    if not _is_fresh(time):
                        continue

                    if device not in device_data:
                        device_data[device] = {}
                    device_data[device][field] = value

                    if device not in device_times or time > device_times[device]:
                        device_times[device] = time

            # Build result for each meter in this site group, in order
            for meter in meters:
                influx_id = meter['influx_device_id']
                fields      = device_data.get(influx_id, {})
                last_fields = device_last.get(influx_id, {})
                t         = device_times.get(influx_id)
                is_online = bool(fields)
                offset    = meter.get('energy_offset_kwh', 0.0)

                # Total export comes from last-known so it persists through an
                # outage. Offset only ever rides on an actual export reading.
                export_last = last_fields.get('energy_active_export_kwh')
                corrected_export_kwh = round(export_last + offset, 2) if export_last is not None else 0.0

                energy_today  = _query_meter_today_energy(query_api, bucket, site_id, influx_id)
                all_meters_result.append({
                    'device_pk':                   meter['pk'],
                    'device_id':                    influx_id,
                    'name':                          meter['name'],
                    'site_type':                    site_type, 
                    'active_power_total_kw': round(fields.get('active_power_total_kw', 0.0) * -1, 2),
                    'reactive_power_total_kvar':      round(fields.get('reactive_power_total_kvar', 0.0), 2),
                    'apparent_power_total_kva':       round(fields.get('apparent_power_total_kva', 0.0), 2),
                    'energy_active_export_kwh':       corrected_export_kwh,
                    'energy_active_import_kwh':        round(fields.get('energy_active_import_kwh', 0.0), 2),
                    'energy_active_net_kwh':           round(fields.get('energy_active_net_kwh', 0.0), 2),
                    'energy_reactive_export_kvarh':    round(fields.get('energy_reactive_export_kvarh', 0.0), 2),
                    'energy_reactive_import_kvarh':    round(fields.get('energy_reactive_import_kvarh', 0.0), 2),
                    'voltage_line_ab_v':               round(fields.get('voltage_line_ab_v', 0.0), 2),
                    'voltage_line_bc_v':               round(fields.get('voltage_line_bc_v', 0.0), 2),
                    'voltage_line_ca_v':               round(fields.get('voltage_line_ca_v', 0.0), 2),
                    'current_phase_a':                 round(fields.get('current_phase_a', 0.0), 2),
                    'current_phase_b':                 round(fields.get('current_phase_b', 0.0), 2),
                    'current_phase_c':                 round(fields.get('current_phase_c', 0.0), 2),
                    'grid_frequency_hz':               round(fields.get('grid_frequency_hz', 0.0), 2),
                    'power_factor_total':              round(fields.get('power_factor_total', 0.0), 2),
                    'energy_today_kwh':                energy_today,
                    'status':                          'online' if is_online else 'offline',
                    'last_updated':                    t.isoformat() if t else None,
                })

        client.close()
        return all_meters_result

    except Exception as e:
        client.close()
        raise Exception(f'Meter overview query failed: {str(e)}')
    

# Analytics Data
def get_analytics_data(bucket, site_id, series_map, date_str=None, interval_minutes=5):
    """
    series_map: { series_key: {'device': 'inverter1', 'field': 'ac_active_power_kw'} }
    series_key is caller-defined and unique per requested device+metric combo
    (view uses '{influx_device_id}__{metric_key}').

    Returns merged, time-aligned points:
    [
        { 'time': '2026-06-18T04:30:00Z', 'inverter1__active_power': 12.4, 'weather_station1__irradiation': 340.0 },
        ...
    ]
    Points only carry keys for series that actually had data at that timestamp
    (no forced zero-fill across series with different reporting devices).
    """
    ist = timezone(timedelta(hours=5, minutes=30))

    if date_str:
        try:
            requested_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=ist)
        except ValueError:
            raise Exception(f'Invalid date format: {date_str}. Use YYYY-MM-DD.')
    else:
        requested_date = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)

    start_ist = requested_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_ist.astimezone(timezone.utc)

    now_ist   = datetime.now(ist)
    today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

    if start_ist.date() == today_ist.date():
        end_utc = datetime.now(timezone.utc)
    else:
        end_utc = (start_ist + timedelta(days=1)).astimezone(timezone.utc)

    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str   = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    match_clauses = ' or '.join(
        f'(r.device == "{s["device"]}" and r._field == "{s["field"]}")'
        for s in series_map.values()
    )

    # Reverse lookup: (device, field) -> series_key, for reassembling results
    key_by_device_field = {
        (s['device'], s['field']): key for key, s in series_map.items()
    }

    client    = get_influx_client()
    query_api = client.query_api()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {match_clauses})
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> aggregateWindow(every: {interval_minutes}m, fn: mean, createEmpty: false)
    '''


    try:
        tables  = query_api.query(flux, org=INFLUX_ORG)
        client.close()

        # Group by timestamp, merging every series' value into that timestamp's point
        points_by_time = {}

        for table in tables:
            for record in table.records:
                device = record.values.get('device')
                field  = record.get_field()
                key    = key_by_device_field.get((device, field))
                if not key:
                    continue

                value = record.get_value()
                if series_map[key].get('negate'):
                    value = -value   # meter active power: export → positive, import → negative

                time_iso = record.get_time().isoformat()
                if time_iso not in points_by_time:
                    points_by_time[time_iso] = {'time': time_iso}
                points_by_time[time_iso][key] = round(value, 3)

        results = sorted(points_by_time.values(), key=lambda p: p['time'])
        return results

    except Exception as e:
        client.close()
        raise Exception(f'Analytics query failed: {str(e)}')
    

# ── Portfolio Overview Queries ─────────────────────────────────────────────────

def _query_portfolio_live_snapshot(query_api, bucket, site_ids, site_meter_map, site_inverters_map, household_sites=()):
    """
    Live active power per meter + inverter online count per site, one query.
    site_ids:           [influx_site_id] — ALL sites in this bucket, including meterless ones
    site_meter_map:     { influx_site_id: meter_influx_device_id }   (may omit sites)
    site_inverters_map: { influx_site_id: [inverter_influx_device_ids] }
    Returns: { influx_site_id: { active_power_kw, meter_online, inverters_online,
                                 inverters_total, last_updated } }
    """
    empty = {
        'active_power_kw': 0.0, 'meter_online': False,
        'inverters_online': 0, 'inverters_total': 0,
        'states': {'running': 0, 'stopped': 0, 'standby': 0,
                   'warning': 0, 'fault': 0, 'other': 0},
        'last_updated': None,
    }
    if not site_ids:
        return {}

    all_devices = set()
    meter_site_tags = set()
    for meter_site, meter_dev in site_meter_map.values():
        all_devices.add(meter_dev)
        meter_site_tags.add(meter_site)
    for inv_ids in site_inverters_map.values():
        all_devices.update(inv_ids)

    # A bucket whose sites have no active devices at all would produce an empty
    # filter expression — invalid Flux. Return the zeroed shape instead.
    if not all_devices:
        return {sid: {**empty, 'inverters_total': len(site_inverters_map.get(sid, []))}
                for sid in site_ids}

    filter_sites = set(site_ids) | meter_site_tags
    site_filter   = ' or '.join([f'r.site == "{s}"' for s in filter_sites])
    device_filter = ' or '.join([f'r.device == "{d}"' for d in all_devices])

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => {site_filter})
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) =>
                r._field == "active_power_total_kw" or
                r._field == "ac_active_power_kw"  or
                r._field == "energy_total_kwh"    or
                r._field == "inverter_status"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    raw = {}         # { (site, device): { field: value, _time } } — fresh (120s), drives online
    status_raw = {}  # { (site, device): status_value } — wider window, online-gated below
    for table in tables:
        for record in table.records:
            key   = (record.values.get('site'), record.values.get('device'))
            field = record.get_field()
            time  = record.get_time()

            if field == 'inverter_status':
                if _is_status_recent(time):
                    status_raw[key] = record.get_value()
                continue

            if not _is_fresh(time):
                continue
            if key not in raw:
                raw[key] = {}
            raw[key][field] = record.get_value()
            raw[key]['_time'] = time

    results = {}
    for influx_site_id in site_ids:
        meter_entry = site_meter_map.get(influx_site_id)      # (meter_site, device) or None
        inv_ids     = site_inverters_map.get(influx_site_id, [])
        if meter_entry:
            meter_site, meter_id = meter_entry
            meter_rec = raw.get((meter_site, meter_id), {})   # meter's OWN site tag
        else:
            meter_rec = {}
        last_time = meter_rec.get('_time')

        raw_power = meter_rec.get('active_power_total_kw', 0.0)
        active_power_kw = 0.0 if raw_power > 0 else round(raw_power * -1, 2)

        # One pass over this site's inverters: a fresh record = online (same count
        # as before); tally its device-reported state alongside. states sums to
        # inverters_online — offline inverters report no state, same as elsewhere.
        online = 0
        states = {'running': 0, 'stopped': 0, 'standby': 0,
                  'warning': 0, 'fault': 0, 'other': 0}
        # Household sites have no meter: power is the sum of the fresh inverters'
        # AC power, and last_updated is the newest inverter record.
        household = influx_site_id in household_sites
        inv_power = None   # stays None until an inverter actually reports power
        for inv_id in inv_ids:
            inv_rec = raw.get((influx_site_id, inv_id))
            if not inv_rec:
                continue
            online += 1
            if household:
                if inv_rec.get('ac_active_power_kw') is not None:
                    inv_power = (inv_power or 0.0) + inv_rec['ac_active_power_kw']
                if last_time is None or inv_rec['_time'] > last_time:
                    last_time = inv_rec['_time']
            # online-gated last-known status from the wider window
            raw_status = status_raw.get((influx_site_id, inv_id))
            code = int(round(raw_status)) if raw_status is not None else None
            states[_INVERTER_STATE_KEYS.get(code, 'other')] += 1

        results[influx_site_id] = {
            'active_power_kw': (
                (round(inv_power, 2) if inv_power is not None else None)
                if household else active_power_kw
            ),
            'meter_online':     None if household else bool(meter_rec),   # None = no meter by design
            'inverters_online': online,
            'inverters_total':  len(inv_ids),
            'states':           states,
            'last_updated':     last_time.isoformat() if last_time else None,
        }

    return results


def _query_portfolio_energy_today(query_api, bucket, site_meter_map):
    """
    Energy today (last - first since IST midnight) for all meters in this bucket.
    site_meter_map: { influx_site_id: meter_influx_device_id }
    Returns: { influx_site_id: energy_today_kwh }
    """
    if not site_meter_map:
        return {}

    site_ids   = list(site_meter_map.keys())
    meter_ids  = list({dev for (_site, dev) in site_meter_map.values()})
    meter_tags = {site for (site, _dev) in site_meter_map.values()}

    # Include meter site tags (substation meters live under their own tag) so
    # their series are in range. Same-bucket is guaranteed by the caller.
    filter_sites  = set(site_ids) | meter_tags
    site_filter   = ' or '.join([f'r.site == "{s}"' for s in filter_sites])
    device_filter = ' or '.join([f'r.device == "{d}"' for d in meter_ids])
    start         = get_ist_midnight_utc()
    ist_offset    = '5h30m'

    # aggregateWindow(every: 24h, timeSrc: "_start") ensures both first and last
    # share the same _time (IST midnight), so pivot can match them correctly.
    flux = f'''
    import "timezone"

    option location = timezone.fixed(offset: {ist_offset})

    day_first = from(bucket: "{bucket}")
        |> range(start: {start})
        |> filter(fn: (r) => r._measurement == "solar_data")
        |> filter(fn: (r) => {site_filter})
        |> filter(fn: (r) => {device_filter})
        |> filter(fn: (r) => r._field == "energy_active_export_kwh")
        |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
        |> aggregateWindow(every: 24h, fn: first, createEmpty: false, timeSrc: "_start")
        |> map(fn: (r) => ({{r with _field: "v_first"}}))

    day_last = from(bucket: "{bucket}")
        |> range(start: {start})
        |> filter(fn: (r) => r._measurement == "solar_data")
        |> filter(fn: (r) => {site_filter})
        |> filter(fn: (r) => {device_filter})
        |> filter(fn: (r) => r._field == "energy_active_export_kwh")
        |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
        |> aggregateWindow(every: 24h, fn: last, createEmpty: false, timeSrc: "_start")
        |> map(fn: (r) => ({{r with _field: "v_last"}}))

    union(tables: [day_first, day_last])
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> map(fn: (r) => ({{r with _value: r.v_last - r.v_first}}))
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    # site + device tags are preserved through union → pivot → map
    energy_map = {}  # { (site_tag, device_tag): energy_kwh }
    for table in tables:
        for record in table.records:
            key = (record.values.get('site'), record.values.get('device'))
            energy_map[key] = abs(round(record.get_value(), 2))

    return {
        influx_site_id: energy_map.get(site_meter_map[influx_site_id], 0.0)
        for influx_site_id in site_ids
    }


def _query_portfolio_inverter_energy_today(query_api, bucket, site_inverters_map, site_register_map):
    """
    Energy today for HOUSEHOLD sites in this bucket = sum of the site's inverters.
    Bulk mirror of _query_inverters_today_energy - keep in lockstep:
      COUNTER   last - first of energy_total_kwh since IST midnight.
      REGISTER  last() of energy_today_kw since IST midnight + grace.
    site_inverters_map: { influx_site_id: [inverter_influx_device_ids] }  household sites only
    site_register_map:  { influx_site_id: [device ids set to REGISTER] }
    Returns: { influx_site_id: energy_today_kwh }. A site whose inverters have
    reported nothing today is omitted (caller defaults it, same as the meter path).
    """
    counter_keys  = set()   # (site, device)
    register_keys = set()
    for site_id, inv_ids in site_inverters_map.items():
        reg = set(site_register_map.get(site_id, []))
        for d in inv_ids:
            (register_keys if d in reg else counter_keys).add((site_id, d))

    start      = get_ist_midnight_utc()
    ist_offset = '5h30m'
    per_device = {}         # { (site, device): kWh }

    if counter_keys:
        site_filter   = ' or '.join([f'r.site == "{s}"' for s in {k[0] for k in counter_keys}])
        device_filter = ' or '.join([f'r.device == "{d}"' for d in {k[1] for k in counter_keys}])

        # _value > 0 drops a spurious zero counter read, which would otherwise
        # become "first" and inflate the day by the whole lifetime total.
        flux = f'''
        import "timezone"

        option location = timezone.fixed(offset: {ist_offset})

        day_first = from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => {site_filter})
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => r._field == "energy_total_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> filter(fn: (r) => r._value > 0.0)
            |> aggregateWindow(every: 24h, fn: first, createEmpty: false, timeSrc: "_start")
            |> map(fn: (r) => ({{r with _field: "v_first"}}))

        day_last = from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => {site_filter})
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => r._field == "energy_total_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> filter(fn: (r) => r._value > 0.0)
            |> aggregateWindow(every: 24h, fn: last, createEmpty: false, timeSrc: "_start")
            |> map(fn: (r) => ({{r with _field: "v_last"}}))

        union(tables: [day_first, day_last])
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> map(fn: (r) => ({{r with _value: r.v_last - r.v_first}}))
        '''

        for table in query_api.query(flux, org=INFLUX_ORG):
            for record in table.records:
                key = (record.values.get('site'), record.values.get('device'))
                # The filters are site-OR x device-OR, so a REGISTER inverter that
                # shares a device id with another site's COUNTER one can come back.
                if key in counter_keys and record.get_value() is not None:
                    per_device[key] = record.get_value()

    if register_keys:
        reg_start_dt = (
            datetime.strptime(start, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            + timedelta(seconds=INVERTER_TODAY_REGISTER_GRACE_SECONDS)
        )
        # Inside the grace window the range would start in the future (Flux errors).
        if datetime.now(timezone.utc) > reg_start_dt:
            reg_start     = reg_start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            site_filter   = ' or '.join([f'r.site == "{s}"' for s in {k[0] for k in register_keys}])
            device_filter = ' or '.join([f'r.device == "{d}"' for d in {k[1] for k in register_keys}])

            flux_reg = f'''
                from(bucket: "{bucket}")
                    |> range(start: {reg_start})
                    |> filter(fn: (r) => r._measurement == "solar_data")
                    |> filter(fn: (r) => {site_filter})
                    |> filter(fn: (r) => {device_filter})
                    |> filter(fn: (r) => r._field == "energy_today_kw")
                    |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
                    |> last()
            '''

            for table in query_api.query(flux_reg, org=INFLUX_ORG):
                for record in table.records:
                    key = (record.values.get('site'), record.values.get('device'))
                    if key in register_keys and record.get_value() is not None:
                        per_device[key] = record.get_value()

    totals = {}
    for (site_id, _device), kwh in per_device.items():
        totals[site_id] = totals.get(site_id, 0.0) + kwh
    return {site_id: round(v, 2) for site_id, v in totals.items()}


def _query_portfolio_logger(query_api, bucket, site_ids, logger_tags=None):
    """
    Data-logger heartbeat for every site in a bucket, one query.
      logger_online    = last heartbeat within STALE_AFTER_SECONDS (120s).
      logger_last_seen = most recent heartbeat within -3h, regardless of freshness.
    Returns: { influx_site_id: { 'logger_online': bool, 'logger_last_seen': iso|None } }

    Resilient: a bucket that doesn't exist yet (placeholder customer) or any Influx
    error degrades to offline/None for that bucket's sites rather than failing the
    whole portfolio. The meter/inverter queries skip device-less buckets before
    touching Influx via their own guards, so this is the first query to actually
    hit such a bucket — it must tolerate a missing one.
    """
    if not site_ids:
        return {}

    offline = {sid: {'logger_online': False, 'logger_last_seen': None} for sid in site_ids}

    # logger_tags: { influx_site_id: site tag its heartbeat is published under }.
    # Household sites share one gateway, whose heartbeat carries the client id as
    # its site tag. Sites not in the map use their own site id, as before.
    logger_tags = logger_tags or {}
    site_filter = ' or '.join(
        [f'r.site == "{s}"' for s in {logger_tags.get(sid, sid) for sid in site_ids}]
    )

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -3h)
            |> filter(fn: (r) => r._measurement == "device_info")
            |> filter(fn: (r) => r.device == "data_logger")
            |> filter(fn: (r) => r._field == "heartbeat")
            |> filter(fn: (r) => {site_filter})
            |> last()
    '''

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)
    except Exception:
        return offline

    last_by_site = {}
    for table in tables:
        for record in table.records:
            sid = record.values.get('site')
            t   = record.get_time()
            if sid not in last_by_site or (t and t > last_by_site[sid]):
                last_by_site[sid] = t

    results = {}
    for sid in site_ids:
        t = last_by_site.get(logger_tags.get(sid, sid))
        results[sid] = {
            'logger_online':    _is_fresh(t),
            'logger_last_seen': t.isoformat() if t else None,
        }
    return results


def _query_portfolio_weather(query_api, bucket, site_weather_map):
    """
    POA today + station liveness for every weather station in a bucket, one call.
    Mirrors Plant Overview exactly:
      poa_kwh_m2     = integral of irradiation_inclined_wm2 since IST midnight
                       (same as _query_poa_irradiation), Wh/m² → kWh/m², round 4.
      weather_online = any weather field fresh within STALE_AFTER_SECONDS
                       (same rule as _query_weather_live).
    site_weather_map: { influx_site_id: weather_influx_device_id }  (plant's own site tag)
    Returns: { influx_site_id: {'poa_kwh_m2': float, 'weather_online': bool} }

    Resilient: any Influx error returns {} → caller treats every station as
    offline, PR goes null, the portfolio stays up.
    """
    if not site_weather_map:
        return {}

    site_filter   = ' or '.join([f'r.site == "{s}"' for s in site_weather_map.keys()])
    device_filter = ' or '.join([f'r.device == "{d}"' for d in set(site_weather_map.values())])
    start         = get_ist_midnight_utc()

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => {site_filter})
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => r._field == "irradiation_inclined_wm2")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> integral(unit: 1h)
            |> yield(name: "poa")

        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => {site_filter})
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) =>
                r._field == "irradiation_inclined_wm2" or
                r._field == "ambient_temp_c"           or
                r._field == "module_temp_c"            or
                r._field == "wind_speed_ms"            or
                r._field == "wind_direction_deg"       or
                r._field == "pressure_hpa"             or
                r._field == "rain_mm"                  or
                r._field == "humidity_pct"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
            |> yield(name: "live")
    '''

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)
    except Exception:
        return {}

    poa_wh = {}     # { (site_tag, device_tag): Wh/m² }
    live   = set()  # { (site_tag, device_tag) } with any fresh field
    for table in tables:
        for record in table.records:
            key = (record.values.get('site'), record.values.get('device'))
            if record.values.get('result') == 'poa':
                poa_wh.setdefault(key, record.get_value() or 0.0)
            elif _is_fresh(record.get_time()):
                live.add(key)

    out = {}
    for influx_site_id, device_id in site_weather_map.items():
        key = (influx_site_id, device_id)
        out[influx_site_id] = {
            'poa_kwh_m2':     round(poa_wh.get(key, 0.0) / 1000.0, 4),
            'weather_online': key in live,
        }
    return out


def get_portfolio_overview(bucket_groups, include_energy=True):
    """
    Fires 2 Flux queries per bucket (live snapshot + energy today), or 1 per
    bucket when include_energy=False.

    include_energy=False drops `energy_today_kwh` (returned as None, never 0.0 —
    zero would render as "generated nothing today"). This is the expensive half:
    the energy query reads from IST midnight TWICE (day_first + day_last) while
    the live query reads 10 minutes once, so by evening it accounts for nearly
    all of the endpoint's scan cost. Use it for wide-tenancy callers (ADMIN)
    that only need liveness.

    bucket_groups: {
        bucket_str: {
            'pk_map':        { influx_site_id: site_pk },   # ALL sites in bucket
            'meter_map':     { influx_site_id: meter_influx_device_id },
            'inverters_map': { influx_site_id: [inv_influx_device_ids] },
            'weather_map':   { influx_site_id: weather_influx_device_id },  # optional
        }
    }
    Returns keyed on Site.pk — NOT influx_site_id. influx_site_id is only unique
    within a customer's bucket, so keying results on it collides across customers
    (two customers can both use "test"). pk is globally unique; the translation
    happens here, at the bucket boundary, while bucket context still exists.

    Returns: { site_pk: { active_power_kw, energy_today_kwh, meter_online,
                          inverters_online, inverters_total, last_updated } }
    """
    client    = get_influx_client()
    query_api = client.query_api()
    results   = {}

    try:
        for bucket, group in bucket_groups.items():
            site_ids = list(group['pk_map'].keys())

            household = group.get('household', set())

            live = _query_portfolio_live_snapshot(
                query_api, bucket, site_ids, group['meter_map'], group['inverters_map'],
                household
            )
            energy = (
                _query_portfolio_energy_today(query_api, bucket, group['meter_map'])
                if include_energy else {}
            )
            # Household sites: energy today from their inverters, not a meter.
            if include_energy and household:
                energy.update(_query_portfolio_inverter_energy_today(
                    query_api, bucket,
                    {sid: group['inverters_map'].get(sid, []) for sid in household},
                    group.get('register_map', {}),
                ))

            weather = (
                _query_portfolio_weather(query_api, bucket, group.get('weather_map', {}))
                if include_energy else {}
            )

            logger = _query_portfolio_logger(
                query_api, bucket, site_ids, group.get('logger_tags')
            )

            for influx_site_id, site_pk in group['pk_map'].items():
                results[site_pk] = {
                    **live.get(influx_site_id, {
                        'active_power_kw': 0.0, 'meter_online': False,
                        'inverters_online': 0, 'inverters_total': 0,
                        'states': {'running': 0, 'stopped': 0, 'standby': 0,
                                   'warning': 0, 'fault': 0, 'other': 0},
                        'last_updated': None,
                    }),
                    'energy_today_kwh': (
                        energy.get(influx_site_id, 0.0) if include_energy else None
                    ),
                    **weather.get(influx_site_id, {'poa_kwh_m2': 0.0, 'weather_online': False}),
                    **logger.get(influx_site_id, {'logger_online': False, 'logger_last_seen': None}),
                }

    finally:
        client.close()

    return results

# Weather Station 
def get_weather_snapshot(bucket, site_id, device_id):
    """
    Live snapshot for the weather station page.
    Delegates to the shared _query_weather_live (staleness-gated),
    so this page and get_plant_overview never disagree on what "online" means.
    """
    client    = get_influx_client()
    query_api = client.query_api()

    try:
        fields, last_time = _query_weather_live(query_api, bucket, site_id, device_id)
        client.close()

        is_online = bool(fields)

        return {
            'irradiation_inclined_wm2': round(fields.get('irradiation_inclined_wm2', 0.0), 2),
            'ambient_temp_c':           round(fields.get('ambient_temp_c', 0.0), 2),
            'module_temp_c':            round(fields.get('module_temp_c', 0.0), 2),
            'wind_speed_ms':            round(fields.get('wind_speed_ms', 0.0), 2),
            'wind_direction_deg':       round(fields.get('wind_direction_deg', 0.0), 1),
            'pressure_hpa':             round(fields.get('pressure_hpa', 0.0), 2),
            'rain_mm':                  round(fields.get('rain_mm', 0.0), 2),
            'humidity_pct':             round(fields.get('humidity_pct', 0.0), 1),
            'status':                   'online' if is_online else 'offline',
            'last_updated':             last_time.isoformat() if last_time else None,
        }

    except Exception as e:
        client.close()
        raise Exception(f'Weather snapshot query failed: {str(e)}')
    

def _query_weather_live(query_api, bucket, site_id, device_id):
    """
    Internal: fetches live weather station fields, gated on staleness.
    Returns (fields, last_time) — fields is {} and last_time is None
    if no fresh data was found.
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) =>
                r._field == "irradiation_inclined_wm2" or
                r._field == "ambient_temp_c"           or
                r._field == "module_temp_c"            or
                r._field == "wind_speed_ms"            or
                r._field == "wind_direction_deg"       or
                r._field == "pressure_hpa"             or
                r._field == "rain_mm"                  or
                r._field == "humidity_pct"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    tables       = query_api.query(flux, org=INFLUX_ORG)
    weather_data = {}
    last_time    = None

    for table in tables:
        for record in table.records:
            if not _is_fresh(record.get_time()):
                continue

            weather_data[record.get_field()] = record.get_value()
            t = record.get_time()
            if last_time is None or t > last_time:
                last_time = t

    return weather_data, last_time


def _query_transformer_live(query_api, bucket, site_id, device_id):
    """
    Internal: fetches live transformer fields, gated on staleness.
    Returns (fields, last_time) — fields is {} and last_time is None
    if no fresh data was found. Only fields present in the fresh reading
    appear in the dict; a missing field is absent → None upstream, never
    0.0 (0°C is a plausible real temperature, so zero-masking would lie).
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) =>
                r._field == "transformer_oil_temp_c" or
                r._field == "transformer_winding_temp_c"
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    tables           = query_api.query(flux, org=INFLUX_ORG)
    transformer_data = {}
    last_time        = None

    for table in tables:
        for record in table.records:
            if not _is_fresh(record.get_time()):
                continue

            transformer_data[record.get_field()] = record.get_value()
            t = record.get_time()
            if last_time is None or t > last_time:
                last_time = t

    return transformer_data, last_time

def _query_poa_irradiation(query_api, bucket, site_id, device_id, start):
    """
    Internal: true cumulative plane-of-array irradiation since IST midnight,
    using Flux's integral() to do the area-under-curve calc server-side.
    irradiation_inclined_wm2 is an instantaneous rate (W/m²); integrating
    it over time with unit: 1h gives Wh/m² directly.
    Returns Wh/m² (0.0 if no weather data in range).
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) => r._field == "irradiation_inclined_wm2")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> integral(unit: 1h)
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    for table in tables:
        for record in table.records:
            return record.get_value() or 0.0

    return 0.0

# Daily Snapshot to Posgres

def _query_meter_energy_for_day(query_api, bucket, site_id, meter_id, start, end):
    """
    Internal: meter energy generated over a specific IST day (start/end
    already resolved via _resolve_ist_date_range). Last-minus-first on the
    cumulative export counter, range-bounded — safe for past dates.

    Returns (energy_kwh, status, start_counter_kwh, end_counter_kwh):
    energy_kwh — the day's generation (last/first of the export counter):
        (float, 'ok')       — trustworthy figure, may legitimately be 0.0
        (None,  'no_data')  — meter reported nothing in this range
        (None,  'anomaly')  — counter went backwards (rollover / meter swap /
                                bad packet). Never store the delta as a real number.
    start_counter_kwh — the day's FIRST raw export reading (odometer at day
        start); None if the meter reported nothing.
    end_counter_kwh   — the day's LAST raw export reading (odometer at day end);
        None if the meter reported nothing.
    Both are raw, NOT offset-corrected, and are surfaced even on 'anomaly' —
    they're real meter readings; only the delta between them is untrustworthy.
    On an 'ok' day, end / start == energy_kwh by construction — which is the
    whole point: an independent second source to catch a bad daily figure.
    """
    flux_first = f'''
        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "energy_active_export_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> first()
    '''
    flux_last = f'''
        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "energy_active_export_kwh")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    first_val = None
    last_val  = None

    tables = query_api.query(flux_first, org=INFLUX_ORG)
    for table in tables:
        for record in table.records:
            first_val = record.get_value()

    tables = query_api.query(flux_last, org=INFLUX_ORG)
    for table in tables:
        for record in table.records:
            last_val = record.get_value()

    start_counter = round(first_val, 2) if first_val is not None else None
    end_counter   = round(last_val, 2)  if last_val  is not None else None

    if first_val is None or last_val is None:
        return None, 'no_data', start_counter, end_counter

    delta = last_val - first_val
    if delta < 0:
        return None, 'anomaly', start_counter, end_counter

    return round(delta, 3), 'ok', start_counter, end_counter


def _query_poa_irradiation_for_day(query_api, bucket, site_id, device_id, start, end):
    """
    Internal: POA irradiation integrated over a specific IST day.
    Same integral() logic as _query_poa_irradiation, range-bounded.
    Returns Wh/m² (0.0 if no weather data in range).
    """
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{device_id}")
            |> filter(fn: (r) => r._field == "irradiation_inclined_wm2")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> integral(unit: 1h)
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    for table in tables:
        for record in table.records:
            return record.get_value() or 0.0

    return 0.0


def _query_meter_peak_power_for_day(query_api, bucket, site_id, meter_id, start, end):
    """
    Internal: max active_power_total_kw for a meter over a specific IST day,
    and the timestamp it occurred at.
    Returns (peak_kw, peak_time) — (None, None) if no data in range.
    """
    flux = f'''import "math"

        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "active_power_total_kw")
            |> map(fn: (r) => ({{r with _value: math.abs(x: float(v: r._value))}}))
            |> top(n: 1)
        '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    for table in tables:
        for record in table.records:
            return round(abs(record.get_value()), 2), record.get_time()

    return None, None


def _query_inverter_daily_sum_for_day(query_api, bucket, site_id, inverter_ids, start, end, register_ids=None):
    """
    Internal: summed inverter generation for the given IST day. Per-device
    source (Device.energy_today_source), same split as the live
    _query_inverters_today_energy:
      COUNTER  → last − first of energy_total_kwh over the day.
      REGISTER → last() of energy_today_kw over
                 [start + INVERTER_TODAY_REGISTER_GRACE_SECONDS, end).
                 The window ends at next IST midnight, before the ~00:01
                 reset, so last() is the day's final total.

    "Reporting" = the inverter communicated its energy field that day
    (COUNTER: both a first and last point; REGISTER: any point). A real
    0 kWh washout still counts as online; only a silent inverter is 0/N.
    Cross-check figure vs the meter, not the authoritative daily energy.

    Returns (total_kwh, count_reporting).
    """
    register_ids = set(register_ids or [])
    counter_ids  = [d for d in inverter_ids if d not in register_ids]
    reg_ids      = [d for d in inverter_ids if d in register_ids]

    total = 0.0
    count = 0

    # ── COUNTER inverters ──
    if counter_ids:
        device_filter = ' or '.join(f'r.device == "{d}"' for d in counter_ids)

        flux = f'''
            base = from(bucket: "{bucket}")
                |> range(start: {start}, stop: {end})
                |> filter(fn: (r) => r._measurement == "solar_data")
                |> filter(fn: (r) => r.site == "{site_id}")
                |> filter(fn: (r) => {device_filter})
                |> filter(fn: (r) => r._field == "energy_total_kwh")
                |> filter(fn: (r) => exists r._value)
                |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
                |> filter(fn: (r) => r._value > 0.0)   // v13 Fix A: a live lifetime
                //  counter is never 0; a night 0 latched by first() = whole odometer

            base |> first() |> yield(name: "first")
            base |> last()  |> yield(name: "last")
        '''

        firsts = {}
        lasts  = {}
        for table in query_api.query(flux, org=INFLUX_ORG):
            for record in table.records:
                result = record.values.get('result')   # 'first' / 'last' (yield name)
                device = record.values.get('device')
                value  = record.get_value()
                if value is None or device is None:
                    continue
                if result == 'first':
                    firsts[device] = value
                elif result == 'last':
                    lasts[device] = value

        for device in counter_ids:
            if device in firsts and device in lasts:
                count += 1                                        # communicated → online
                total += max(0.0, lasts[device] - firsts[device])  # a day can't generate negative

    # ── REGISTER inverters ──
    if reg_ids:
        fmt          = '%Y-%m-%dT%H:%M:%SZ'
        reg_start_dt = (
            datetime.strptime(start, fmt).replace(tzinfo=timezone.utc)
            + timedelta(seconds=INVERTER_TODAY_REGISTER_GRACE_SECONDS)
        )
        reg_stop_dt = (
            datetime.strptime(end, fmt).replace(tzinfo=timezone.utc)
            - timedelta(seconds=INVERTER_TODAY_REGISTER_END_GUARD_SECONDS)
        )

        # Skip rather than send Flux an empty/inverted range.
        if reg_start_dt < reg_stop_dt:
            device_filter = ' or '.join(f'r.device == "{d}"' for d in reg_ids)

            flux_reg = f'''
                from(bucket: "{bucket}")
                    |> range(start: {reg_start_dt.strftime(fmt)}, stop: {reg_stop_dt.strftime(fmt)})
                    |> filter(fn: (r) => r._measurement == "solar_data")
                    |> filter(fn: (r) => r.site == "{site_id}")
                    |> filter(fn: (r) => {device_filter})
                    |> filter(fn: (r) => r._field == "energy_today_kw")
                    |> filter(fn: (r) => exists r._value)
                    |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
                    |> group(columns: ["device"])
                    |> last()
            '''

            reg_vals = {}
            for table in query_api.query(flux_reg, org=INFLUX_ORG):
                for record in table.records:
                    device = record.values.get('device')
                    value  = record.get_value()
                    if device is not None and value is not None:
                        reg_vals[device] = value

            for device in reg_ids:
                if device in reg_vals:
                    count += 1
                    total += max(0.0, reg_vals[device])

    return round(total, 3), count

# Generation Start End (Optimize it later)

def _query_plant_generation_window(query_api, bucket, site_id, meter_id):
    """
    Internal: start/end of today's generation window, derived purely from
    meter active_power_total_kw — independent of the weather station.

    Meter convention: active_power_total_kw is negative while exporting
    (generating), >= 0 while idle/importing. We use a small negative
    threshold (not strict < 0) so noise near zero doesn't register as
    generation.

    start_time: first timestamp today the meter crossed into export.
    end_time:   depends on state —
        - still generating, or dipped only briefly (< GENERATION_STOP_GAP_SECONDS
          since last export) → None, status 'operational'
        - hasn't exported in over GENERATION_STOP_GAP_SECONDS → frozen at the
          last export timestamp, status 'stopped'
        - meter has no fresh data at all (dead/offline) → status 'no_data',
          start/end reflect whatever was seen before it went dark
    """
    start = get_ist_midnight_utc()

    # Push the reduction into Influx: instead of transferring every raw point
    # since midnight (thousands by evening) and looping in Python every 60s, ask
    # for just the three timestamps we use. first()/last() run server-side; ~3
    # rows come back regardless of how long the day has been.
    flux = f'''
        base = from(bucket: "{bucket}")
            |> range(start: {start})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "active_power_total_kw")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))

        exporting = base
            |> filter(fn: (r) => r._value <= {EXPORT_THRESHOLD_KW})

        exporting |> first() |> yield(name: "first_export")
        exporting |> last()  |> yield(name: "last_export")
        base      |> last()  |> yield(name: "latest_point")
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    first_export_time = None
    last_export_time  = None
    latest_point_time = None

    for table in tables:
        for record in table.records:
            result = record.values.get('result')
            t      = record.get_time()
            if result == 'first_export':
                first_export_time = t
            elif result == 'last_export':
                last_export_time = t
            elif result == 'latest_point':
                latest_point_time = t

    now = datetime.now(timezone.utc)

    # Meter isn't reporting at all — can't distinguish night from a dead meter
    if latest_point_time is None or not _is_fresh(latest_point_time):
        return {
            'status':     'no_data',
            'start_time': first_export_time.isoformat() if first_export_time else None,
            'end_time':   last_export_time.isoformat() if last_export_time else None,
        }

    # Meter is alive but has never exported today (before sunrise, or a total washout)
    if last_export_time is None:
        return {
            'status':     'operational',
            'start_time': None,
            'end_time':   None,
        }

    gap_seconds = (now - last_export_time).total_seconds()

    if gap_seconds <= GENERATION_STOP_GAP_SECONDS:
        return {
            'status':     'operational',
            'start_time': first_export_time.isoformat(),
            'end_time':   None,
        }
    else:
        return {
            'status':     'stopped',
            'start_time': first_export_time.isoformat(),
            'end_time':   last_export_time.isoformat(),
        }


def _query_generation_window_for_day(query_api, bucket, site_id, meter_id, start, end):
    """
    Internal: first/last export timestamps over a *completed* IST day, for the
    daily snapshot. Snapshot twin of _query_plant_generation_window with the
    live-only machinery removed: a finished day has no now(), no freshness gate,
    and no operational/stopped/no_data state — that day-level health already
    lives in meter_status. So we return just the two boundary timestamps.

    first()/last() run server-side on the exporting-only stream, so only two
    rows come back regardless of how long the day was.

    Returns (start_time, end_time) as tz-aware UTC datetimes, or (None, None)
    if the meter never crossed the export threshold that day (washout / dark).
    """
    flux = f'''
        exporting = from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => r.device == "{meter_id}")
            |> filter(fn: (r) => r._field == "active_power_total_kw")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> filter(fn: (r) => r._value <= {EXPORT_THRESHOLD_KW})

        exporting |> first() |> yield(name: "first_export")
        exporting |> last()  |> yield(name: "last_export")
    '''

    tables = query_api.query(flux, org=INFLUX_ORG)

    start_time = None
    end_time   = None

    for table in tables:
        for record in table.records:
            result = record.values.get('result')
            if result == 'first_export':
                start_time = record.get_time()
            elif result == 'last_export':
                end_time = record.get_time()

    return start_time, end_time


# Faults / inverter_status timeline ------------------------------------------

# Per-minute inverter_status: interior gaps shorter than this are absorbed into
# the surrounding run (one or two dropped samples shouldn't shatter the bar);
# longer gaps — and the head/tail of the window — surface as 'No Data', so a
# stopped-reporting inverter never paints its last state forward across time we
# know nothing about.
TIMELINE_GAP_SECONDS = 300


def _ist_day_window(date_str=None):
    """
    Resolve a 'YYYY-MM-DD' (IST) date to a query window, matching the trend
    endpoints exactly so pages agree on what a given day means.
    Returns (start_utc, end_utc, is_today, ist_date_str). end is 'now' for today,
    else next IST midnight. Raises on a malformed date_str.
    """
    ist = timezone(timedelta(hours=5, minutes=30))

    if date_str:
        try:
            requested = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=ist)
        except ValueError:
            raise Exception(f'Invalid date format: {date_str}. Use YYYY-MM-DD.')
    else:
        requested = datetime.now(ist)

    start_ist = requested.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_ist.astimezone(timezone.utc)

    today_ist = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    is_today  = start_ist.date() == today_ist.date()

    end_utc = (datetime.now(timezone.utc) if is_today
               else (start_ist + timedelta(days=1)).astimezone(timezone.utc))

    return start_utc, end_utc, is_today, start_ist.strftime('%Y-%m-%d')


def _query_inverter_status_series(query_api, bucket, site_id, inverter_ids, start_str, end_str):
    """
    Raw inverter_status for the given inverters over [start_str, end_str], grouped
    by device, ascending. Returns { device_id: [(time, value_float), ...] }.
    (Graduated from the temporary portfolio probe; bounded window instead of -Nh.)
    """
    device_filter = ' or '.join([f'r.device == "{d}"' for d in inverter_ids])
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => r._field == "inverter_status")
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> sort(columns: ["_time"])
    '''
    out = {}
    for table in query_api.query(flux, org=INFLUX_ORG):
        for record in table.records:
            device = record.values.get('device')
            value  = record.get_value()
            time   = record.get_time()
            if device is None or value is None or time is None:
                continue
            out.setdefault(device, []).append((time, value))
    for dev in out:
        out[dev].sort(key=lambda p: p[0])
    return out


def _timeline_segment(code, start, end):
    """One contiguous segment. code=None → 'No Data' (no reading for this span),
    distinct from an unrecognized code → 'Unknown'."""
    return {
        'code':  code,
        'label': 'No Data' if code is None else _inverter_status_label(code),
        'start': start.isoformat(),
        'end':   end.isoformat(),
    }


def _build_inverter_timeline(samples, window_start, window_end, gap_seconds=TIMELINE_GAP_SECONDS):
    """
    samples: [(time, value_float), ...] ascending, ONE device, inside the window.
    Returns contiguous, hole-free segments spanning [window_start, window_end]:
    collapsed state runs interleaved with synthetic 'No Data' for gaps > gap_seconds
    and for a late start / early stop.

    Boundaries: a state change with no gap is dated at the FIRST sample of the new
    state (the exact transition between two 1-min samples is unknown). A gap closes
    the prior run at its LAST seen sample — we only claim the state held as long as
    we actually saw it, which is what stops a stale state extending to 'now'.
    """
    gap = timedelta(seconds=gap_seconds)

    if not samples:
        return [_timeline_segment(None, window_start, window_end)]

    segments = []
    first_t = samples[0][0]
    if first_t - window_start > gap:
        segments.append(_timeline_segment(None, window_start, first_t))
        run_start = first_t
    else:
        run_start = window_start

    run_code = int(round(samples[0][1]))
    prev_t   = first_t

    for t, v in samples[1:]:
        code = int(round(v))
        if t - prev_t > gap:
            segments.append(_timeline_segment(run_code, run_start, prev_t))
            segments.append(_timeline_segment(None, prev_t, t))
            run_code, run_start = code, t
        elif code != run_code:
            segments.append(_timeline_segment(run_code, run_start, t))
            run_code, run_start = code, t
        prev_t = t

    if window_end - prev_t > gap:
        segments.append(_timeline_segment(run_code, run_start, prev_t))
        segments.append(_timeline_segment(None, prev_t, window_end))
    else:
        segments.append(_timeline_segment(run_code, run_start, window_end))

    return segments


def get_inverter_faults(bucket, site_id, inverter_ids, date_str=None):
    """
    Per-inverter inverter_status timeline for one site, for a single IST day.

    current  — freshness-gated (_is_fresh) latest state, TODAY only; None when
               stale or for a past date. The overview page owns online/offline,
               so a stale inverter here simply has no current state.
    timeline — contiguous segments (state runs + 'No Data' gaps) across the whole
               window; a stopped-reporting inverter tails into No Data, never its
               last state frozen forward.

    Forward-compatible: a segment can later gain a 'faults' list (specific bits)
    without reshaping.
    """
    start_utc, end_utc, is_today, date_iso = _ist_day_window(date_str)
    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str   = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    client    = get_influx_client()
    query_api = client.query_api()

    try:
        samples_by_device = _query_inverter_status_series(
            query_api, bucket, site_id, inverter_ids, start_str, end_str
        )
        client.close()

        inverter_list = []
        for device_id in inverter_ids:
            samples  = samples_by_device.get(device_id, [])
            timeline = _build_inverter_timeline(samples, start_utc, end_utc)

            current = None
            if is_today and samples:
                last_t, last_v = samples[-1]
                if _is_status_recent(last_t):          # was _is_fresh(last_t)
                    code = int(round(last_v))
                    current = {'code': code, 'label': _inverter_status_label(code)}

            inverter_list.append({
                'device_id': device_id,
                'current':   current,
                'timeline':  timeline,
            })

        return {
            'date':      date_iso,
            'is_today':  is_today,
            'window':    {'start': start_utc.isoformat(), 'end': end_utc.isoformat()},
            'gap_threshold_seconds': TIMELINE_GAP_SECONDS,
            'inverters': inverter_list,
        }

    except Exception as e:
        client.close()
        raise Exception(f'Inverter faults query failed: {str(e)}')


# ── Annunciator (transformer protection alarms) ─────────────────────────────
# Digital 0/1 channels off the annunciator device. Any point at 1 = active alarm.
# Order here is the display order; alarm/trip pairs grouped, spares last. Labels
# are a starting proposal — edit freely, the field keys are what must match Influx.
ANNUNCIATOR_ALARM_FIELDS = [
    ('bucholz_alarm', 'Buchholz Alarm'),
    ('bucholz_trip',  'Buchholz Trip'),
    ('oti_alarm',     'Oil Temp (OTI) Alarm'),
    ('oti_trip',      'Oil Temp (OTI) Trip'),
    ('wti_alarm',     'Winding Temp (WTI) Alarm'),
    ('wti_trip',      'Winding Temp (WTI) Trip'),
    ('mog_trip',      'Magnetic Oil Gauge (MOG) Trip'),
    ('osr_trip',      'Oil Surge Relay (OSR) Trip'),
    ('ef_trip',       'Earth Fault Trip'),
    ('oc_trip',       'Over Current Trip'),
    ('ov_trip',       'Over Voltage Trip'),
    ('uv_trip',       'Under Voltage Trip'),
    ('spare',         'Spare'),
    ('spare1',        'Spare 1'),
    ('spare2',        'Spare 2'),
    ('spare3',        'Spare 3'),
]

# A run of 1s is one alarm event. Interior data gaps up to this are tolerated
# (a dropped 60s sample won't split a continuous alarm); a longer gap means the
# feed stopped, so we close the event at the last point we actually saw it at 1.
ANNUNCIATOR_GAP_SECONDS = 180


def _query_annunciator_series(query_api, bucket, site_id, device_ids, start_str, end_str):
    """
    Raw annunciator alarm points for the given device(s) over [start, end],
    grouped by device then field, ascending. Every configured alarm channel is
    pulled (0/1). Returns { device_id: { field: [(time, value_float), ...] } }.
    """
    device_filter = ' or '.join([f'r.device == "{d}"' for d in device_ids])
    field_filter  = ' or '.join([f'r._field == "{f}"' for f, _ in ANNUNCIATOR_ALARM_FIELDS])
    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) => {field_filter})
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> sort(columns: ["_time"])
    '''
    out = {}
    for table in query_api.query(flux, org=INFLUX_ORG):
        for record in table.records:
            device = record.values.get('device')
            field  = record.get_field()
            value  = record.get_value()
            time   = record.get_time()
            if device is None or field is None or value is None or time is None:
                continue
            out.setdefault(device, {}).setdefault(field, []).append((time, value))
    for dev in out:
        for f in out[dev]:
            out[dev][f].sort(key=lambda p: p[0])
    return out


def _build_annunciator_events(samples, is_today, gap_seconds=ANNUNCIATOR_GAP_SECONDS):
    """
    samples: [(time, value_float), ...] ascending for ONE (device, field).
    Returns every activation (run of value==1) as an event — no minimum duration.
    A run is broken by a 0 reading, or by a data gap > gap_seconds (we never claim
    an alarm spanned time we saw no data for). See dating conventions in the
    endpoint docstring.
    """
    gap = timedelta(seconds=gap_seconds)
    events = []
    open_start  = None    # start of the currently open alarm run
    last_active = None     # last sample seen at 1 within the open run
    prev_t      = None

    def close(start, end):
        events.append({
            'start': start.isoformat(),
            'end':   end.isoformat(),
            'duration_seconds': int((end - start).total_seconds()),
            'ongoing': False,
        })

    for t, v in samples:
        active = int(round(v)) == 1

        # Feed stopped mid-alarm → close at the last point we saw it at 1.
        if open_start is not None and prev_t is not None and (t - prev_t) > gap:
            close(open_start, last_active)
            open_start = None

        if active:
            if open_start is None:
                open_start = t
            last_active = t
        elif open_start is not None:
            close(open_start, t)          # end at the clearing 0-sample
            open_start = None
        prev_t = t

    # Alarm still open at the end of the series.
    if open_start is not None:
        if is_today and _is_fresh(last_active):
            events.append({
                'start': open_start.isoformat(),
                'end':   None,
                'duration_seconds': int((datetime.now(timezone.utc) - open_start).total_seconds()),
                'ongoing': True,
            })
        else:
            close(open_start, last_active)  # feed stopped → cleared at last seen
    return events


def get_annunciator_history(bucket, site_id, device_ids, date_str=None):
    """
    Per-annunciator alarm event log for one site, one IST day. Every activation
    of every channel is documented — no sub-5-min filtering (that's the inverter
    timeline's rule, deliberately not applied here).

    channels — full ordered field set (incl. spares), so the UI shows every point
               even on a clean day with no events.
    events   — per device, each channel's runs of 1, merged chronologically.

    Dating: start = first 1-sample; end = clearing 0-sample, or last-seen 1 when
    the feed stopped; ongoing = still 1 on a live today feed (end null).
    """
    start_utc, end_utc, is_today, date_iso = _ist_day_window(date_str)
    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str   = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    client    = get_influx_client()
    query_api = client.query_api()

    try:
        series_by_device = _query_annunciator_series(
            query_api, bucket, site_id, device_ids, start_str, end_str
        )
        client.close()

        label_of = dict(ANNUNCIATOR_ALARM_FIELDS)
        channels = [{'field': f, 'label': lbl} for f, lbl in ANNUNCIATOR_ALARM_FIELDS]

        annunciators = []
        for device_id in device_ids:
            per_field = series_by_device.get(device_id, {})
            events = []
            for field, _lbl in ANNUNCIATOR_ALARM_FIELDS:
                for ev in _build_annunciator_events(per_field.get(field, []), is_today):
                    events.append({'field': field, 'label': label_of[field], **ev})
            events.sort(key=lambda e: e['start'])
            annunciators.append({
                'device_id':   device_id,
                'events':      events,
                'event_count': len(events),
            })

        return {
            'date':     date_iso,
            'is_today': is_today,
            'window':   {'start': start_utc.isoformat(), 'end': end_utc.isoformat()},
            'gap_threshold_seconds': ANNUNCIATOR_GAP_SECONDS,
            'channels': channels,
            'annunciators': annunciators,
        }
    except Exception as e:
        client.close()
        raise e
    

# ─── Household Overview Query ────────────────────────────────────────────────
def get_household_overview(bucket, site_id, inverter_ids, ac_capacity_kw=None, register_inverter_ids=None):
    """
    Household (inverter-only) site page - one function, three internal queries.
    No meter anywhere: site power and energy are sums over the site's inverters.

    Real-data rule: a value the inverter did not send is None, never 0.0, so the
    frontend renders "-". E.g. these inverters stop sending ac_active_power_kw at
    night while still reporting energy and frequency - power is None, the inverter
    stays online. Site totals are the sum of the inverters that reported, and None
    when none did.


    Online / state rules mirror get_inverter_overview: any fresh field = online;
    inverter_status is read from the wider status window and shown only when online.
    """
    client    = get_influx_client()
    query_api = client.query_api()

    device_filter = ' or '.join([f'r.device == "{d}"' for d in inverter_ids])

    flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -6h)
            |> filter(fn: (r) => r._measurement == "solar_data")
            |> filter(fn: (r) => r.site == "{site_id}")
            |> filter(fn: (r) => {device_filter})
            |> filter(fn: (r) =>
                r._field == "ac_active_power_kw"      or
                r._field == "energy_total_kwh"        or
                r._field == "inverter_status"         or
                r._field == "grid_voltage_a_v"        or
                r._field == "ac_current_phase_a"      or
                r._field == "grid_frequency_hz"       or
                r._field == "ac_power_factor"         or
                r._field == "internal_temp_c"         or
                r._field == "dc_input_power_kw"       or
                r._field == "inverter_efficiency_pct" 
            )
            |> map(fn: (r) => ({{r with _value: float(v: r._value)}}))
            |> last()
    '''

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)
        today_by_device = _query_inverters_today_energy(
            query_api, bucket, site_id, inverter_ids, register_ids=register_inverter_ids
        )
    except Exception as e:
        raise Exception(f'Household overview query failed: {str(e)}')
    finally:
        client.close()

    device_data   = {}   # fresh only - live values, drives online/offline
    device_total  = {}   # last-known lifetime counter, kept while offline
    device_status = {}
    device_times  = {}

    for table in tables:
        for record in table.records:
            device = record.values.get('device')
            field  = record.get_field()
            value  = record.get_value()
            time   = record.get_time()

            if field == 'energy_total_kwh':
                device_total[device] = value

            if field == 'inverter_status':
                if _is_status_recent(time):
                    device_status[device] = value
                continue

            if not _is_fresh(time):
                continue

            device_data.setdefault(device, {})[field] = value
            if device not in device_times or time > device_times[device]:
                device_times[device] = time

    def pick(fields, name, digits):
        v = fields.get(name)
        return round(v, digits) if v is not None else None

    inverter_list = []
    online_count  = 0
    state_counts  = {'running': 0, 'stopped': 0, 'standby': 0,
                     'warning': 0, 'fault': 0, 'other': 0}

    for device_id in inverter_ids:
        fields    = device_data.get(device_id, {})
        t         = device_times.get(device_id)
        is_online = bool(fields)

        inverter_status = None
        if is_online:
            online_count += 1
            raw_status = device_status.get(device_id)
            if raw_status is not None:
                code = int(round(raw_status))
                inverter_status = {'code': code, 'label': _inverter_status_label(code)}
            state_counts[_INVERTER_STATE_KEYS.get(
                inverter_status['code'] if inverter_status else None, 'other'
            )] += 1


        inverter_list.append({
            'device_id':               device_id,
            'ac_active_power_kw':      pick(fields, 'ac_active_power_kw', 2),
            'energy_daily_kwh':        pick(today_by_device, device_id, 3),
            'energy_total_kwh':        pick(device_total, device_id, 2),
            'grid_voltage_a_v':        pick(fields, 'grid_voltage_a_v', 1),
            'ac_current_phase_a':      pick(fields, 'ac_current_phase_a', 2),
            'grid_frequency_hz':       pick(fields, 'grid_frequency_hz', 2),
            'ac_power_factor':         pick(fields, 'ac_power_factor', 2),
            'internal_temp_c':         pick(fields, 'internal_temp_c', 1),
            'dc_input_power_kw':       pick(fields, 'dc_input_power_kw', 2),
            'inverter_efficiency_pct': pick(fields, 'inverter_efficiency_pct', 1),
            'status':          'online' if is_online else 'offline',
            'inverter_status': inverter_status,
            'last_updated':    t.isoformat() if t else None,
        })

    # Site totals: sum of the inverters that reported; None when none did.
    powers = [i['ac_active_power_kw'] for i in inverter_list if i['ac_active_power_kw'] is not None]
    todays = [i['energy_daily_kwh']   for i in inverter_list if i['energy_daily_kwh']   is not None]
    totals = [i['energy_total_kwh']   for i in inverter_list if i['energy_total_kwh']   is not None]

    energy_today = round(sum(todays), 2) if todays else None

    return {
        'last_updated': max(
            (t.isoformat() for t in device_times.values() if t), default=None
        ),
        'summary': {
            'active_power_kw':  round(sum(powers), 2) if powers else None,
            'energy_today_kwh': energy_today,
            'energy_month_kwh': None,   # not built for household yet (backlog)
            'energy_total_kwh': round(sum(totals), 2) if totals else None,
            'ac_capacity_kw':   float(ac_capacity_kw) if ac_capacity_kw else None,
            'cuf_pct': (
                round((energy_today / (float(ac_capacity_kw) * 24)) * 100, 2)
                if ac_capacity_kw and energy_today is not None else None
            ),
            'co2_avoided_today_kg': (
                round(energy_today * CO2_AVOIDED_FACTOR_KG_PER_KWH, 2)
                if energy_today is not None else None
            ),
            'inverters_online': online_count,
            'inverters_total':  len(inverter_ids),
            'states':           state_counts,
        },
        # No heartbeat for household: "logger" online when any inverter sends
        # fresh data; last_seen = newest inverter reading. Same shape as before.
        'data_logger': {
            'status':    'online' if online_count else 'offline',
            'last_seen': max(
                (t.isoformat() for t in device_times.values() if t), default=None
            ),
        },
        'inverters': inverter_list,
    }