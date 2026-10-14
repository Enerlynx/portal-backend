# apps/influx/portfolio_views.py
# Role-neutral portfolio overview — the landing page for any user with >1 site.
# GET /api/v1/influx/portfolio/overview/
#
# Scoped via TenantFilterMixin, so the same endpoint serves all three roles:
#   ADMIN     → every customer
#   INSTALLER → customers reachable through their sites
#   CUSTOMER  → themselves, one customer block
# Poll every 60 seconds.

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from core.permissions import IsAnyRole
from core.mixins import TenantFilterMixin
from apps.sites.models import Site, Device

from datetime import datetime, timedelta, timezone as dt_timezone
from django.db.models import Sum

from apps.reports.models import DailySiteSnapshot
from .queries import get_portfolio_overview, MIN_POA_KWH_M2_FOR_PR, CO2_AVOIDED_FACTOR_KG_PER_KWH


def _prior_month_energy_by_site(site_pks):
    """
    {site_pk: kWh} — sum of DailySiteSnapshot.energy_today_kwh from the 1st of
    the IST month through YESTERDAY, for all sites in ONE grouped query.
    Bulk mirror of dashboard_views._month_to_date_energy_kwh's Postgres half —
    keep in lockstep. Sites with no rows (or the 1st of the month) are absent;
    caller defaults them to 0.0, same as the single-site version.
    """
    ist         = dt_timezone(timedelta(hours=5, minutes=30))
    today_ist   = datetime.now(ist).date()
    month_start = today_ist.replace(day=1)

    rows = (
        DailySiteSnapshot.objects
        .filter(site_id__in=site_pks, date__gte=month_start, date__lt=today_ist)
        .values('site_id')
        .annotate(total=Sum('energy_today_kwh'))
    )
    return {
        r['site_id']: float(r['total']) if r['total'] is not None else 0.0
        for r in rows
    }


class PortfolioOverviewView(TenantFilterMixin, APIView):
    permission_classes = [IsAnyRole]

    def _scope_name(self, user):
        """Heading text for the portfolio page. ADMIN has no single tenant."""
        if user.role == 'INSTALLER':
            return user.installer.name if user.installer_id else None
        if user.role == 'CUSTOMER':
            return user.customer.name if user.customer_id else None
        if user.role == 'SITE_USER':
            first = user.sites.select_related('customer').first()
            return first.customer.name if first else None
        return 'All Customers'

    def _empty_response(self, user, include_energy=True):
        return Response({
            'scope_name': self._scope_name(user),
            'portfolio_summary': {
                'total_active_power_kw':  0.0,
                'total_energy_today_kwh': 0.0 if include_energy else None,
                'total_energy_month_kwh': 0.0 if include_energy else None,
                'co2_avoided_today_kg': 0.0 if include_energy else None,
                'ac_capacity_kw':         0.0,
                'cuf_pct': None,
                'sites_online':           0,
                'sites_total':            0,
                'inverters_online':       0,
                'inverters_total':        0,
                'loggers_online':         0,
                'loggers_total':          0,
                'states': {'running': 0, 'stopped': 0, 'standby': 0,
                            'warning': 0, 'fault': 0, 'other': 0},

            },
            'customers': [],
        })

    def get(self, request):
        user = request.user

        include_energy = request.query_params.get('detail') != 'basic'

        # Utility and Household are separate portfolio pages: one category per call.
        category = request.query_params.get('category', Site.Category.UTILITY)
        if category not in Site.Category.values:
            return Response(
                {'detail': f'Unknown category: {category}'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Tenant filtering is the ONLY access control here — never read
        # request.user.installer to scope the queryset, or ADMIN and CUSTOMER break.
        sites = list(
            self.get_filtered_sites()
            .filter(site_type=Site.SiteType.GENERATION, is_active=True, category=category)
            .select_related('customer', 'installer')
            .order_by('customer__name', 'name')
        )

        if not sites:
            return self._empty_response(user, include_energy)

        site_pks = [s.pk for s in sites]

        ref_meters = Site.reference_meters_for(sites)
        inverters = Device.objects.filter(
            site_id__in=site_pks, device_type='INVERTER', is_active=True
        )

        # One weather station per plant — lowest pk wins, same as .first() in Plant Overview.
        weather_by_pk = {}
        for w in Device.objects.filter(
            site_id__in=site_pks, device_type='WEATHER_STATION', is_active=True
        ).order_by('pk'):
            weather_by_pk.setdefault(w.site_id, w.influx_device_id)

        # {plant_pk: (meter_site_tag, meter_device_id)} — the meter's OWN site
        # tag, which differs from the plant's when the reference meter lives on a
        # substation. meter1 is not unique across sites, so both tags travel.
        meter_by_pk = {
            pk: m.influx_location            # (site_influx_id, device_id)
            for pk, m in ref_meters.items() if m
        }
        inverters_by_pk = {}
        register_by_pk  = {}
        for inv in inverters:
            inverters_by_pk.setdefault(inv.site_id, []).append(inv.influx_device_id)
            if inv.energy_today_source == Device.EnergyTodaySource.REGISTER:
                register_by_pk.setdefault(inv.site_id, []).append(inv.influx_device_id)

        # Group by bucket — one Flux query pair per bucket. influx_site_id is safe
        # as a key *inside* a bucket (unique per customer), which is why pk_map
        # rides along to translate back on the way out.
        bucket_groups = {}
        for site in sites:
            bucket = site.customer.influx_bucket
            group = bucket_groups.setdefault(
                bucket, {'pk_map': {}, 'meter_map': {}, 'inverters_map': {}, 'weather_map': {},
                         'household': set(), 'register_map': {}, 'logger_tags': {}}
            )
            iid = site.influx_site_id
            household = site.category == Site.Category.HOUSEHOLD
            group['pk_map'][iid] = site.pk
            if site.pk in meter_by_pk and not household:
                group['meter_map'][iid] = meter_by_pk[site.pk]   # now a (tag, device) pair
            group['inverters_map'][iid] = inverters_by_pk.get(site.pk, [])
            if site.pk in weather_by_pk:
                group['weather_map'][iid] = weather_by_pk[site.pk]
            if household:
                group['household'].add(iid)
                group['register_map'][iid] = register_by_pk.get(site.pk, [])
        try:
            influx_results = get_portfolio_overview(bucket_groups, include_energy=include_energy)
        except Exception as e:
            return Response({'detail': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Month energy, Postgres half. Guarded like Plant Overview: a DB hiccup
        # nulls month energy instead of 500-ing the landing page.
        prior_month = None
        if include_energy:
            try:
                prior_month = _prior_month_energy_by_site(site_pks)
            except Exception:
                prior_month = None

        # Group sites by customer for response shape
        customer_sites = {}
        customer_obj   = {}
        for site in sites:
            cid = site.customer_id
            if cid not in customer_sites:
                customer_sites[cid] = []
                customer_obj[cid]   = site.customer
            customer_sites[cid].append(site)

        # Assemble response + compute portfolio totals in one pass
        total_active_power     = 0.0
        total_energy_today     = 0.0
        total_energy_month     = 0.0
        total_ac_capacity      = 0.0
        cuf_energy_sum   = 0.0   # energy today of sites that have AC capacity
        cuf_capacity_sum = 0.0   # their AC capacity
        sites_online           = 0
        power_reported         = False  # False when no site reported power (household at night)
        logger_keys            = set()  # distinct loggers; household sites share one
        logger_keys_online     = set()
        inverters_online_total = 0
        inverters_total_total  = 0
        inverters_online_total = 0
        inverters_total_total  = 0
        states_total           = {'running': 0, 'stopped': 0, 'standby': 0,
                                  'warning': 0, 'fault': 0, 'other': 0}

        customers_list = []
        for cid, cust_sites in customer_sites.items():
            site_cards = []
            for site in cust_sites:
                r = influx_results.get(site.pk, {})

                active_power = r.get('active_power_kw',  0.0)
                energy_today = r.get('energy_today_kwh') 
                meter_online = r.get('meter_online',     False)
                inv_online   = r.get('inverters_online', 0)
                inv_total    = r.get('inverters_total',  0)
                inv_states   = r.get('states', {'running': 0, 'stopped': 0, 'standby': 0,
                                                'warning': 0, 'fault': 0, 'other': 0})
                logger_online    = r.get('logger_online',    False)
                logger_last_seen = r.get('logger_last_seen')

                poa_kwh_m2     = r.get('poa_kwh_m2', 0.0)
                weather_online = r.get('weather_online', False)

                # Same formulas and gates as get_plant_overview. basic → all None.
                energy_month_kwh      = None
                performance_ratio_pct = None
                cuf_pct               = None
                if include_energy:
                    if prior_month is not None and site.category != Site.Category.HOUSEHOLD:
                        energy_month_kwh = round(
                            prior_month.get(site.pk, 0.0) + (energy_today or 0.0), 2
                        )
                    # PR null when: no station, station offline, or POA below threshold
                    # (plus meter offline / no DC capacity, as in Plant Overview).
                    if (meter_online and site.dc_capacity_kw and weather_online
                            and poa_kwh_m2 >= MIN_POA_KWH_M2_FOR_PR):
                        performance_ratio_pct = round(
                            (energy_today / (float(site.dc_capacity_kw) * poa_kwh_m2)) * 100, 2
                        )
                    if site.ac_capacity_kw:
                        cuf_pct = round(
                            (energy_today / (float(site.ac_capacity_kw) * 24)) * 100, 2
                        )
                    if site.ac_capacity_kw:
                        cuf_pct = round(
                            (energy_today / (float(site.ac_capacity_kw) * 24)) * 100, 2
                        )
                        cuf_energy_sum   += energy_today
                        cuf_capacity_sum += float(site.ac_capacity_kw)

                if energy_month_kwh is not None:
                    total_energy_month += energy_month_kwh

                if active_power is not None:
                    total_active_power += active_power
                    power_reported = True
                if energy_today is not None:
                    total_energy_today += energy_today
                total_ac_capacity      += float(site.ac_capacity_kw or 0)
                household = site.category == Site.Category.HOUSEHOLD
                if household:
                    # No heartbeat for household: the site, and the logger shown
                    # on its card, is online when any inverter sends fresh data.
                    logger_online    = inv_online > 0
                    logger_last_seen = r.get('last_updated')
                if logger_online:
                    sites_online += 1
                inverters_online_total += inv_online
                inverters_total_total  += inv_total
                # Household sites of one customer share a gateway, so its logger
                # is counted once. Same tag rule as logger_tags above.
                logger_key = (
                    site.customer.influx_bucket,
                    (site.customer.influx_client_id if household else None) or site.influx_site_id,
                )
                logger_keys.add(logger_key)
                if logger_online:
                    logger_keys_online.add(logger_key)
                for k, v in inv_states.items():
                    states_total[k] += v

                site_cards.append({
                    'site_id':          site.pk,
                    'site_name':        site.name,
                    'category':         site.category,
                    'location':         site.location,
                    'installer_name':   site.installer.name if site.installer_id else None,
                    'active_power_kw':  active_power,
                    'energy_today_kwh': energy_today,
                    'energy_month_kwh':      energy_month_kwh,
                    'performance_ratio_pct': performance_ratio_pct,
                    'cuf_pct':               cuf_pct,
                    'capabilities':          {'weather': site.pk in weather_by_pk},
                    'dc_capacity_kw':   float(site.dc_capacity_kw) if site.dc_capacity_kw is not None else None,
                    'ac_capacity_kw':   float(site.ac_capacity_kw) if site.ac_capacity_kw is not None else None,
                    'meter_online':     meter_online,
                    'inverters_online': inv_online,
                    'inverters_total':  inv_total,
                    'states':           inv_states,
                    'logger_online':    logger_online,
                    'logger_last_seen': logger_last_seen,
                    'last_updated':     r.get('last_updated'),
                })

            customers_list.append({
                'customer_id':   customer_obj[cid].pk,
                'customer_name': customer_obj[cid].name,
                'sites':         site_cards,
            })

        return Response({
            'scope_name': self._scope_name(user),
            'portfolio_summary': {
                'total_active_power_kw':  round(total_active_power, 2) if power_reported else None,
                'total_energy_today_kwh': round(total_energy_today, 2) if include_energy else None,
                'total_energy_month_kwh': (
                    round(total_energy_month, 2)
                    if include_energy and prior_month is not None
                    and category != Site.Category.HOUSEHOLD else None
                ),
                'cuf_pct': (
                    round((cuf_energy_sum / (cuf_capacity_sum * 24)) * 100, 2)
                    if include_energy and cuf_capacity_sum > 0 else None
                ),
                'co2_avoided_today_kg': (
                    round(total_energy_today * CO2_AVOIDED_FACTOR_KG_PER_KWH, 2)
                    if include_energy else None
                ),
                'ac_capacity_kw':         round(total_ac_capacity, 2),
                'sites_online':           sites_online,
                'sites_total':            len(sites),
                'inverters_online':       inverters_online_total,
                'inverters_total':        inverters_total_total,
                'loggers_online':         len(logger_keys_online),
                'loggers_total':          len(logger_keys),
                'states':                 states_total
            },
            'customers': customers_list,
        })