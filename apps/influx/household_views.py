# apps/influx/household_views.py
# Household (inverter-only) site page.
#   /household/overview/  → live site totals + per-inverter values, polls every 60s
# Charts reuse the existing inverter endpoints:
#   /inverter/power-trend/?site=              → site power trend (sums the inverters)
#   /inverter/detail/daily-energy/?site=&device=  → 7-day bars per inverter

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from core.permissions import IsAnyRole
from core.mixins import TenantFilterMixin
from apps.sites.models import Site, Device

from .queries import get_household_overview


class HouseholdOverviewView(TenantFilterMixin, APIView):
    """
    GET /api/v1/household/overview/?site=1
    Single endpoint for the household site page. Household sites only.
    """
    permission_classes = [IsAnyRole]

    def get(self, request):
        site_id = request.query_params.get('site')

        if not site_id:
            return Response(
                {'detail': 'site param is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            site = self.get_filtered_sites().select_related('customer').get(pk=site_id)
        except Exception:
            return Response(
                {'detail': 'Site not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        if site.category != Site.Category.HOUSEHOLD:
            return Response(
                {'detail': 'Not a household site'},
                status=status.HTTP_400_BAD_REQUEST
            )

        inverters = list(Device.objects.filter(
            site=site, device_type='INVERTER', is_active=True
        ).order_by('pk'))
        if not inverters:
            return Response(
                {'detail': 'No active inverters found'},
                status=status.HTTP_404_NOT_FOUND
            )

        by_influx_id = {d.influx_device_id: d for d in inverters}

        try:
            data = get_household_overview(
                bucket       = site.customer.influx_bucket,
                site_id      = site.influx_site_id,
                inverter_ids = list(by_influx_id),
                ac_capacity_kw = site.ac_capacity_kw,
                register_inverter_ids = [
                    d.influx_device_id for d in inverters
                    if d.energy_today_source == Device.EnergyTodaySource.REGISTER
                ],
            )
        except Exception as e:
            return Response(
                {'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        # id is the Device pk - the inverter chart endpoints take it as ?device=
        for inv in data['inverters']:
            device = by_influx_id[inv['device_id']]
            inv['id']   = device.pk
            inv['name'] = device.name

        return Response({
            'site':     site.name,
            'customer': site.customer.name,
            'category': site.category,
            **data,
        })