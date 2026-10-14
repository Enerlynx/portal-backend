# apps/reports/management/commands/send_household_whatsapp.py
#
# Daily energy-today WhatsApp for HOUSEHOLD (rooftop) owners.
# One message per household site, to each active SITE_USER assigned to it
# (via the `sites` M2M) who has a whatsapp_number. Runs at ~19:30 IST next to
# send_daily_whatsapp, reading live Influx: inverters have stopped generating by
# then but still report energy, so the figure is the day's final value.
#
#   python manage.py send_household_whatsapp               # all household sites
#   python manage.py send_household_whatsapp --site=21     # one site
#   python manage.py send_household_whatsapp --dry-run     # resolve + log, DON'T send
#
# Energy = sum of the site's inverters, same helper as the household page
# (COUNTER or REGISTER per Device.energy_today_source). A site whose inverters
# reported nothing today is SKIPPED (no "-" message); a real 0 is sent as 0.0.
#
# One site/user failing never stops the rest. Non-zero exit if any send failed.

import sys
from datetime import datetime, timedelta, timezone as dt_timezone

from django.core.management.base import BaseCommand
from django.db.models import Prefetch

from apps.users.models import User
from apps.sites.models import Site, Device
from apps.influx.client import get_influx_client
from apps.influx.queries import _query_inverters_today_energy

# AiSensy campaign for the approved household template (Template A):
#   {{1}} name, {{2}} energy today kWh, {{3}} date
# NOTE: must match the campaign name in the AiSensy dashboard exactly.
CAMPAIGN_NAME = 'household_daily_v1'


class Command(BaseCommand):
    help = "Sends today's energy to household (rooftop) SITE_USERs via WhatsApp."

    def add_arguments(self, parser):
        parser.add_argument('--site', type=int, default=None,
                            help='Postgres Site pk. Omit to run all household sites.')
        parser.add_argument('--dry-run', action='store_true',
                            help='Resolve recipients + data and log, but do NOT send.')

    def handle(self, *args, **options):
        site_pk = options.get('site')
        dry_run = options.get('dry_run')

        from core.whatsapp import send_template

        recipients_qs = (
            User.objects.filter(role=User.Role.SITE_USER, is_active=True)
            .exclude(whatsapp_number__isnull=True)
            .exclude(whatsapp_number='')
        )
        sites = (
            Site.objects.filter(
                category=Site.Category.HOUSEHOLD,
                site_type=Site.SiteType.GENERATION,
                is_active=True,
            )
            .select_related('customer')
            .prefetch_related(Prefetch('site_users', queryset=recipients_qs))
            .order_by('customer__name', 'name')
        )
        if site_pk:
            sites = sites.filter(pk=site_pk)

        sites = [s for s in sites if s.site_users.all()]
        if not sites:
            self.stdout.write('No household sites with a WhatsApp recipient found.')
            return

        ist_tz = dt_timezone(timedelta(hours=5, minutes=30))
        date_label = datetime.now(ist_tz).strftime('%d %b %Y')

        self.stdout.write(
            f'{"[DRY RUN] " if dry_run else ""}'
            f'Household send for {len(sites)} site(s), {date_label}...'
        )

        sent = failed = skipped = 0

        client = get_influx_client()
        try:
            query_api = client.query_api()

            for site in sites:
                users = list(site.site_users.all())

                inverters = list(Device.objects.filter(
                    site=site, device_type=Device.DeviceType.INVERTER, is_active=True
                ))
                if not inverters:
                    skipped += len(users)
                    self.stdout.write(self.style.WARNING(
                        f'{site.name}: no active inverters; skipped.'
                    ))
                    continue

                try:
                    by_device = _query_inverters_today_energy(
                        query_api,
                        site.customer.influx_bucket,
                        site.influx_site_id,
                        [d.influx_device_id for d in inverters],
                        register_ids=[
                            d.influx_device_id for d in inverters
                            if d.energy_today_source == Device.EnergyTodaySource.REGISTER
                        ],
                    )
                except Exception as e:
                    failed += len(users)
                    self.stderr.write(self.style.ERROR(
                        f'{site.name}: data query FAILED — {e}'
                    ))
                    continue

                if not by_device:
                    skipped += len(users)
                    self.stdout.write(self.style.WARNING(
                        f'{site.name}: no inverter energy today; skipped.'
                    ))
                    continue

                energy = f'{abs(sum(by_device.values())):.1f}'

                for user in users:
                    name = f'{user.first_name} {user.last_name}'.strip() or 'Customer'
                    params = [
                        name,        # {{1}}
                        energy,      # {{2}} energy today kWh
                        date_label,  # {{3}}
                    ]

                    if dry_run:
                        sent += 1
                        self.stdout.write(
                            f'[{user.pk}] {site.name} -> {user.whatsapp_number} | {params}'
                        )
                        continue

                    ok, detail = send_template(
                        user.whatsapp_number, CAMPAIGN_NAME, params, user_name=name
                    )
                    if ok:
                        sent += 1
                        self.stdout.write(self.style.SUCCESS(
                            f'[{user.pk}] {site.name} -> {user.whatsapp_number}: '
                            f'sent (energy={energy})'
                        ))
                    else:
                        failed += 1
                        self.stderr.write(self.style.ERROR(
                            f'[{user.pk}] {site.name} -> {user.whatsapp_number}: '
                            f'FAILED — {detail}'
                        ))
        finally:
            client.close()

        verb = 'would send' if dry_run else 'sent'
        summary = f'Done. {sent} {verb}, {skipped} skipped, {failed} failed.'
        if failed:
            self.stderr.write(self.style.ERROR(summary))
            sys.exit(1)
        self.stdout.write(self.style.SUCCESS(summary))