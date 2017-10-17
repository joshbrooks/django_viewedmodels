from django.core.management.base import BaseCommand
from viewedmodels.models import ViewDefinition
import logging
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Refresh materialized views'

    def add_arguments(self, parser):
        parser.add_argument('--apps', default='all',
                            help='Comma separated list of apps to rebuild views in')

    def handle(self, *args, **options):
        logger.info('Regenerating views.')
        ViewDefinition.refresh_mv(apps=str(options['apps']))
