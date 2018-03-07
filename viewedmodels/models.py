import json
from collections import defaultdict
from json import JSONDecodeError

from django.db import transaction, models, connection
from toposort import toposort_flatten
from .helpers import table_name, get_subclasses, model_default_table_name, get_model, dependency_lookup
from django.db.utils import ProgrammingError

import logging
logger = logging.getLogger(__name__)


def time_from_db():
    with connection.cursor() as c:
        c.execute('SELECT now()::text')
        return c.fetchone()[0]


class ViewedModel(models.Model):

    class Meta:
        abstract = True

    # Specify dependencies. Dependencies used to find model instances and table names to generate
    # drop/create statements in the correct order
    dependencies = (('Aims', 'Activity'),)

    # Every subclass should have a sql method.
    @classmethod
    def sql(cls):
        # dependency_lookup generates a dict with keys of "default" table names to "actual" (model defined) table names
        tables = dependency_lookup(cls.dependencies)

        # Returns a SQL string to be executed
        # This will be wrapped in CREATE VIEW
        return_example = '''
        SELECT
            {mytable}.remote_data_id activity_id, -- Note the use of dependency_lookup here to get a table name from the canonical (app, model) format
            mytable.code aidtypecategory_id,
            "mytable".dollars dollars
             -- Note that any of these 3 forms of table identifiers is valid
             -- The first is recommended as it will track changes to table names
        FROM
            {my_othertable},
        WHERE
            {my_othertable}.remote_data_id = {my_othertable}.activity_id
        AND {my_othertable}.transaction_type_id = 'C'
        AND something > 0
        '''.format(**tables)
        logger.debug(return_example)  # Avoid F841 error
        raise NotImplementedError(
            'This is a demonstration SQL statement which should be replaced in your subclass')
    del(sql)  # Remove this "demo" SQL

    @classmethod
    def sql_drop(cls, **kwargs):
        """Generate SQL code to drop a view

        Kwargs:
            drop_cascade (bool): Add "CASCADE" to the command
            dry_run (bool): Do not execute the script

        """
        params = {'name': table_name(cls),
                  'type': 'MATERIALIZED VIEW' if getattr(cls, 'materialized', False) else 'VIEW',
                  'cascade': 'CASCADE' if kwargs.get('drop_cascade', True) else ''}

        sql = '''DROP %(type)s IF EXISTS "%(name)s" %(cascade)s;''' % (params)

        if not kwargs.get('dryrun', False):
            with connection.cursor() as cursor:
                cursor.execute(sql, None)
        return [sql, None]

    @classmethod
    def sql_create(cls, **kwargs):
        """Generate SQL code to create a view

        Kwargs:
            dry_run (bool): Do not execute the script
        """
        assert hasattr(cls, 'sql'), 'Class {} has no sql statement'.format(cls)

        params = {'name': table_name(cls),
                  'type': 'MATERIALIZED VIEW' if getattr(cls, 'materialized', False) else 'VIEW'}

        sql = 'CREATE {} "{}" AS ({})'.format(
            params['type'], params['name'], cls.sql())

        with connection.cursor() as cursor:
            if not kwargs.get('dryrun', False):
                cursor.execute(sql, getattr(cls, 'params', None))
            return [sql, getattr(cls, 'params', None)]


class MaterializedViewedModel(ViewedModel):

    @classmethod
    def sql(cls):
        pass

    class Meta:
        abstract = True
        managed=False

    concurrently = True  # Concurrently refresh this view
    materialized = True

    @classmethod
    def sql_refresh(cls, **kwargs):
        """Generate code to refresh a Materialized View"""

        if not cls.update_mv():
            logger.info('Update skipped')
            return
        concurrently = 'CONCURRENTLY' if cls.concurrently is True else ''

        sql = 'REFRESH MATERIALIZED VIEW {} {}'.format(concurrently, table_name(cls))

        if not kwargs.get('dryrun', False):
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, None)
                cls.set_comment()
            except ProgrammingError as e:
                logger.error(e)
                raise
            return [sql, None]
            
    @classmethod
    def sql_vacuum(cls, **kwargs):
        """Generate code to vacuum a Materialized View"""

        sql = 'VACUUM ANALYZE {}'.format(table_name(cls))

        if not kwargs.get('dryrun', False):
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, None)
            except ProgrammingError as e:
                logger.error(e)
                raise
            return [sql, None]

    @classmethod
    def get_comment(cls):
        with connection.cursor() as get_comment:
            get_comment.execute('''
            SELECT description
            FROM   pg_description
            WHERE  objoid = '{}'::regclass;
            '''.format(table_name(cls)))
            comment = get_comment.fetchone()
            if not comment:
                return None
            return comment[0]

    @classmethod
    def _set_comment(cls, comment):
        with connection.cursor() as set_comment:
            set_comment.execute(
                "COMMENT ON MATERIALIZED VIEW {} IS '{}'".format(table_name(cls), comment))

    @classmethod
    def set_comment(cls):
        """
        Comment to write on a materialized view update
        :return:
        """
        comment = cls.get_comment() or '{}'
        # Comment should be JSON string
        try:
            comment = json.loads(comment)
        except JSONDecodeError:
            comment = {'old_content': '%s' % (comment)}
        comment['last_updated'] = time_from_db()
        cls._set_comment(json.dumps(comment))

    @classmethod
    def update_mv(cls):
        """
        Return False if an mv SHOULD NOT be updated
        :return:
        """
        # For instance, we may want to have a "do not refresh if my data is less than 60 seconds old":
        # age = cls.interval_since_last_update()
        # if age < 60:
        #     logger.info('View is less than one minute old. No update is done.')
        #     return False
        return True

    @classmethod
    def interval_since_last_update(cls):
        """
        Seconds since this view was last refreshed according to the comment
        """
        with connection.cursor() as cursor:
            cursor.execute("""
            SELECT now() - (description::json -> 'last_updated')::TEXT::TIMESTAMP WITH TIME ZONE
            FROM   pg_description
            WHERE  objoid = 'aims_transactionvalueusd'::regclass;
            """.format(table_name(cls)))
            return cursor.fetchone()[0].seconds


class ViewDefinition:

    @classmethod
    def sort_dependencies(cls, apps='all'):
        """
        Return a list of models in their order of dependency
        """
        dependencies = defaultdict(set)
        model_objects = []
        if apps != 'all':
            apps = apps.split(',')

        for c in get_subclasses(ViewedModel):
            if apps != 'all' and c._meta.app_label not in apps:
                continue
            if c._meta.abstract is True:
                continue
            class_string = model_default_table_name(c)
            for dependency in getattr(c, 'dependencies', []):
                try:
                    m = get_model(app_name=dependency[0], model_name=dependency[1])
                    dependencies[class_string].add(model_default_table_name(m))
                except LookupError as m:
                    logger.error(dependency)
                    raise LookupError("Dependency of %s failed", dependency)

        flattened = toposort_flatten(dependencies)
        logger.info(flattened)
        for name in flattened:
            splitname = name.split('_')
            model = splitname[-1]
            app = '_'.join(splitname[:-1])
            if app == 'None' or model == 'viewedmodel':
                continue
            model_object = get_model(app, model)
            if hasattr(model_object, 'sql'):
                model_objects.append(model_object)

        return model_objects

    @classmethod
    def drop_all_statements(cls, dryrun=True, apps='all'):
        """
        Return a set of statements to drop views in a sorted order which *should* always work
        ie sorted in such a way that no dependencies of a view will be dropped out of order
        """
        ordered_models = cls.sort_dependencies(apps=apps)
        ordered_models.reverse()
        return [model.sql_drop(drop_cascade=False, dryrun=dryrun) for model in ordered_models]

    @classmethod
    def create_all_statements(cls, dryrun=True, apps='all'):
        """
        Return a set of statements to drop views in a sorted order which *should* always work
        ie sorted in such a way that no dependencies of a view will be created out of order
        """
        ordered_models = cls.sort_dependencies(apps=apps)
        return [model.sql_create(drop_cascade=False, dryrun=dryrun) for model in ordered_models]

    @classmethod
    @transaction.atomic
    def recreate(cls, apps='all'):
        """
        Generate the SQL required to drop and create all of the views of subclasses of
        ViewDefinition, in the correct order
        """
        # Generate a "DROP VIEW" and "CREATE VIEW" statement for each applicable model
        # Statements returned ought to be a 2-element tuple of (statement, params)
        statements = cls.drop_all_statements(apps=apps)
        statements.extend(cls.create_all_statements(apps=apps))
        with connection.cursor() as cursor:
            for s in statements:
                if len(s) == 0:
                    s += None  # params for query
                cursor.execute(s[0], s[1])

    @classmethod
    @transaction.atomic
    def refresh_mv(cls, apps='all', **kwargs):
        """
        Refresh all materialized views on the application
        """
        ordered_models = cls.sort_dependencies(apps=apps)
        mat_models = [m for m in ordered_models if getattr(
            m, 'materialized', False)]
        return [model.sql_refresh(**kwargs) for model in mat_models]


    @classmethod
    def vacuum_mv(cls, apps='all', **kwargs):
        """
        Refresh all materialized views on the application
        """
        ordered_models = cls.sort_dependencies(apps=apps)
        mat_models = [m for m in ordered_models if getattr(
            m, 'materialized', False)]
        return [model.sql_vacuum(**kwargs) for model in mat_models]

