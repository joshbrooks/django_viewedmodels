from collections import defaultdict
from django.db import transaction, models, connection
from toposort import toposort_flatten
from .helpers import table_name, get_subclasses, model_default_table_name, get_model, dependency_lookup


class ViewedModel(models.Model):
    """
    Features To Work On
     - Allow Materialized Views
    """
    class Meta:
        abstract = True
        managed = False

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
        return '''
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
        raise NotImplementedError('This is a demonstration SQL statement which should be replaced in your subclass')
    del(sql)  # Remove this "demo" SQL

    @classmethod
    def sql_drop(cls, **kwargs):
        """Generate SQL code to drop a view

        Kwargs:
            drop_cascade (bool): Add "CASCADE" to the command
            dry_run (bool): Do not execute the script

        """

        if getattr(cls, 'materialized', False):
            view_type = 'MATERIALIZED VIEW'
        else:
            view_type = 'VIEW'

        if kwargs.get('drop_cascade', True):
            sql = 'DROP {} IF EXISTS "{}" CASCADE;'
        else:
            sql = 'DROP {} IF EXISTS "{}";'

        sql = sql.format(view_type, table_name(cls))

        if not kwargs.get('dryrun', False):
            with connection.cursor() as cursor:
                cursor.execute(sql)
        return [sql, None]

    @classmethod
    def sql_create(cls, **kwargs):
        """Generate SQL code to create a view

        Kwargs:
            drop (bool): Drop the view first. Normally you'll want to, unless using with dry_run to
                format a list of SQL to include in one transaction.
            drop_cascade (bool): Add "CASCADE" to the drop command
            dry_run (bool): Do not execute the script
        """
        assert hasattr(cls, 'sql'), 'Class {} has no sql statement'.format(cls)
        statements = []
        if kwargs.get('drop', False):
            drop_sql = cls.sql_drop(**kwargs)
            statements.append(drop_sql)

        if getattr(cls, 'materialized', False):
            view_type = 'MATERIALIZED VIEW'
        else:
            view_type = 'VIEW'

        sql = 'CREATE {} "{}" AS ({})'.format(view_type, table_name(cls), cls.sql())

        with connection.cursor() as cursor:
            if not kwargs.get('dryrun', False):
                cursor.execute(sql, getattr(cls, 'params', None))
            return [sql, getattr(cls, 'params', None)]


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
            class_string = model_default_table_name(c)
            for dependency in getattr(c, 'dependencies', []):
                m = get_model(app_name=dependency[0], model_name=dependency[1])
                dependencies[class_string].add(model_default_table_name(m))

        flattened = toposort_flatten(dependencies)
        print(flattened)
        for name in flattened:
            app, model = name.split('_')
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
