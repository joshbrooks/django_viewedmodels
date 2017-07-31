from collections import defaultdict

from django.db import models, transaction
from django.db import connection
from django.apps import apps

from toposort import toposort_flatten


def table_name(model):
    '''
    Return tablename from a model
    Careful: Accesses a private class _meta on the model
    '''
    return model._meta.db_table


def get_model(app_name, model_name):
    '''
    Return model instance from a tuple (app, model)
    '''
    try:
        app = apps.get_app_config(app_name.lower())
    except LookupError:
        raise LookupError('App {} not found'.format(app_name, model_name))
    try:
        model = app.get_model(model_name.lower())
    except LookupError:
        raise LookupError('App {} Model {} not found'.format(app_name, model_name))
    return model


def table_name_get_model(app_name, model_name):
    """
    Return table name from an (app , model) tuple
    """
    return table_name(get_model(app_name, model_name))


def model_default_table_name(model):
    return default_table_name(model._meta.app_label, model._meta.model_name)


def default_table_name(app_name, model_name):
    from django.db import connection
    from django.db.backends.utils import truncate_name
    db_table = "%s_%s" % (app_name, model_name)
    db_table = truncate_name(db_table, connection.ops.max_name_length())
    return db_table.lower()


def get_subclasses(cls):
    """
    Wraper function returns subclasses of a single class
    :param cls:
    :return:
    """
    for subclass in cls.__subclasses__():
        yield from get_subclasses(subclass)
        yield subclass


class ViewDefinition:

    @classmethod
    def sort_dependencies(cls):
        """
        Return a list of models in their order of dependency
        """
        dependencies = defaultdict(set)
        model_objects = []

        for c in get_subclasses(ViewedModel):
            class_string = model_default_table_name(c)
            for dependency in getattr(c, 'dependencies', []):
                m = get_model(app_name=dependency[0], model_name=dependency[1])
                dependencies[class_string].add(model_default_table_name(m))

        for name in toposort_flatten(dependencies):
            app, model = name.split('_')
            if app == 'None' or model == 'viewedmodel':
                continue
            model_object = get_model(app, model)
            if hasattr(model_object, 'sql'):
                model_objects.append(model_object)
        return model_objects

    @classmethod
    def drop_all_statements(cls, dryrun=True):
        """
        Return a set of statements to drop views in a sorted order which *should* always work
        ie sorted in such a way that no dependencies of a view will be dropped out of order
        """
        ordered_models = cls.sort_dependencies()
        ordered_models.reverse()
        return [model.sql_drop(drop_cascade=False, dryrun=dryrun) for model in ordered_models]

    @classmethod
    def create_all_statements(cls, dryrun=True):
        """
        Return a set of statements to drop views in a sorted order which *should* always work
        ie sorted in such a way that no dependencies of a view will be created out of order
        """
        ordered_models = cls.sort_dependencies()
        return [model.sql_create(drop_cascade=False, dryrun=dryrun) for model in ordered_models]

    @classmethod
    @transaction.atomic
    def recreate(cls):
        """
        Generate the SQL required to drop and create all of the views of subclasses of
        ViewDefinition, in the correct order
        """
        # Generate a "DROP VIEW" and "CREATE VIEW" statement for each applicable model
        # Statements returned ought to be a 2-element tuple of (statement, params)
        statements = cls.drop_all_statements()
        statements.extend(cls.create_all_statements())
        with connection.cursor() as cursor:
            for s in statements:
                if len(s) == 0:
                    s += None  # params for query
                cursor.execute(s[0], s[1])


def dependency_lookup(dependencies):
    """
    Generate a lookup table to convert "normal" underscored Django table names to
    custom names, if used, and wrap in PostgreSQL table identifier (double quote)
    """
    d = {default_table_name(dep[0], dep[1]): '"{}"'.format(table_name_get_model(dep[0], dep[1])) for dep in dependencies}
    return d


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
        assert hasattr(cls, 'sql'), 'Class {} has no sql statement'.format(cls)
        if kwargs.get('drop_cascade', True):
            sql = 'DROP VIEW IF EXISTS "{}" CASCADE;'
        else:
            sql = 'DROP VIEW IF EXISTS "{}";'
        sql = sql.format(table_name(cls))

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

        sql = 'CREATE VIEW "{}" AS ({})'.format(table_name(cls), cls.sql())

        with connection.cursor() as cursor:
            if not kwargs.get('dryrun', False):
                cursor.execute(sql, getattr(cls, 'params', None))
            return [sql, getattr(cls, 'params', None)]
