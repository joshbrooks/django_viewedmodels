from django.apps import apps
from django.db.backends.utils import truncate_name
from django.db import connection


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
        raise LookupError(
            'App {} Model {} not found'.format(app_name, model_name))
    return model


def table_name_get_model(app_name, model_name):
    """
    Return table name from an (app , model) tuple
    """
    return table_name(get_model(app_name, model_name))


def model_default_table_name(model):
    return default_table_name(model._meta.app_label, model._meta.model_name)


def default_table_name(app_name, model_name):
    db_table = "%s_%s" % (app_name, model_name)
    db_table = truncate_name(db_table, connection.ops.max_name_length())
    return db_table.lower()


def get_subclasses(cls):
    """
    Wraper function returns subclasses of a single class
    Requires py 3.3+
    :param cls:
    :return:
    """
    for subclass in cls.__subclasses__():
        yield from get_subclasses(subclass)
        yield subclass


def dependency_lookup(dependencies):
    """
    Generate a lookup table to convert "normal" underscored Django table names to
    custom names, if used, and wrap in PostgreSQL table identifier (double quote)
    """
    d = {default_table_name(dep[0], dep[1]): '"{}"'.format(
        table_name_get_model(dep[0], dep[1])) for dep in dependencies}
    return d
