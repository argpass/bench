#!coding: utf-8
import json
import os
from importlib import import_module
from pending.core.exceptions import ImproperlyConfigured


ENVIRONMENT_NAME = 'CONFIG_FILE'


def check_database_conf(config):
    assert len(config.keys()) > 0, u"invalid settings of database"

required_settings = {
    'INSTALLED_APPS': lambda x: x
}


class LazySettings(object):
    """Lazy Object as a proxy of config object
    """
    def __init__(self):
        self._wrapped = None

    def _setup(self):
        """Setup settings from either config file or setting module
        """
        cfg_file = os.environ.get(ENVIRONMENT_NAME)
        if not cfg_file:
            desc = u"Before using config module, " \
                   u"you must set " \
                   u"environment variable `%s`" % (ENVIRONMENT_NAME,)
            raise ImproperlyConfigured(desc)

        self._setup_from_settings(cfg_file)
        self._check_settings()

    def _setup_from_settings(self, cfg_file):
        if not os.path.exists(cfg_file):
            raise ImproperlyConfigured(
                u"the config file `%s` doesn't exist yet!" % cfg_file)
        try:
            with open(cfg_file) as fi:
                this_settings_dict = dict(json.load(fi))
        except Exception as e:
            raise ImproperlyConfigured(
                u"Can not parse config file with err:%s %s" % e)
        else:
            # update settings with app.settings
            merged_settings_dict = dict()
            apps = this_settings_dict.get("INSTALLED_APPS", [])
            app_default_settings_list = []
            for app_name in apps:
                try:
                    app_default_settings = import_module(
                        '%s.settings' % app_name)
                    app_default_settings_list.append(app_default_settings)
                    merged_settings_dict.update(vars(app_default_settings))
                except ImportError:
                    pass
            merged_settings_dict.update(this_settings_dict)
            for app_default_settings in app_default_settings_list:
                vars(app_default_settings).update(merged_settings_dict)
            self._wrapped = merged_settings_dict

    def _check_settings(self):
        # To check required settings
        for key, validator in required_settings.items():
            config = self._wrapped.get(key)
            assert config, u"need settings of %s" % key
            validator(config)

    def __getattr__(self, name):
        if self._wrapped is None:
            self._setup()
        try:
            value = self._wrapped[name]
            return value
        except KeyError:
            desc = u"%s hasn't been configured in settings" % name
            raise ImproperlyConfigured(desc)


settings = LazySettings()

