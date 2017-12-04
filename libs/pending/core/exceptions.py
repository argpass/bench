#!coding: utf-8


class ServiceError(Exception):
    def __init__(self, error_code, log_message=None):
        self.error_code = error_code
        self.log_message = log_message

    def __str__(self):
        return u"[SE]%s, %s" % (self.error_code, self.log_message)


class APIError(Exception):
    def __init__(self, error_code, message=None, log_message=None):
        self.error_code = error_code
        self.message = message
        self.log_message = log_message

    def __str__(self):
        return u"[%s] %s" % (self.error_code, self.message)


class ImproperlyConfigured(Exception):
    """Improperly configured"""
    pass

