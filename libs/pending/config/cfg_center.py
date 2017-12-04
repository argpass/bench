#!coding: utf-8
import logging
import os
import json
from jsonschema import Draft4Validator
from pending.core.exceptions import ImproperlyConfigured
from pending.config import settings


def generate_mysql_config(tlp_file, schema_file, force=False):
    """
    Args:
        tlp_file(unicode): config template to be written in to config center db
        schema_file(unicode): scheme file
    """

    if not os.path.exists(tlp_file):
        raise ImproperlyConfigured(
            u"invalid config template file `%s`" % tlp_file)
    if not os.path.exists(schema_file):
        raise ImproperlyConfigured(
            u"invalid config scheme file path `%s`" % schema_file)
    with open(tlp_file) as fi:
        config = json.load(fi)
    with open(schema_file) as fi:
        schema = json.load(fi)
        Draft4Validator(schema).validate(config)
    config_center = settings.CONFIG_CENTER
    env = settings.ENV
    logging.warn("init config center with env `%s`" % env)
    from torndb import Connection
    db = None
    try:
        db_name = config_center["database"]
        db = Connection(host=config_center["host"],
                        database=db_name,
                        user=config_center["user"],
                        password=config_center["password"])
        table_name = "t_s_config_%s" % env
        if force:
            db.execute("DROP TABLE if EXISTS %s" % table_name)
        if db.query("show tables where Tables_in_%s = '%s'" % (
                db_name, table_name)):
            raise RuntimeError(
                u"config center of env `%s` has already been initialized !")
        create_table_sql = u"""
            CREATE TABLE IF NOT EXISTS %s (
              `id` int(20) unsigned zerofill NOT NULL AUTO_INCREMENT,
              `node` varchar(40) COLLATE utf8_bin NOT NULL COMMENT '模块节点名',
              `key` varchar(40) COLLATE utf8_bin NOT NULL COMMENT '模块下键名',
              `value` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '模块键值',
              `key_type` varchar(6) COLLATE utf8_bin NOT NULL DEFAULT 'string' COMMENT '配置键对应的类型',
              `key_note` varchar(45) COLLATE utf8_bin DEFAULT NULL COMMENT '键名备注说明',
              PRIMARY KEY (`id`),
              UNIQUE KEY `unique_config` (`node`,`key`) USING BTREE COMMENT '配置唯一索引'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='%s环境 系统运行配置表'"""\
                           % (table_name, env)
        db.execute(create_table_sql)
        sql = "INSERT INTO %s" % table_name
        sql += "(`node`, `key`, `value`, `key_type`) VALUES (%s, %s, %s, %s)"
        datas = []
        for key, value in config.items():
            for k, v in value.items():
                key_type = "string"
                if isinstance(v, list):
                    v = ", ".join(v)
                elif isinstance(v, bool):
                    key_type = "bool"
                elif isinstance(v, int):
                    key_type = "int"

                data = (key, k, v, key_type)
                datas.append(data)
        db.executemany_rowcount(sql, datas)
    except KeyError as e:
        raise ImproperlyConfigured(
            u"need config key `%s` of config_center" % (e.args[0]))
    finally:
        if db:
            db.close()

