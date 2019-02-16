"""
"""
import os
import configparser


CONFIG_DIR = os.path.join(os.path.expanduser('~'), '.config', 'capgains')
CONFIG_PATH = os.path.join(CONFIG_DIR, 'capgains.cfg')


class CapgainsConfig(configparser.SafeConfigParser):
    def make_default(self):
        self['db'] = {'dialect': 'postgresql', 'driver': 'psycopg2',
                      'username': '', 'password': 'T0PS3CR3T',
                      'host': 'localhost', 'port': '5432',
                      'database': 'capgains'}
        self['data'] = {'default_dir': ''}
        self['work'] = {'default_dir': ''}
        self['books'] = {'functional_currency': 'USD'}

    @property
    def db_uri(self):
        db = self['db']
        template = '{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}'
        values = {'dialect': db['dialect'],
                  'driver': db['driver'],
                  'username': db.get('username', ''),
                  'password': db.get('password', ''),
                  'host': db.get('host', ''),
                  'port': db['port'],
                  'database': db.get('database', '')}
        return template.format(**values)


CONFIG = CapgainsConfig()


# If no config exists, generate & write defaults
if os.path.exists(CONFIG_PATH):
    CONFIG.read(CONFIG_PATH)
else:
    CONFIG.make_default()
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(CONFIG_PATH, 'w') as configfile:
        CONFIG.write(configfile)
