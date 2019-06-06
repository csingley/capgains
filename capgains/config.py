"""
"""
import os
import configparser


CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".config", "capgains")
CONFIG_PATH = os.path.join(CONFIG_DIR, "capgains.cfg")


class CapgainsConfig(configparser.SafeConfigParser):
    def make_default(self):
        self["db"] = {
            "dialect": "postgresql",
            "driver": "psycopg2",
            "username": "",
            "password": "T0PS3CR3T",
            "host": "localhost",
            "port": "5432",
            "database": "capgains",
        }
        self["test"] = {"dialect": "sqlite"}
        self["data"] = {"default_dir": ""}
        self["work"] = {"default_dir": ""}
        self["books"] = {"functional_currency": "USD"}

    @property
    def db_uri(self):
        return self._make_db_uri(**self["db"])

    @property
    def test_db_uri(self):
        return self._make_db_uri(**self["test"])

    def _make_db_uri(self, **kwargs):
        schema = "{dialect}"
        if kwargs.get("driver", None):
            schema += "+{driver}"

        credentials = ""
        if kwargs.get("username", None):
            credentials = "{username}"
            if kwargs.get("password", None):
                credentials += ":{password}"

        authority = ""
        if kwargs.get("host", None):
            authority = "@{host}"
            if kwargs.get("port", None):
                authority += ":{port}"

        db = ""
        if kwargs.get("database", None):
            db = "/{database}"

        template = "{schema}://{credentials}{authority}{db}".format(
            schema=schema, credentials=credentials, authority=authority, db=db
        )
        return template.format(**kwargs)


CONFIG = CapgainsConfig()


# If no config exists, generate & write defaults
if os.path.exists(CONFIG_PATH):
    CONFIG.read(CONFIG_PATH)
else:
    CONFIG.make_default()
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(CONFIG_PATH, "w") as configfile:
        CONFIG.write(configfile)
