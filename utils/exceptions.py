class LDAPError(Exception):
    """Raised when there is an issue connecting to the LDAP server."""


class SpreadSheetParseError(Exception):
    """Raised when there is an issue parsing the spreadsheet."""


class YamlParseError(Exception):
    """Raised when there is an issue parsing the yaml file."""

class ConfigurationError(Exception):
    """Raised when a configuration file is missing or inaccessible."""

class InvalidConfigurationError(Exception):
    """Raised when a configuration file is present but contains invalid content."""
