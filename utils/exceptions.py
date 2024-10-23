class LDAPError(Exception):
    """Raised when there is an issue connecting to the LDAP server."""


class SpreadSheetParseError(Exception):
    """Raised when there is an issue parsing the spreadsheet."""


class YamlParseError(Exception):
    """Raised when there is an issue parsing the yaml file."""
