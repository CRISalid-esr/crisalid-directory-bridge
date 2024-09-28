class LDAPConnectionError(Exception):
    """Raised when there is an issue connecting to the LDAP server."""


class LDAPSizeLimitExceededError(Exception):
    """Raised when the LDAP response exceeds the size limit."""


class SpreadSheetParseError(Exception):
    """Raised when there is an issue parsing the spreadsheet."""
