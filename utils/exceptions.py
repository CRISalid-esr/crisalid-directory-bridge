class LDAPConnectionError(Exception):
    """Raised when there is an issue connecting to the LDAP server."""
    pass


class LDAPSizeLimitExceededError(Exception):
    """Raised when the LDAP response exceeds the size limit."""
    pass
