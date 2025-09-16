# utils/validators.py
import re

URL_WEBSITE_PATTERN = re.compile(
    r"^https?://"
    r"[A-Za-z0-9@:%._\+~#=-]{1,256}\.[A-Za-z]{2,63}\b"
    r"[-A-Za-z0-9@:%_\+.~#?&//=]*$"
)

def is_valid_website_url(url: str) -> bool:
    """Return True if the given URL is a valid website URL, else False."""
    return bool(URL_WEBSITE_PATTERN.match(url))
