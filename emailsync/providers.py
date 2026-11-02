"""Built-in provider connection presets."""

GOOGLE_IMAP = {"host": "imap.gmail.com", "port": 993, "security": "ssl"}
PROVIDER_PRESETS = {
    "google": {
        "label": "Google",
        "issuer_url": "https://accounts.google.com",
        "scopes": (
            "openid email https://mail.google.com/ "
            "https://www.googleapis.com/auth/carddav https://www.googleapis.com/auth/calendar"
        ),
        "auth_params": {"access_type": "offline", "prompt": "consent"},
        "endpoints": {
            "gmail": GOOGLE_IMAP,
            "imap": GOOGLE_IMAP,
            "carddav": {"url": "https://www.googleapis.com/.well-known/carddav"},
            "caldav": {"url": "https://apidata.googleusercontent.com/caldav/v2/"},
        },
    }
}
GOOGLE_CLIENT_REQUIRED = (
    "Google sign-in needs your own OAuth client ID and secret from Google Cloud; see the README."
)
