"""
refresh_garmin_token.py  --  regenerate the GARMIN_OAUTH_TOKEN secret value.

Run this LOCALLY (not in CI). Best from your home network (a residential IP is
what Garmin trusts most). It logs in once and prints a SINGLE token string that
contains BOTH the OAuth1 and OAuth2 tokens (the full garth session). Paste that
string into the GARMIN_OAUTH_TOKEN GitHub Actions secret.

Why the full session matters: garth's OAuth1 token is long-lived (~1 year) and
is what lets garth silently refresh the short-lived OAuth2 token. Saving only
the OAuth2 token (the old behaviour) is why the sync used to die every few days.
With the full session stored, you should only need to run this about once a year.

Setup:
    pip install "garth==0.8.0"   # pinned to match scripts/requirements.txt — the
                                 # token MUST be generated with the same garth
                                 # version CI uses
    python refresh_garmin_token.py

You'll be prompted for your Garmin email, password, and (if enabled) the MFA
code Garmin emails/texts you. Nothing is stored to disk by this script.
"""

import getpass
import garth


def main():
    email = input("Garmin email: ").strip()
    password = getpass.getpass("Garmin password: ")

    # garth handles MFA by prompting on stdin if the account requires it.
    garth.login(email, password)

    if garth.client.oauth1_token is None or garth.client.oauth2_token is None:
        raise SystemExit(
            "Login did not produce a full token pair. Try again later -- "
            "Garmin may be rate-limiting (HTTP 429); wait a few hours."
        )

    # dumps() serialises BOTH tokens (oauth1 + oauth2) into one base64 string.
    session = garth.client.dumps()

    bar = "=" * 70
    print()
    print(bar)
    print("Copy EVERYTHING on the single line below into the")
    print("GARMIN_OAUTH_TOKEN GitHub secret (Settings > Secrets and variables >")
    print("Actions > GARMIN_OAUTH_TOKEN > Update):")
    print(bar)
    print()
    print(session)
    print()
    print(bar)
    try:
        print("OAuth1 (long-lived) expires_at (unix):", garth.client.oauth1_token.expires_at)
        print("OAuth2 expires_at (unix):             ", garth.client.oauth2_token.expires_at)
    except Exception:
        pass
    print("Done. After updating the secret, re-run the Daily Health Sync workflow.")
    print("With the full session stored, this should last ~a year.")


if __name__ == "__main__":
    main()
