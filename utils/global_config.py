"""
This file contains constants like configuration values to be used across the project across all the APIs
"""


class AuthConfig:
    """
    This class will contain the list of all the configuration related to google auth
    """
    google_auth_config = {
        "endpointURL": "https://accounts.google.com/o/oauth2/v2/auth?",
        "scope": "profile email",
        "accessType": "offline",
        "redirectURI": "{}/create-tokens",
        "responseType": "code",
        "grantType": "authorization_code",
        "tokenURL": "https://oauth2.googleapis.com/token",
        "emailURL": "https://www.googleapis.com/oauth2/v3/userinfo?",
        "businessURL": "https://mybusinessbusinessinformation.googleapis.com",
    }

    # TODO: Update when microsoft auth is completed
    microsoft_auth_config = {
        "endpointURL": "https://login.microsoftonline.com/common/oauth2/v2.0/authorize?",
        "scope": "offline_access openid email user.read",
        "accessType": "offline",
        "redirectURI": "{}/create-tokens",
        "responseType": "code",
        "grantType": "authorization_code",
        "tokenURL": "https://login.microsoftonline.com/common/oauth2/v2.0/token",
        "emailURL": "https://graph.microsoft.com/v1.0/me"
    }

    facebook_auth_config = {
        "constructEndpointURL": "https://www.facebook.com/v14.0/dialog/oauth?",
        "scope": "read_insights,pages_show_list,pages_messaging,page_events,pages_read_engagement,"
                 "pages_manage_metadata,pages_read_user_content,pages_manage_posts,pages_manage_engagement,"
                 "public_profile",
        "redirectURI": "https%3A%2F%2Fapi.restaverse.com%2Fget-long-fb-access-token%2F",
        "graphVersion": "v14.0",
        "endpointURL": "https://graph.facebook.com"
    }
