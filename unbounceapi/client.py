# unbounceapi/client.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Unbounce
# Super Class: None
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     7/23/2019   Initial Release
#
# File Description
# ----------------
# Contains API routes for querying and manipulating Unbounce objects.
# https://developer.unbounce.com/api_reference/
#
# Class Methods
# -------------
#       Name                                         Description
# ------------------                  ------------------------------------------------
# __init__()                          Constructor
# post()                              Submits data to be processed to the Unbounce
#                                     server.
# get()                               Requests data from the Unbounce server.
# get_global()                        Requests global API meta-information.
# delete()                            Deletes data from the Unbounce server.
# _parsed_response()                  Parses Response objects and returns the
#                                     appropriate JSON/message.
#*************************************************************************************
# Imported Packages/Variables:
import requests
from unbounceapi.accounts import Account
from unbounceapi.sub_accounts import Sub_Account
from unbounceapi.domains import Domain
from unbounceapi.pages import Page
from unbounceapi.page_groups import Page_Group
from unbounceapi.leads import Lead
from unbounceapi.users import User
from . import __version__

# Initializing HTTP response code variables and messages.
OK = 200
BAD_REQUEST = 400
UNAUTHORIZED_REQUEST = 401
FORBIDDEN_REQUEST = 403
NOT_FOUND = 404
VERSION_CONFLICT = 409
TOO_MANY_REQUESTS = 429
SERVER_ERROR = 500
UNBOUNCE_OK_MESSAGE = 'Ok: Successful request.'
UNBOUNCE_BAD_REQUEST_MESSAGE = 'Bad Request: The request could not be understood, possible syntax malformation.'
UNBOUNCE_UNAUTHORIZED_REQUEST_MESSAGE = 'Unauthorized Request: The request requires user authentication. API Key or Access Token is missing.'
UNBOUNCE_FORBIDDEN_REQUEST_MESSAGE = 'Forbidden Access: The API Key is forbidden to access the resource, or the Access Token is bad or has expired.'
UNBOUNCE_NOT_FOUND_MESSAGE = 'Not Found: The server has not found anything matching the request-uri.'
UNBOUNCE_VERSION_CONFLICT_MESSAGE = 'Version Conflict: The request could not be completed due to a conflict with the current state of the resource.'
UNBOUNCE_TOO_MANY_REQUESTS_MESSAGE = 'Too Many Requests: Too many requests in a given amount of time.'
UNBOUNCE_SERVER_ERROR_MESSAGE = 'Server Error: Something went wrong on Unbounce\'s end.'


class Unbounce(object):
    # Initializing static variables providing version and content information.
    USER_AGENT = 'Unbounce Python {0}'.format(__version__)
    CONTENT_TYPE = 'application/json'

    #**************************************************************************************
    # Constructor: __init__(self, library)
    #
    # Description
    # -----------
    # This constructor takes an API KEY and instantiates all sub-classes representing all
    # appropriate Unbounce objects.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                api_key       Unbounce API key.
    #*************************************************************************************
    def __init__(self, api_key, timeout_limit=600):
        self._api_key = api_key
        self.accounts = Account(self)
        self.sub_accounts = Sub_Account(self)
        self.domains = Domain(self)
        self.pages = Page(self)
        self.page_groups = Page_Group(self)
        self.leads = Lead(self)
        self.users = User(self)
        self.timeout = timeout_limit

        # Testing the Unbounce connection to ensure the correct API key has been passed.
        r = requests.get('https://api.unbounce.com/accounts', auth=(self._api_key, ''))
        self.__parsed_response(r)

    #*************************************************************************************
    # Method: post(self, string)
    #
    # Description
    # -----------
    # This method is accessed by all child classes and, enables the ability to submit data
    # to be processed to the Unbounce server.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the response from the private method __parsed_response().
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                url           the Unbounce URL to communicate with.
    #*************************************************************************************
    def post(self, url):
        r = requests.post(url, auth=(self._api_key, ''))
        return self.__parsed_response(r)

    #*************************************************************************************
    # Method: get(self, string)
    #
    # Description
    # -----------
    # This method is accessed by all child classes and, enables the ability to request
    # data from the Unbounce server. The request timeout limit is 600 seconds (10 mins).
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the response from the private method __parsed_response().
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                url           the Unbounce URL to communicate with.
    # **kwargs              CONDITIONAL   Keyword arguments accepted by Unbounce's server.
    #*************************************************************************************
    def get(self, url, **kwargs):
        r = requests.get(url, auth=(self._api_key, ''), timeout=self.timeout, **kwargs)

        return self.__parsed_response(r)

    #*************************************************************************************
    # Method: get_global(self)
    #
    # Description
    # -----------
    # This method allows users to retrieve the global API meta-information.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the global API meta-information as a JSON object.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    #*************************************************************************************
    def get_global(self):
        url = 'https://api.unbounce.com/'
        r = requests.get(url, auth=(self._api_key, ''))
        return r.json()

    #*************************************************************************************
    # Method: delete(self, string)
    #
    # Description
    # -----------
    # This method is accessed by all child classes and, enables the ability to delete
    # data from the Unbounce server.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the response from the private method __parsed_response().
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                url           the Unbounce URL to communicate with.
    #*************************************************************************************
    def delete(self, url):
        r = requests.delete(url, auth=(self._api_key, ''))
        return self.__parsed_response(r)

    #*************************************************************************************
    # Method: __parsed_response(self, Response)
    #
    # Description
    # -----------
    # This method takes a Response object and, returns the response body as a JSON object.
    # Depending on the Response status code, the appropriate message is returned or not
    # returned. If the Response object has a Timeout exception associated with it, an
    # appropriate error message is returned.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the Response body as a JSON object or, a message depending on the
    #         Response status code or potential exception associated with the Response
    #         object.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # Response              response      the Response object received from the Unbounce
    #                                     server.
    #*************************************************************************************
    def __parsed_response(self, response):
        # If the Response status code is 200, return Response.json().
        if response.status_code == OK:
            return response.json()

        # Else, handle specific errors...
        elif response.status_code == BAD_REQUEST:
            raise requests.HTTPError('{0} '.format(response.status_code) + UNBOUNCE_BAD_REQUEST_MESSAGE)
        elif response.status_code == UNAUTHORIZED_REQUEST:
            raise requests.ConnectionError('{0} '.format(response.status_code) + UNBOUNCE_UNAUTHORIZED_REQUEST_MESSAGE)
        elif response.status_code == FORBIDDEN_REQUEST:
            raise requests.ConnectionError('{0} '.format(response.status_code) + UNBOUNCE_FORBIDDEN_REQUEST_MESSAGE)
        elif response.status_code == NOT_FOUND:
            raise requests.HTTPError('{0} '.format(response.status_code) + UNBOUNCE_NOT_FOUND_MESSAGE)
        elif response.status_code == VERSION_CONFLICT:
            raise requests.HTTPError('{0} '.format(response.status_code) + UNBOUNCE_VERSION_CONFLICT_MESSAGE)
        elif response.status_code == TOO_MANY_REQUESTS:
            raise requests.HTTPError('{0} '.format(response.status_code) + UNBOUNCE_TOO_MANY_REQUESTS_MESSAGE)
        elif response.status_code == SERVER_ERROR:
            raise requests.HTTPError('{0} '.format(response.status_code) + UNBOUNCE_SERVER_ERROR_MESSAGE)
        else:
            raise requests.HTTPError('{0} '.format(response.status_code) + 'Unknown Error...')
