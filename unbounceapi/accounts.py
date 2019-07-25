# unbounceapi/accounts.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Account
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     7/23/2019   Initial Release
#
# File Description
# ----------------
# Contains API routes for querying and manipulating Accounts.
# https://developer.unbounce.com/api_reference/#id_accounts
#
# Class Methods
# -------------
#    Name                                     Description
# ----------                  --------------------------------------------------------
# __init__()                  Constructor
# get_accounts()              Returns Unbounce accounts.
# get_sub_accounts()          Returns Unbounce sub accounts.
# get_account_pages()         Returns Unbounce account pages.
#*************************************************************************************
# Imported Packages:
import requests

class Account(object):
    # Initializing static variable for Unbounce Account URL base.
    ACCOUNT_URL_BASE = 'https://api.unbounce.com/accounts'

    #**************************************************************************************
    # Constructor: __init__(self, library)
    #
    # Description
    # -----------
    # This constructor takes the client class as a parameter in order to gain access to
    # it's variables and methods.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # Class                 client        The parent class that houses all of the primary
    #                                     modules (variables and methods) to be accessed
    #                                     by child classes.
    #*************************************************************************************
    def __init__(self, client):
        # Instantiating client (Parent) class in order to gain access to it's methods/variables.
        self.client = client

    #**************************************************************************************
    # Method: get_accounts(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve the accounts collection. Users may
    # explicitly specify account_id in order to retrieve details of a single
    # account.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                account_id    The ID for a given Unbounce account.
    #                                     Default: None
    # **kwargs (string)     sort_order    Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    #*************************************************************************************
    def get_accounts(self, account_id=None, **kwargs):
        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            params = kwargs
        if account_id == None:
            url = self.ACCOUNT_URL_BASE
            # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url, params=params)
        else:
            url = self.ACCOUNT_URL_BASE + '/{0}'.format(account_id)
            # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url)

    #*************************************************************************************
    # Method: get_sub_accounts(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve all sub-accounts for a given account.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                account_id    The ID for a given Unbounce account.
    # **kwargs (string)     sort_order    Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)    count         When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)     from          Limit results to those created after from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)     to            Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)    offset        Omit the first offset number of results (ex: 3).
    # **kwargs (integer)    limit         Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #*************************************************************************************
    def get_sub_accounts(self, account_id, **kwargs):
        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            params = kwargs
        url = self.ACCOUNT_URL_BASE + '/{0}'.format(account_id) + '/sub_accounts'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)

    #*************************************************************************************
    # Method: get_account_pages(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve a list of all pages for the specified account.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                account_id    The ID for a given Unbounce account.
    # **kwargs (string)     sort_order    Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)    count         When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)     from          Limit results to those created after from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)     to            Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)    offset        Omit the first offset number of results (ex: 3).
    # **kwargs (integer)    limit         Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #*************************************************************************************
    def get_account_pages(self, account_id, **kwargs):
        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            params = kwargs
        url = self.ACCOUNT_URL_BASE + '/{0}'.format(account_id) + '/pages'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)
