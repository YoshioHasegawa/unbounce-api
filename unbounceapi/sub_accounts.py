# unbounceapi/sub_accounts.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Sub_Account
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     7/23/2019   Initial Release
#   1.1     8/23/2019   Including Docstrings for Constructor and Methods.
#
# File Description
# ----------------
# Contains API routes for querying Sub-Accounts.
# https://developer.unbounce.com/api_reference/#id_sub_accounts__sub_account_id_
#
# Class Methods
# -------------
#    Name                                     Description
# ----------                  --------------------------------------------------------
# __init__()                  Constructor
# get_sub_account()           Returns details of a single Unbounce Sub-Account.
# get_sub_account_domains     Returns a Sub-Account's Domains.
# get_sub_account_page_groups Returns a Sub-Account's Page Groups.
# get_sub_account_pages       Returns a Sub-Account's Pages.
#*************************************************************************************
# Imported Packages:
import requests

class Sub_Account(object):
    """A sub-class to Unbounce that contains routes for Sub-Account Objects.

    Arguments
    ---------
    1. client {class} -- The parent class; Unbounce.
    
    Raises
    ------
    None

    Returns
    -------
    Class -- Instance of Sub_Account.
    """

    # Initializing static variable for Unbounce Sub-Account URL base.
    SUB_ACCOUNT_URL_BASE = 'https://api.unbounce.com/sub_accounts'

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
    # Method: get_sub_account(self, string)
    #
    # Description
    # -----------
    # This method allows users to retrieve the details of a single Sub-Account.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type          Name                         Description
    # -----------  ---------------  ------------------------------------------------------
    # string       sub_account_id   The ID for a given Sub-Account.
    #*************************************************************************************
    def get_sub_account(self, sub_account_id):
        """Allows users to retrieve the details of a single Sub-Account.

        Arguments
        ---------
        1. sub_account_id {string} -- The ID for a given Sub-Account.

        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url)

    #*************************************************************************************
    # Method: get_sub_account_domains(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve a list of all custom Domains belonging to
    # a given Sub-Account.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # ------------------  --------------  ------------------------------------------------
    # string              sub_account_id  The ID for a given Sub-Account.
    # **kwargs (string)   sort_order      Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)  count           When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)   _from           Limit results to those created after _from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)   to              Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)  offset          Omit the first offset number of results (ex: 3).
    # **kwargs (integer)  limit           Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #*************************************************************************************
    def get_sub_account_domains(self, sub_account_id, **kwargs):
        """Allows users to retrieve a list of all custom Domains belonging to
        a given Sub-Account.

        Arguments
        ---------
        1. sub_account_id {string} -- The ID for a given Sub-Account.

        Keyword Arguments
        -----------------
        1. sort_order -- Sort by creation date ('asc' or 'desc').
        2. count -- When true, don't return the response's collection attribute.
        3. _from -- Limit results to those created after _from.
        4. to -- Limit results to those created before to.
        5. offset -- Omit the first offset number of results.
        6. limit -- Only return limit number of results.

        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            if '_from' in kwargs:
                kwargs['from'] = kwargs.pop('_from')
            params = kwargs
        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id) + '/domains'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)

    #*************************************************************************************
    # Method: get_sub_account_page_groups(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve a list of all Page Groups for a given
    # Sub-Account.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # ------------------  --------------  ------------------------------------------------
    # string              sub_account_id  The ID for a given Sub-Account.
    # **kwargs (string)   sort_order      Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)  count           When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)   _from           Limit results to those created after _from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)   to              Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)  offset          Omit the first offset number of results (ex: 3).
    # **kwargs (integer)  limit           Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #*************************************************************************************
    def get_sub_account_page_groups(self, sub_account_id, **kwargs):
        """Allows users to retrieve a list of all Page Groups for a given 
        Sub-Account.

        Arguments
        ---------
        1. sub_account_id {string} -- The ID for a given Sub-Account.

        Keyword Arguments
        -----------------
        1. sort_order -- Sort by creation date ('asc' or 'desc').
        2. count -- When true, don't return the response's collection attribute.
        3. _from -- Limit results to those created after _from.
        4. to -- Limit results to those created before to.
        5. offset -- Omit the first offset number of results.
        6. limit -- Only return limit number of results.

        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            if '_from' in kwargs:
                kwargs['from'] = kwargs.pop('_from')
            params = kwargs
        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id) + '/page_groups'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)

    #*************************************************************************************
    # Method: get_sub_account_pages(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve a list of all Pages for a given Sub-Account.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # ------------------  --------------  ------------------------------------------------
    # string              sub_account_id  The ID for a given Sub-Account.
    # **kwargs (string)   sort_order      Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)  count           When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)   _from           Limit results to those created after _from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)   to              Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)  offset          Omit the first offset number of results (ex: 3).
    # **kwargs (integer)  limit           Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #*************************************************************************************
    def get_sub_account_pages(self, sub_account_id, **kwargs):
        """Allows users to retrieve a list of all Pages for a given
        Sub-Account.

        Arguments
        ---------
        1. sub_account_id {string} -- The ID for a given Sub-Account.

        Keyword Arguments
        -----------------
        1. sort_order -- Sort by creation date ('asc' or 'desc').
        2. count -- When true, don't return the response's collection attribute.
        3. _from -- Limit results to those created after _from.
        4. to -- Limit results to those created before to.
        5. offset -- Omit the first offset number of results.
        6. limit -- Only return limit number of results.

        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            if '_from' in kwargs:
                kwargs['from'] = kwargs.pop('_from')
            params = kwargs
        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id) + '/pages'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)
