# unbounceapi/sub_accounts.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Sub_Account
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     7/23/2019   Initial Release
#
# File Description
# ----------------
# Contains API routes for querying Sub Accounts.
# https://developer.unbounce.com/api_reference/#id_sub_accounts__sub_account_id_
#
# Class Methods
# -------------
#    Name                                     Description
# ----------                  --------------------------------------------------------
# __init__()                  Constructor
# get_sub_account()           Returns details of a single Unbounce sub account.
# get_sub_account_domains     Returns a sub-account's domains.
# get_sub_account_page_groups Returns a sub-account's page groups.
# get_sub_account_pages       Returns a sub-account's pages.
#*************************************************************************************
# Imported Packages:
import requests

class Sub_Account(object):
    # Initializing static variable for Unbounce Sub Account URL base.
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
	# This method allows users to retrieve the details of a single sub-account.
	#
	# RETurn
	#  Type                            Description
	# ------  ----------------------------------------------------------------------------
	# JSON    Returns the client (Parent) class get() method's response.
	#
	# ------------------------------- Arguments ------------------------------------------
	#     Type          Name                         Description
	# -----------  ---------------  ------------------------------------------------------
	# string       sub_account_id   The ID for a given sub-account.
	#*************************************************************************************
    def get_sub_account(self, sub_account_id):
        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url)

    #*************************************************************************************
	# Method: get_sub_account_domains(self, string, **kwargs)
	#
	# Description
	# -----------
	# This method allows users to retrieve a list of all custom domains belonging to
    # a given sub-account.
	#
	# RETurn
	#  Type                            Description
	# ------  ----------------------------------------------------------------------------
	# JSON    Returns the client (Parent) class get() method's response.
	#
	# ------------------------------- Arguments ------------------------------------------
	#        Type               Name                         Description
	# ------------------  --------------  ------------------------------------------------
	# string              sub_account_id  The ID for a given sub-account.
    # **kwargs (string)   sort_order      Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)  count           When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)   from            Limit results to those created after from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)   to              Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)  offset          Omit the first offset number of results (ex: 3).
    # **kwargs (integer)  limit           Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
	#*************************************************************************************
    def get_sub_account_domains(self, sub_account_id, **kwargs):
        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            params = kwargs
        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id) + '/domains'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)

    #*************************************************************************************
	# Method: get_sub_account_page_groups(self, string, **kwargs)
	#
	# Description
	# -----------
	# This method allows users to retrieve a list of all page groups for a given
    # sub-account.
	#
	# RETurn
	#  Type                            Description
	# ------  ----------------------------------------------------------------------------
	# JSON    Returns the client (Parent) class get() method's response.
	#
	# ------------------------------- Arguments ------------------------------------------
	#        Type               Name                         Description
	# ------------------  --------------  ------------------------------------------------
	# string              sub_account_id  The ID for a given sub-account.
    # **kwargs (string)   sort_order      Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)  count           When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)   from            Limit results to those created after from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)   to              Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)  offset          Omit the first offset number of results (ex: 3).
    # **kwargs (integer)  limit           Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
	#*************************************************************************************
    def get_sub_account_page_groups(self, sub_account_id, **kwargs):
        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            params = kwargs
        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id) + '/page_groups'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)

    #*************************************************************************************
	# Method: get_sub_account_pages(self, string, **kwargs)
	#
	# Description
	# -----------
	# This method allows users to retrieve a list of all pages for a given sub-account.
	#
	# RETurn
	#  Type                            Description
	# ------  ----------------------------------------------------------------------------
	# JSON    Returns the client (Parent) class get() method's response.
	#
	# ------------------------------- Arguments ------------------------------------------
	#        Type               Name                         Description
	# ------------------  --------------  ------------------------------------------------
	# string              sub_account_id  The ID for a given sub-account.
    # **kwargs (string)   sort_order      Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)  count           When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)   from            Limit results to those created after from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)   to              Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    # **kwargs (integer)  offset          Omit the first offset number of results (ex: 3).
    # **kwargs (integer)  limit           Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
	#*************************************************************************************
    def get_sub_account_pages(self, sub_account_id, **kwargs):
        # Initializing a dictionary for potential URL parameters.
        params = {}
        if kwargs:
            params = kwargs
        url = self.SUB_ACCOUNT_URL_BASE + '/{0}'.format(sub_account_id) + '/pages'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url)
