# unbounceapi/page_groups.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Page_Group
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     7/23/2019   Initial Release
#   1.1     8/23/2019   Including Docstrings for Constructor and Methods.
#
# File Description
# ----------------
# Contains API routes for querying Page Groups.
# https://developer.unbounce.com/api_reference/#id_page_groups__page_group_id__pages
#
# Class Methods
# -------------
#    Name                                     Description
# ----------                          ------------------------------------------------
# __init__()                          Constructor
# get_page_group_pages()              Returns Pages belonging to a given Unbounce Page
#                                     Group.
#*************************************************************************************
# Imported Packages:
import requests

class Page_Group(object):
    """A sub-class to Unbounce that contains routes for Page Group Objects.

    Arguments
    ---------
    1. client {class} -- The parent class; Unbounce.
    
    Raises
    ------
    None

    Returns
    -------
    Class -- Instance of Page_Group.
    """
    
    # Initializing static variable for Unbounce Page Group URL base.
    PAGE_GROUP_URL_BASE = 'https://api.unbounce.com/page_groups'

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

    #*************************************************************************************
    # Method: get_page_group_pages(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve a list of all Pages that belong to a given Page
    # Group.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                page_id       The ID for a given Unbounce Page.
    #                                     Default: None
    # **kwargs (string)     sort_order    Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    # **kwargs (boolean)    count         When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    # **kwargs (string)     _from         Limit results to those created after _from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    # **kwargs (string)     to            Limit results to those created before to
    # **kwargs (integer)    offset        Omit the first offset number of results (ex: 3).
    # **kwargs (integer)    limit         Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #*************************************************************************************
    def get_page_group_pages(self, page_group_id, **kwargs):
        """Allows users to retrieve a list of all Pages that belong to a 
        given Page Group.

        Arguments
        ---------
        1. page_id {string} -- The ID for a given Unbounce Page.

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
        url = self.PAGE_GROUP_URL_BASE + '/{0}'.format(page_group_id) + '/pages'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)
