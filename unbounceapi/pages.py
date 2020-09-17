# unbounceapi/pages.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Page
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     07/23/2019   Initial Release
#   1.1     08/23/2019   Including Docstrings for Constructor and Methods.
#
# File Description
# ------------------------------------------------------------------------------------
# Contains API routes for querying and manipulating Pages.
# https://developer.unbounce.com/api_reference/#id_pages
#
# Class Methods
# ------------------------------------------------------------------------------------
#    Name                                     Description
# ----------                          ------------------------------------------------
# __init__()                          Constructor
#
# get_pages()                         Returns one or more Unbounce Pages.
#
# get_form_fields()                   Returns form fields for a given Unbounce Page.
#
# get_page_leads()                    Returns Leads for a given Unboune Page.
#
# create_page_lead()                  Creates a Lead under a given Unbounce Page.
#
# delete_page_lead()                  Deletes a Lead under a given Unbounce Page.
#
# lead_deletion_request()             Creates a request to delete one or more Leads
#                                     for a given Unbounce Page.
#
# get_lead_deletion_request_status()  Returns the status of a given deletion request.
#*************************************************************************************
# Imported Packages:
import requests

class Page(object):
    """A sub-class to Unbounce that contains routes for Page Objects.

    Arguments
    ---------
    1. client {class} -- The parent class; Unbounce.
    
    Raises
    ------
    None

    Returns
    -------
    Class -- Instance of Page.
    """

    # Initializing static variable for Unbounce Page URL base.
    PAGE_URL_BASE = 'https://api.unbounce.com/pages'

    #**************************************************************************************
    # Constructor: __init__(self, library)
    #
    # Description
    # ------------------------------------------------------------------------------------
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
    # Method: get_pages(self, string, **kwargs)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to retrieve a list of all Pages. User may explicitly
    # specify page_id in order to retrieve details of a single Page.
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
    #
    # **kwargs (string)     sort_order    Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    #
    # **kwargs (boolean)    count         When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    #
    # **kwargs (string)     _from         Limit results to those created after _from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    #
    # **kwargs (string)     to            Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    #
    # **kwargs (integer)    offset        Omit the first offset number of results (ex: 3).
    #
    # **kwargs (integer)    limit         Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #
    # **kwargs (boolean)    with_stats    When true, include Page stats for the collection
    #                                     (ex: 'True').
    #
    # **kwargs (string)     role          Restricts the scope of the returned Pages
    #                                     ('author' or 'viewer').
    #*************************************************************************************
    def get_pages(self, page_id=None, **kwargs):
        """Allows users to retrieve Unbounce Page collections.

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
        7. with_stats -- When true, include Page stats for the collection.
        8. role -- Only return limit number of results.
        
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
        if page_id == None:
            url = self.PAGE_URL_BASE
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url, params=params)
        else:
            url = self.PAGE_URL_BASE + '/{0}'.format(page_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url)

    #*************************************************************************************
    # Method: get_form_fields(self, string, **kwargs)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to retrieve a full list of all form fields across all Page
    # variants of a specific Page.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type                 Name                       Description
    # --------------------  -----------------  -------------------------------------------
    # string                page_id            The ID for a given Unbounce Page.
    #
    # **kwargs (string)     sort_order         Sort by creation date ('asc' or 'desc').
    #                                          Default: 'asc'
    #
    # **kwargs (boolean)    count              When true, don't return the response's
    #                                          collection attribute (ex: 'True').
    #
    # **kwargs (boolean)    include_sub_pages  When true, include Sub Page form fields
    #                                          in the response (ex: 'True').
    #*************************************************************************************
    def get_form_fields(self, page_id, **kwargs):
        """Allows users to retrieve a full list of all form fields across all
        Page variants of a specific Page.

        Arguments
        ---------
        1. page_id {string} -- The ID for a given Unbounce Page.

        Keyword Arguments
        -----------------
        1. sort_order -- Sort by creation date ('asc' or 'desc').
        2. count -- When true, don't return the response's collection attribute.
        3. include_sub_pages -- When true, include Sub Page form fields in the response.
        
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
            params = kwargs
        url = self.PAGE_URL_BASE + '/{0}/form_fields'.format(page_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)

    #*************************************************************************************
    # Method: get_page_leads(self, string, string, **kwargs)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to retrieve a list of all Leads for a given Page. Users may
    # explicitly specify page_id AND lead_id in order to retrieve details of a single Lead.
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
    #
    # string                lead_id       The ID for a given Unbounce Lead.
    #                                     Default: None
    #
    # **kwargs (string)     sort_order    Sort by creation date ('asc' or 'desc').
    #                                     Default: 'asc'
    #
    # **kwargs (boolean)    count         When true, don't return the response's collection
    #                                     attribute (ex: 'True').
    #
    # **kwargs (string)     _from         Limit results to those created after _from
    #                                     (ex: '2014-12-31T00:00:00.000Z').
    #
    # **kwargs (string)     to            Limit results to those created before to
    #                                     (ex: '2014-12-31T23:59:59.999Z').
    #
    # **kwargs (integer)    offset        Omit the first offset number of results (ex: 3).
    #
    # **kwargs (integer)    limit         Only return limit number of results (ex: 100).
    #                                     Default: 50
    #                                     Maximum: 1000
    #*************************************************************************************
    def get_page_leads(self, page_id, lead_id=None, **kwargs):
        """Allows users to retrieve a list of all Leads for a given Page.

        Arguments
        ---------
        1. page_id {string} -- The ID for a given Unbounce Page.
        2. lead_id {string} -- The ID for a given Unbounce Lead.

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
        if lead_id == None:
            url = self.PAGE_URL_BASE + '/{0}/leads'.format(page_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url, params=params)
        else:
            url = self.PAGE_URL_BASE + '/{0}/leads/{1}'.format(page_id, lead_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url)

    #*************************************************************************************
    # Method: create_page_lead(self, string, **kwargs)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to create a new Lead. New Leads created via the API are
    # marked distinctly from those created through the webapp. Leads created via the API
    # will have a new attribute in their extra_data field: {'created_by': 'api'}
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class post() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                page_id       The ID for a given Unbounce Page.
    #*************************************************************************************
    def create_page_lead(self, page_id):
        """Allows users to create a new Lead.

        Arguments
        ---------
        1. page_id {string} -- The ID for a given Unbounce Page.
        
        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        url = self.PAGE_URL_BASE + '/{0}/leads/'.format(page_id)
        # Return the result of the client (Parent) class post() method, pass an appropriate URL.
        return self.client.post(url)

    #*************************************************************************************
    # Method: delete_page_lead(self, string, **kwargs)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to delete a single Lead. Only available to the Account
    # owner.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class delete() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                page_id       The ID for a given Unbounce Page.
    #
    # string                lead_id       The ID for a given Unbounce Lead.
    #*************************************************************************************
    def delete_page_lead(self, page_id, lead_id):
        """Allows users to delete a single Lead.

        Arguments
        ---------
        1. page_id {string} -- The ID for a given Unbounce Page.
        2. lead_id {string} -- The ID for a given Unbounce Lead.
        
        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        url = self.PAGE_URL_BASE + '/{0}/leads/{1}'.format(page_id, lead_id)
        # Return the result of the client (Parent) class delete() method, pass an appropriate URL.
        return self.client.delete(url)

    #*************************************************************************************
    # Method: post_lead_deletion_request(self, string, **kwargs)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to create a request to asynchronously delete one or more
    # Leads for a given Page. To check the status of the request, perform a GET request
    # using the response body's metadata.location. This endpoint is only available to the
    # Account owner. Deleted Leads cannot be recovered.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class post() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                page_id       The ID for a given Unbounce Page.
    #*************************************************************************************
    def post_lead_deletion_request(self, page_id):
        """Allows users to create a request to asynchronously delete one or
        more Leads for a given Page.

        Arguments
        ---------
        1. page_id {string} -- The ID for a given Unbounce Page.
        
        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        url = self.PAGE_URL_BASE + '/{0}/lead_deletion_request'.format(page_id)
        # Return the result of the client (Parent) class post() method, pass an appropriate URL.
        return self.client.post(url)

    #*************************************************************************************
    # Method: post_lead_deletion_request(self, string, **kwargs)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to retrieve the status of a leads_deletion_request. The
    # status of the request can be found in the response body under the key 'status'.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #   Type               Name                         Description
    # ---------  -------------------------  ----------------------------------------------
    # string     page_id                    The ID for a given Unbounce Page.
    #
    # string     lead_deletion_request_id   The ID for a given Lead Deletion Request.
    #*************************************************************************************
    def get_lead_deletion_request_status(self, page_id, lead_deletion_request_id):
        """Allows users to retrieve the status of a Lead Deletion Request.

        Arguments
        ---------
        1. page_id {string} -- The ID for a given Unbounce Page.
        2. lead_deletion_request_id {string} -- The ID for a given Lead Deletion Request.
        
        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        url = self.PAGE_URL_BASE + '/{0}/lead_deletion_request/{1}'.format(page_id, lead_deletion_request_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url)
