# unbounceapi/domains.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Domain
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     7/23/2019   Initial Release
#   1.1     8/23/2019   Including Docstrings for Constructor and Methods.
#
# File Description
# ----------------
# Contains API routes for querying Domains.
# https://developer.unbounce.com/api_reference/#id_domains__domain_id_
#
# Class Methods
# -------------
#    Name                                     Description
# ----------                  --------------------------------------------------------
# __init__()                  Constructor
# get_domain()                Returns a custom Domain registered with Unbounce.
# get_domain_pages()          Returns a Domain's Pages.
#*************************************************************************************
# Imported Packages:
import requests

class Domain(object):
    """A sub-class to Unbounce that contains routes for Domain Objects.

    Arguments
    ---------
    1. client {class} -- The parent class; Unbounce.
    
    Raises
    ------
    None

    Returns
    -------
    Class -- Instance of Domain.
    """

    # Initializing static variable for Unbounce Domain URL base.
    DOMAIN_URL_BASE = 'https://api.unbounce.com/domains'

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
    # Method: get_domain(self, string)
    #
    # Description
    # -----------
    # This method allows users to retrieve a custom Domain that has been registered
    # with Unbounce.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                domain_id     The ID for a given Domain.
    #*************************************************************************************
    def get_domain(self, domain_id):
        """Allows users to retrieve a custom Domain that has been registered 
        with Unbounce.

        Arguments
        ---------
        1. account_id {string} -- The ID for a given Unbounce Account.
        
        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        url = self.DOMAIN_URL_BASE + '/{0}'.format(domain_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url)

    #*************************************************************************************
    # Method: get_domain_pages(self, string, **kwargs)
    #
    # Description
    # -----------
    # This method allows users to retrieve a list of all Pages based on the Domain.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # ------------------  --------------  ------------------------------------------------
    # string              domain_id       The ID for a given Unbounce Domain.
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
    def get_domain_pages(self, domain_id, **kwargs):
        """Allows users to retrieve a list of all Pages based on the Domain.

        Arguments
        ---------
        1. domain_id {string} -- The ID for a given Unbounce Domain.

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
        url = self.DOMAIN_URL_BASE + '/{0}'.format(domain_id) + '/pages'
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url, params=params)
