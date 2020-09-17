# unbounceapi/leads.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: Lead
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     07/23/2019   Initial Release
#   1.1     08/23/2019   Including Docstrings for Constructor and Methods.
#
# File Description
# ------------------------------------------------------------------------------------
# Contains API routes for querying Leads.
# https://developer.unbounce.com/api_reference/#id_leads__lead_id_
#
# Class Methods
# ------------------------------------------------------------------------------------
#    Name                                     Description
# ----------                  --------------------------------------------------------
# __init__()                  Constructor
#
# get_lead()                  Returns details of a single Unbounce Lead.
#*************************************************************************************
# Imported Packages:
import requests

class Lead(object):
    """A sub-class to Unbounce that contains routes for Lead Objects.

    Arguments
    ---------
    1. client {class} -- The parent class; Unbounce.
    
    Raises
    ------
    None

    Returns
    -------
    Class -- Instance of Lead.
    """

    # Initializing static variable for Unbounce Lead URL base.
    LEAD_URL_BASE = 'https://api.unbounce.com/leads'

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

    #**************************************************************************************
    # Method: get_user(self, string)
    #
    # Description
    # ------------------------------------------------------------------------------------
    # This method allows users to retrieve a single Unbounce Lead.
    #
    # RETurn
    #  Type                            Description
    # ------  ----------------------------------------------------------------------------
    # JSON    Returns the client (Parent) class get() method's response.
    #
    # ------------------------------- Arguments ------------------------------------------
    #        Type               Name                         Description
    # --------------------  ------------  ------------------------------------------------
    # string                lead_id       The ID for a given Unbounce Lead.
    #*************************************************************************************
    def get_lead(self, lead_id):
        """Allows users to retrieve a single Unbounce Lead.

        Arguments
        ---------
        1. lead_id {string} -- The ID for a given Unbounce Lead.

        Raises
        ------
        None

        Returns
        -------
        JSON -- The Response object received from the Unbounce server.
        """

        url = self.LEAD_URL_BASE + '/{0}'.format(lead_id)
        # Return the result of the client (Parent) class get() method, pass an appropriate URL.
        return self.client.get(url)
