# unbounceapi/users.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: User
# Super Class: client (unbounceapi/client.py)
#
# Revision     Date                        Release Comment
# --------  ----------  --------------------------------------------------------------
#   1.0     7/23/2019   Initial Release
#
# File Description
# ----------------
# Contains API routes for querying current User.
# https://developer.unbounce.com/api_reference/#id_users
#
# Class Methods
# -------------
#    Name                                     Description
# ----------                  --------------------------------------------------------
# __init__()                  Constructor
# get_user()                  Returns current Unbounce user.
#*************************************************************************************
# Imported Packages:
import requests

class User(object):
    # Initializing static variable for Unbounce User URL base.
    USER_URL_BASE = 'https://api.unbounce.com/users'

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
	# Method: get_user(self, string)
	#
	# Description
	# -----------
	# This method allows users to retrieve details of a given user.
	#
	# RETurn
	#  Type                            Description
	# ------  ----------------------------------------------------------------------------
	# JSON    Returns the client (Parent) class get() method's response.
	#
	# ------------------------------- Arguments ------------------------------------------
	#        Type               Name                         Description
	# --------------------  ------------  ------------------------------------------------
	# string                user_id       The ID for a given Unbounce user.
    #                                     Default: None (results in self)
	#*************************************************************************************
    def get_user(self, user_id=None):
        if user_id == None:
            url = self.USER_URL_BASE + '/self'
            # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url)
        else:
            url = self.USER_URL_BASE + '/{0}'.format(user_id)
            # Return the result of the client (Parent) class get() method, pass an appropriate URL.
            return self.client.get(url)
