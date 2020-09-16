# bulk_data_extraction/unbounce_connection.py
#*************************************************************************************
# Programmer: Yoshio Hasegawa
# Class Name: UnbounceConnection
# Super Class: None
#
# Revision      Date                            Release Comment
# --------   ----------   ------------------------------------------------------------
#   1.0      8/9/2019     Initial Release
#
# File Description
# ----------------
# This Python class is a wrapper for the unbounceapi package and, contains methods for
# simple querying of bulk data from the Unbounce server. The only 2 extractable
# objects are Pages and Leads via the bulk_extract() method. The return value for
# both objects are lists of JSON objects. Please investigate each method for any
# further details.
#
# Class Methods
# -------------
#         Name                                      Description
# --------------------         -------------------------------------------------------
# __init__()                   The constructor used to instantiate this class.
# bulk_get_pages()             The method that interacts with the unbounceapi wrapper
#                              to retrive and return Page objects as lists of JSON
#                              objects.
# bulk_get_leads()             The method that interacts with the unbounceapi wrapper
#                              to retrive and return Lead objects as lists of JSON
#                              objects.
# process_date_range()         The method that checks and processes any date filters.
# process_bulk_pages()         The method that checks and processes all Page filters
#                              applied to the bulk_extract() method. This method also
#                              initiates the call to the bulk_get_pages() method.
# process_bulk_leads()         The method that checks and processes all Lead filters
#                              applied to the bulk_extract() method. This method also
#                              initiates the call to the bulk_get_leads() method.
# bulk_extract()               The method used to initiate any bulk extract for
#                              Page and/or Lead objects from the Unbounce server.
#*************************************************************************************
# Imported Packages:
#    Name                                      Description
# -----------         ----------------------------------------------------------------
# Unbounce            An API wrapper for the Unbounce server. Installation of this
#                     library is required. Enter the following command from the
#                     command line: 'pip install unbounce-python-api'
# pandas              A package containing data structures and data analysis tools.
# datetime            A package imported for manipulating date data type variables.
# time                A package used for stalling methods that have the potential for
#                     reaching Unbounce API limitations.
#*************************************************************************************
from unbounceapi.client import Unbounce
import pandas as pd
from datetime import datetime, timedelta
import time


class UnbounceConnection():

    #**************************************************************************************
    # Constructor: __init__(self)
    #
    # Description
    # -----------
    # This constructor instantiates the Unbounce API wrapper. This wrapper contains API
    # routes for easy retrieval and manipulation of data within the Unbounce server.
    #
    # For further detail please explore the following webpages:
    # Unbounce API guide: https://developer.unbounce.com/getting_started/
    # Unbounce API wrapper (PyPi): https://pypi.org/project/unbounce-python-api/
    # Unbounce API wrapper (GitHub): https://github.com/YoshioHasegawa/unbounce-python-api
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type             Name                            Description
    # -------------   ---------------------   --------------------------------------------
    # string          api_key                 The API key to your Ubounce account.
    # int             get_timeout             The timeout limit for the get method in the
    #                                         underlying API wrapper (unbounceapi.client).
    # int             bulk_extract_timeout    The runtime limit for this class' bulk get
    #                                         methods.
    #*************************************************************************************
    def __init__(self, api_key, get_timeout=600, bulk_extract_timeout=3600):

        # Initialize the timeout limit for the underlying API wrapper's get method.
        self.get_timeout_time = get_timeout

        # Initialize the bulk extract timeout limit. This time limit is in seconds and,
        # will timeout the bulk get methods (Pages/Leads) if the time limit has been
        # reached.
        self.extract_timeout_time = bulk_extract_timeout

        # Establish connection with Unbounce, via Unbounce API wrapper.
        # The timeout limit is the limit for a given get method call.
        self.client = Unbounce(api_key, timeout_limit=self.get_timeout_time)

    #*************************************************************************************
    # Method: bulk_get_pages(self, string=None, string=None, list=None, list=None,
    #                        string=None)
    #
    # Description
    # -----------
    # This method makes the calls to the Unbounce API wrapper in order to retrieve Page
    # objects from the Unbounce server. Specifically, this method makes an initial call
    # to retrieve Page objects. Then, continues to make further calls to retrive potential
    # Page objects that were not extracted initially due to a 1000 object/request limit.
    # All request calls are done within a while-loop using the last Page's 'createdAt'
    # data point from the previous call, to know where the previous call left off.
    # DataFrame comparisons are done to remove any duplicate rows. An iteration variable
    # is also used to record number of calls to handle a 500 requests/min limit. Finally,
    # filters passed to this method are handled within the get requests and, at the end of
    # the method with DataFrame slicing and indexing techniques.
    #
    # RETurn
    #  Type                                   Description
    # ----------   -----------------------------------------------------------------------
    # list         The final list consisting of Pages objects requested by the user.
    #              Page objects are formatted as JSON objects.
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type             Name                            Description
    # -------------   --------------   ---------------------------------------------------
    # string          date_start       To filter Page objects by Pages created later than
    #                                  or equal to a given date (inclusive).
    # string          date_end         To filter Page objects by Pages created earlier
    #                                  than a given date (exclusive).
    # list            domain_list      To filter Page objects by Page domain.
    # list            page_id_list     To filter Page objects by Page ID(s).
    # string          state            To filter Page objects by Page publishing status.
    #*************************************************************************************
    def bulk_get_pages(self, date_start=None, date_end=None, domain_list=None, page_id_list=None, state=None):

        # Initialize method starting time to set a max runtime limit.
        START_TIME = datetime.now()

        print('- Extracting Pages -')

        # Initialize empty DataFrame for Initial Page objects to be appended,
        # as well as additional Page objects requested.
        pages_df = pd.DataFrame()

        # Initialize boolean variable that enters/continues the while-loop below.
        cont = True

        # Initialize iteration variable to handle the 1000 object/request limitation.
        itr = 0

        # Initialize iteration variable to keep count of requests.
        call_counter = 1

        # Enter the while loop and run request calls for Page objects until all desired objects are extracted.
        # Due to the 1000 object/request limitation, we will run an initial request call,
        # re-initialize the starting date as the last Page objects created date (line 150) and,
        # continue to run the request call iteratively until we have extracted all desired objects.
        while cont:

            print('\nPage Request Call #: {0}'.format(call_counter))

            # If the current time less of the method start time is greater than the given extract runtime limit
            # (default is 1 hour), raise an error with an explanation. This error is included to limit the runtime
            # of this method, in case we encounter a Recursion Error.
            if (datetime.now() - START_TIME).seconds > self.extract_timeout_time:
                raise RecursionError('The max run-time limit of {0} seconds has been reached.'.format(self.extract_timeout_time))

            print('Page Extract Starting Date: {0}'.format(date_start))
            print('Page Extract Ending Date: {0}'.format(date_end))

            # Run request call for Page objects, with the given date range.
            pages_meta_data = self.client.pages.get_pages(_from=date_start, to=date_end, limit='1000', with_stats='true')
            # Initialize new Page specific data as DataFrame.
            new_pages_df = pd.DataFrame(pages_meta_data['pages'])

            # Iterate requests counter.
            call_counter += 1

            print(' > New Pages Extracted: {0}'.format(len(new_pages_df)))

            # If the Pages extracted is empty, no Pages were returned. Thus, break from this while-loop.
            # This will stop us from appending an empty list to the final DataFrame.
            if pages_meta_data['pages'] == []:
                print('\n>> No new Pages extracted. Exiting request loop...')
                break

            # Append the new Page objects to the Pages DataFrame.
            pages_df = pages_df.append(new_pages_df)

            # Initialize duplicate Page objects as DataFrame and drop duplicates from Pages DataFrame.
            pages_dropped = pages_df[pages_df.duplicated(subset=['id'], keep='first')]
            pages_df.drop_duplicates(subset=['id'], keep='first', inplace=True)

            print(' > Duplicated Pages Dropped: {0}'.format(len(pages_dropped)))
            print(' > Total Pages: {0}'.format(len(pages_df)))

            # Re-initialize the starting date variable as the Last Page's created date.
            # This tells us where we left off in case we hit the 1000 object/request limitation.
            # If needed, this while-loop will continue until we pull zero net new Page objects.
            # Note that the starting date is inclusive. Thus, we should expect at least one duplicated Page object.
            date_start = pages_meta_data['pages'][-1]['createdAt']

            # If the new Page objects pulled equals the Pages dropped,
            # we know that no net new Pages were pulled and all desired Pages were extracted.
            # Thus, exit the while-loop (cont = False).
            if new_pages_df.equals(pages_dropped):
                print('\n>> No net new Pages extracted. Exiting request loop...')
                cont = False

            # Increment the iteration variable to count requests made.
            # If this variable reached 495, the program will pause for 1 minute.
            # This is implemented to account for the 500 requests/minute limitation.
            itr += 1
            if itr == 495:
                time.sleep(60)
                itr = 0

        # Rename the Page ID column from 'id' to 'page_id'.
        # This will mitigate duplicate column name issues. 'page_id' is the unique key.
        pages_df.rename(columns={'id': 'page_id'}, inplace=True)

        print('\nApplying Page filters (Domain(s), Page ID(s) and State) if applicable...')

        # The following if statements will index and slice the Pages DataFrame according to passed filters.
        if domain_list:
            print(' > Applying Domain filter...')
            pages_df = pages_df[pages_df['domain'].isin(domain_list)]
        if page_id_list:
            print(' > Applying Page ID filter...')
            pages_df = pages_df[pages_df['page_id'].isin(page_id_list)]
        if state:
            print(' > Applying State filter...')
            pages_df = pages_df[pages_df['state'] == state]
        else:
            print(' > No additional filters applied...')

        print('\n~ Final Total Pages: {0} ~\n'.format(len(pages_df)))

        # Initialize a list with the final Pages DataFrame, via the to_dict() method.
        # This list will consist of Page object dictionaries.
        pages_list = pages_df.to_dict(orient='records')

        # Return final Page object dictionaries list.
        return pages_list

    #*************************************************************************************
    # Method: bulk_get_leads(self, string=None, string=None, list=None, list=None)
    #
    # Description
    # -----------
    # This method makes the calls to the Unbounce API wrapper in order to retrieve Lead
    # objects from the Unbounce server. Specifically, this method makes an initial call
    # to retrieve all Page object's IDs. This is required due to Lead objects only being
    # accessible via Page ID. Then, this method will iterate over all Page IDs to retrieve
    # Lead objects and, continues to make further calls to retrive potential Lead objects
    # that were not extracted initially due to a 1000 object/request limit. All request
    # calls are done within a while-loop using the last Leads's 'created_at' data point
    # from the previous call, to know where the previous call left off. DataFrame
    # comparisons are done to remove any duplicate rows. An iteration variable is also
    # used to record number of calls to handle a 500 requests/min limit. Finally, filters
    # passed to this method are handled within the get requests and, at the end of the
    # method with DataFrame slicing and indexing techniques.
    #
    # RETurn
    #  Type                                   Description
    # ----------   -----------------------------------------------------------------------
    # list         The final list consisting of Lead objects requested by the user.
    #              Lead objects are formatted as JSON objects.
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type             Name                            Description
    # -------------   --------------   ---------------------------------------------------
    # string          date_start       To filter Lead objects by Leads created later than
    #                                  or equal to a given date (inclusive).
    # string          date_end         To filter Lead objects by Leads created earlier
    #                                  than a given date (exclusive).
    # list            lead_id_list     To filter Lead objects by Lead ID(s).
    # list            page_id_list     To filter Lead objects by Page ID(s).
    #*************************************************************************************
    def bulk_get_leads(self, date_start=None, date_end=None, lead_id_list=None, page_id_list=None):

        # Initialize method starting time to set a max runtime limit.
        START_TIME = datetime.now()

        print('- Extracting Leads -\n')

        # Initialize the passed starting date as a static variable.
        OG_DATE_START = date_start

        # Initialize empty DataFrame for Initial Lead objects to be appended,
        # as well as additional Lead objects requested.
        leads_df = pd.DataFrame()

        # If Page ID list is passed to this method,
        # use this list to iterate over when requesting Lead objects.
        if page_id_list:
            print('Initializing Page ID(s) passed in the filters argument...\n')
            page_ids_all = page_id_list
        # Else, request and initialize all Page IDs.
        # Page ID is required for requesting Lead objects.
        else:
            print('Retrieving Page IDs for requesting Leads...\n')
            page_ids_all = pd.DataFrame(self.bulk_get_pages(date_end=date_end))['page_id']

        # If no Pages are returned, return empty list.
        # With no Pages to iterate over, no Leads exist.
        if page_ids_all.empty:
            return []

        print('{0} Page IDs Initialized, commencing Lead extraction with Page IDs...'.format(len(page_ids_all)))

        # Initialize iteration variable to handle the 1000 object/request limitation
        itr = 0
        # Initialize Page counter iteration variable to keep count of Page IDs.
        page_counter = 1

        # Use a for-loop to iterate over all Page IDs when requesting Lead objects,
        # to ensure we capture all Leads on all Pages.
        for page_id in page_ids_all:

            # Initialize boolean variable that continues the while-loop below.
            cont = True

            # Re-initialize the starting date as the original starting date passed to this method.
            # This will ensure we reset the starting date for each Page ID.
            date_start = OG_DATE_START

            print('\nCurrent Page ID: {0} (Page #: {1})'.format(page_id, page_counter))
            # Iterate Page counter.
            page_counter += 1

            # Initialize iteration variable to keep count of requests.
            call_counter = 1

            # Enter the while loop and run request calls for Lead objects until all desired objects are extracted.
            # Due to the 1000 object/request limitation, we will run an initial request call,
            # re-initialize the starting date as the last Lead objects created date (line 272) and,
            # continue to run the request call iteratively until we have extracted all desired objects.
            while cont:

                print('\nLead Request Call #: {0}'.format(call_counter))

                # If the current time less of the method start time is greater than the given extract runtime limit
                # (default is 1 hour), raise an error with an explanation. This error is included to limit the runtime
                # of this method, in case we encounter a Recursion Error.
                if (datetime.now() - START_TIME).seconds > self.extract_timeout_time:
                    raise RecursionError('The max run-time limit of {0} seconds has been reached.'.format(self.extract_timeout_time))

                print('Lead Extract Starting Date: {0}'.format(date_start))
                print('Lead Extract Ending Date: {0}'.format(date_end))

                leads_meta_data = self.client.pages.get_page_leads(page_id=page_id, _from=date_start, to=date_end, limit='1000')
                # Initialize new Lead specific data as DataFrame.
                new_leads_df = pd.DataFrame(leads_meta_data['leads'])

                # Iterate requests counter.
                call_counter += 1

                print(' > New Leads Extracted: {0}'.format(len(new_leads_df)))

                # If the Leads extracted is empty, no Leads were returned. Thus, break from this while-loop.
                if leads_meta_data['leads'] == []:
                    print('\n>> No new Leads extracted. Exiting request loop...')
                    break

                # Append the new Lead objects to the Leads DataFrame.
                leads_df = leads_df.append(new_leads_df)

                # Initialize duplicate Lead objects as DataFrame and drop duplicates from Leads DataFrame.
                leads_dropped = leads_df[leads_df.duplicated(subset=['created_at', 'id', 'page_id', 'variant_id'], keep='first')]
                leads_df.drop_duplicates(subset=['created_at', 'id', 'page_id', 'variant_id'], keep='first', inplace=True)

                print(' > Duplicated Leads Dropped: {0}'.format(len(leads_dropped)))
                print(' > Total Leads: {0}'.format(len(leads_df)))

                # Re-initialize the starting date variable as the Last Lead's created date.
                # This tells us where we left off in case we hit the 1000 object/request limitation.
                # If needed, this while-loop will continue until we pull zero net new Lead objects.
                # Note that the starting date is inclusive. Thus, we should expect at least one duplicated Page object.
                date_start = leads_meta_data['leads'][-1]['created_at']

                # If the new Lead objects pulled equals the Leads dropped,
                # we know that no net new Leads were pulled and all desired Leads were extracted.
                # Thus, exit the while-loop (cont = False).
                if new_leads_df.equals(leads_dropped):
                    print('\n>> No net new Leads extracted. Exiting request loop...')
                    cont = False

                # Increment the iteration variable to count requests made.
                # If this variable reached 495, the program will pause for 1 minute.
                # This is implemented to account for the 500 requests/minute limitation.
                itr += 1
                if itr == 495:
                    time.sleep(60)
                    itr = 0

        # Rename the Lead ID column from 'id' to 'lead_id'.
        # This will mitigate duplicate column name issues. 'lead_id' is the unique key.
        leads_df.rename(columns={'id': 'lead_id'}, inplace=True)

        print('\nApplying Lead filter (Lead ID(s)) if applicable...')

        # The following if statement will index and slice the Leads DataFrame according to the passed Lead ID filter.
        if lead_id_list:
            print(' > Applying Lead ID filter...')
            leads_df = leads_df[leads_df['lead_id'].isin(lead_id_list)]
        else:
            print(' > No additional filter applied...')

        print('\n~ Final Total Leads: {0} ~\n'.format(len(leads_df)))

        # Initialize a list with the final Leads DataFrame, via the to_dict() method.
        # This list will consist of Lead object dictionaries.
        leads_list = leads_df.to_dict(orient='records')

        # Return final Lead object dictionaries list.
        return leads_list

    #*************************************************************************************
    # Method: process_date_range(self, dictionary)
    #
    # Description
    # -----------
    # This method is called when the user applies any date filter to the bulk_extract()
    # method. The date range dictionary, which is the value for the 'created_at' key
    # found wihin the filters dictionary passed to the bulk_extract() method, is the
    # variable processed by this method. This date range dictionary can consist of both,
    # a starting date and ending date or, either a starting date or an ending date. The
    # processing done by this method includes checking for valid keys and values,
    # appropriate value formats, a valid date range and, editing date string values to
    # adhere to accepted API formats.
    #
    # RETurn
    #  Type                                   Description
    # ----------   -----------------------------------------------------------------------
    # string       The starting date used to filter the bulk extract request (inclusive).
    # string       The ending date used to filter the bulk extract request (exclusive).
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type             Name                            Description
    # -------------   --------------   ---------------------------------------------------
    # dictionary      date_range       Consists of a starting and ending date in the
    #                                  format of '%Y-%m-%d'.
    #*************************************************************************************
    def process_date_range(self, date_range):

        print('Date filter applied. Processing date filter...')

        # Initialize acceptable date range keys and format examples, as static variables.
        ACCEPTABLE_DATE_RANGE_KEYS = ['date_start', 'date_end']
        ACCEPTABLE_CREATED_AT_DICTIONARY = {'date_start': '%Y-%m-%d', 'date_end': '%Y-%m-%d'}
        DATE_FORMAT = '%Y-%m-%d'
        # Initialize starting date and ending date as None values,
        # in case one of both is not appplied.
        date_start = None
        date_end = None

        # If the date range argument is a dictionary, pass.
        if isinstance(date_range, dict):
            pass
        # Else, raise an error with an explanation and example.
        else:
            raise TypeError('Please input a dictionary with the correct format for the created_at filter type: {0}'.format(ACCEPTABLE_CREATED_AT_DICTIONARY))

        # Iterate over keys in the date range dictionary to check for validity.
        for key in date_range.keys():
            # If the key is not in the correct format, raise an error with an explanation and example.
            if key not in ACCEPTABLE_DATE_RANGE_KEYS:
                raise ValueError('Please input a valid key for the created_at filter type: {0}'.format(ACCEPTABLE_DATE_RANGE_KEYS))

        # If a starting date is applied, format the date to be accepted by the request call in the appropriate get method.
        if 'date_start' in date_range.keys():
            date_start = date_range['date_start']
            # Try to initialize passed date variable as a datetime object for further checking later.
            # Also, add 'T00:00:00.000Z' to the date string. This is the accepted format by the request call.
            try:
                datetime_start = datetime.strptime(date_start, DATE_FORMAT)
                date_start = date_start + 'T00:00:00.000Z'
            # If Try block fails, raise an error with an explanation and example.
            except:
                raise ValueError('Please input date_start value in the valid format \'{0}\''.format(DATE_FORMAT))

        # If a ending date is applied, format the date to be accepted by the request call in the appropriate get method.
        if 'date_end' in date_range.keys():
            date_end = date_range['date_end']
            # Try to initialize passed date variable as a datetime object for further checking later.
            # Also, add 'T00:00:00.000Z' to the date string. This is the accepted format by the request call.
            try:
                datetime_end = datetime.strptime(date_end, DATE_FORMAT)
                date_end = date_end + 'T00:00:00.000Z'
            # If Try block fails, raise an error with an explanation and example.
            except:
                raise ValueError('Please input date_end value in the valid format \'{0}\''.format(DATE_FORMAT))

        # If both starting and ending dates are passed...
        if date_start and date_end:
            # If the date range equals 0, meaning the same date was applied to both starting and ending dates,
            # raise an error with an explanation and example.
            if (datetime_end - datetime_start).days == 0:
                raise ValueError('The specified date range ({0}, {1}) equals 0.'.format(datetime_start, datetime_end))
            # If the date range is negative, meaning the ending date is earlier than the starting date,
            # raise an error with an explanation and example.
            if (datetime_end - datetime_start).days < 0:
                raise ValueError('date_end ({0}) is earlier than the date_start({1})'.format(datetime_end, datetime_start))

        print(' > Date filter processing was successful...')
        print(' > Starting Date: {0}'.format(date_start))
        print(' > Ending Date: {0}'.format(date_end))

        # Return the final starting date and ending date.
        return date_start, date_end

    #*************************************************************************************
    # Method: process_bulk_pages(self, dictionary={})
    #
    # Description
    # -----------
    # This method checks and processes all Page object filters. Specifically, all filter
    # keys and values are checked for correct spelling, data types and, other
    # inaccuracies. The date filter is passed to process_date_range() to be processed.
    # Finally, once all filters are processed, they are passed to the bulk_get_pages()
    # method.
    #
    # RETurn
    #  Type                                   Description
    # ----------   -----------------------------------------------------------------------
    # method       The DataFrame returned by bulk_get_pages().
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type             Name                            Description
    # -------------   --------------   ---------------------------------------------------
    # dictionary      filters          All of the accpeted filter keys and values for
    #                                  segmenting Page objects from the Unbounce server.
    #*************************************************************************************
    def process_bulk_pages(self, filters={}):

        print('- Processing Page Filters -\n')

        # Initialize acceptable filter keys, values and format examples, as static variables.
        ACCEPTABLE_FILTERS_FORMAT = {'created_at': {'date_start': '%Y-%m-%d', 'date_end': '%Y-%m-%d'}, 'domain': 'str/list', 'page_id': 'str/list', 'state': 'str'}
        ACCEPTABLE_FILTER_TYPES = ['created_at', 'domain', 'page_id', 'state']
        ACCEPTABLE_STATE_TYPES = ['published', 'unpublished']
        # Initialize filter variables as None values,
        # in case one, some or all is/are not appplied.
        date_start = None
        date_end = None
        domain_list = None
        page_id_list = None
        state = None

        # If the passed filters argument is a dictionary, pass.
        if isinstance(filters, dict):
            pass
        # Else, raise an error with an explanation and example.
        else:
            raise TypeError('Please input a dictionary with the correct format for the filters argument:\n{0}'.format(ACCEPTABLE_FILTERS_FORMAT))

        # Iterate over keys and values in the passed filters dictionary to check for validity.
        for key in filters:
            # If the key is not in the correct format, raise an error with an explanation and example.
            if key not in ACCEPTABLE_FILTER_TYPES:
                raise ValueError('Please input a valid Page filter types: {0}'.format(ACCEPTABLE_FILTER_TYPES))

        # If created at date filter is applied, initialize the created at dictionary value as a variable.
        # Pass this variable to the process_date_range() method to be processed and to initialize formatted dates as variables.
        if 'created_at' in filters.keys():
            date_range = filters['created_at']
            date_start, date_end = self.process_date_range(date_range)

        # If domain filter is applied...
        if 'domain' in filters.keys():

            print('\nDomain filter applied. Processing Domain filter...')

            # If the domain dictionary value consists of a string,
            # initialize the string as a single entry list.
            if isinstance(filters['domain'], str):
                domain_list = [filters['domain']]
            # Else if, the domain dictionary value consists of a list,
            # Initialize this list, as a variable.
            elif isinstance(filters['domain'], list):
                domain_list = filters['domain']
            # Else, if the domain dictionary value is neither a string or list,
            # raise an error with an explanation.
            else:
                raise TypeError('Please input either a string or list for the domain filter type')

            print(' > Domain filter processing was successful.')
            print(' > Domain(s): {0}'.format(domain_list))

        # If Page ID filter is applied...
        if 'page_id' in filters.keys():

            print('\nPage ID filter applied. Processing Page ID filter...')

            # If the Page ID dictionary value consists of a string,
            # initialize the string as a single entry list.
            if isinstance(filters['page_id'], str):
                page_id_list = [filters['page_id']]
            # Else if, the Page ID dictionary value consists of a list,
            # Initialize this list, as a variable.
            elif isinstance(filters['page_id'], list):
                page_id_list = filters['page_id']
            # Else, if the Page ID dictionary value is neither a string or list,
            # raise an error with an explanation.
            else:
                raise TypeError('Please input either a string or list for the page_id filter type')

            print(' > Page ID filter processing was successful.')
            print(' > Page ID(s): {0}'.format(page_id_list))

        # If State filter is applied...
        if 'state' in filters.keys():

            print('\nState filter applied. Processing State filter...')

            # If State dictionary value is not in the correct format, raise an error with an explanation and example.
            if filters['state'] not in ACCEPTABLE_STATE_TYPES:
                raise ValueError('Please input a valid state filter type: {0}'.format(ACCEPTABLE_STATE_TYPES))
            # Else, initialize the State dictionary value as a variable.
            else:
                state = filters['state']

            print(' > State filter processing was successful.')
            print(' > State: {0}'.format(state))

        if all(filter is None for filter in [date_start, date_end, domain_list, page_id_list, state]):
            print('\nNo Page filters applied!')
            print('\n-----------------------------')
        else:
            print('\nPage Filters Processed!')
            print('\n-----------------------------')

        # Return bulk Pages:
        # The return value from the bulk_get_pages() method with the formatted filter arguments applied.
        bulk_pages_dict = self.bulk_get_pages(date_start=date_start, date_end=date_end, domain_list=domain_list, page_id_list=page_id_list, state=state)
        return bulk_pages_dict

    #*************************************************************************************
    # Method: process_bulk_leads(self, dictionary={})
    #
    # Description
    # -----------
    # This method checks and processes all Lead object filters. Specifically, all filter
    # keys and values are checked for correct spelling, data types and, other
    # inaccuracies. The date filter is passed to process_date_range() to be processed.
    # Finally, once all filters are processed, they are passed to the bulk_get_leads()
    # method.
    #
    # RETurn
    #  Type                                   Description
    # ----------   -----------------------------------------------------------------------
    # method       The DataFrame returned by bulk_get_leads().
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type             Name                            Description
    # -------------   --------------   ---------------------------------------------------
    # dictionary      filters          All of the accpeted filter keys and values for
    #                                  segmenting Lead objects from the Unbounce server.
    #*************************************************************************************
    def process_bulk_leads(self, filters={}):

        print('- Processing Lead Filters -\n')

        # Initialize acceptable filter keys, values and format examples, as static variables.
        ACCEPTABLE_FILTERS_FORMAT = {'created_at': {'date_start': '%Y-%m-%d', 'date_end': '%Y-%m-%d'}, 'lead_id': 'str/list', 'page_id': 'str/list'}
        ACCEPTABLE_FILTER_TYPES = ['created_at', 'lead_id', 'page_id']
        # Initialize filter variables as None values,
        # in case one, some or all is/are not appplied.
        date_start = None
        date_end = None
        lead_id_list = None
        page_id_list = None

        # If the passed filters argument is a dictionary, pass.
        if isinstance(filters, dict):
            pass
        # Else, raise an error with an explanation and example.
        else:
            raise TypeError('Please input a dictionary with the correct format for the filters argument:\n{0}'.format(ACCEPTABLE_FILTERS_FORMAT))

        # Iterate over keys and values in the passed filters dictionary to check for validity.
        for key in filters:
            # If the key is not in the correct format, raise an error with an explanation and example.
            if key not in ACCEPTABLE_FILTER_TYPES:
                raise ValueError('Please input a valid Lead filter types: {0}'.format(ACCEPTABLE_FILTER_TYPES))

        # If created at date filter is applied, initialize the created at dictionary value as a variable.
        # Pass this variable to the process_date_range() method to be processed and to initialize formatted dates as variables.
        if 'created_at' in filters.keys():
            date_range = filters['created_at']
            date_start, date_end = self.process_date_range(date_range)

        # If Lead ID filter is applied...
        if 'lead_id' in filters.keys():

            print('\nLead ID filter applied. Processing Lead ID filter...')

            # If the Lead ID dictionary value consists of a string,
            # initialize the string as a single entry list.
            if isinstance(filters['lead_id'], str):
                lead_id_list = [filters['lead_id']]
            # Else if, the Lead ID dictionary value consists of a list,
            # Initialize this list, as a variable.
            elif isinstance(filters['lead_id'], list):
                lead_id_list = filters['lead_id']
            # Else, if the Lead ID dictionary value is neither a string or list,
            # raise an error with an explanation.
            else:
                raise TypeError('Please input either a string or list for the lead_id filter type')

            print(' > Lead ID filter processing was successful.')
            print(' > Lead ID(s): {0}'.format(lead_id_list))

        # If Page ID filter is applied...
        if 'page_id' in filters.keys():

            print('\nPage ID filter applied. Processing Page ID filter...')

            # If the Page ID dictionary value consists of a string,
            # initialize the string as a single entry list.
            if isinstance(filters['page_id'], str):
                page_id_list = [filters['page_id']]
            # Else if, the Page ID dictionary value consists of a list,
            # Initialize this list, as a variable.
            elif isinstance(filters['page_id'], list):
                page_id_list = filters['page_id']
            # Else, if the Page ID dictionary value is neither a string or list,
            # raise an error with an explanation.
            else:
                raise TypeError('Please input either a string or list for the page_id filter type')

            print(' > Page ID filter processing was successful.')
            print(' > Page ID(s): {0}'.format(page_id_list))

        if all(filter is None for filter in [date_start, date_end, lead_id_list, page_id_list]):
            print('\nNo Lead filters applied!')
            print('\n-----------------------------')
        else:
            print('\nLead Filters Processed!')
            print('\n-----------------------------')

        # Return bulk Leads:
        # The return value from the bulk_get_leads() method with the formatted filter arguments applied.
        bulk_leads_dict = self.bulk_get_leads(date_start=date_start, date_end=date_end, lead_id_list=lead_id_list, page_id_list=page_id_list)
        return bulk_leads_dict

    #*************************************************************************************
    # Method: bulk_extract(self, string, dictionary={})
    #
    # Description
    # -----------
    # This method is the only method needed by the user to extract bulk Page or Lead
    # objects from the Unbounce server. The desired object type and associated filters
    # should be passed to this method. Calling this method will allow for the appropriate
    # processing and request methods to be called and the correct value to be returned.
    # The final return value is a formated DataFrame consisting of Page or Lead objects
    # from the Unbounce server.
    #
    # RETurn
    #  Type                                   Description
    # ----------   -----------------------------------------------------------------------
    # method       The appropriate method to process and request data from the Unbounce
    #              server.
    #
    # ------------------------------- Arguments ------------------------------------------
    #     Type             Name                            Description
    # -------------   --------------   ---------------------------------------------------
    # string          extract_obj      The object type desired from the Unbounce server.
    #                                  Either 'Pages' or 'Leads'.
    # dictionary      filters          All of the accpeted filter keys and values for
    #                                  segmenting Page or Lead objects from the Unbounce
    #                                  server.
    #
    # --------------------------------- Notes --------------------------------------------
    # Pages filter formats:
    # filters = {'created_at': {'date_start': '%Y-%m-%d', 'date_end': '%Y-%m-%d'},
    #            'domain': 'str/list',  --One or more domains
    #            'page_id': 'str/list', --One or more Page IDs
    #            'state': 'str'}        --Only accepts 'published'/'unpublished'
    #
    # Leads filter formats:
    # filters = {'created_at': {'date_start': '%Y-%m-%d', 'date_end': '%Y-%m-%d'},
    #            'lead_id': 'str/list',  --One or more Lead IDs
    #            'page_id': 'str/list'} --One or more Page IDs
    #
    # -------------------------------- Examples ------------------------------------------
    # Pages Example 1:
    # filters_p1 = {'created_at': {'date_start': '2019-01-01'},
    #            'domain': 'get.pitchbook.com',
    #            'state': 'published'}
    # pages_ex_1 = bulk_extract('pages', filters=filters_p1)
    #
    # Pages Example 2:
    # filters_p2 = {'created_at': {'date_start': '2018-01-01', 'date_end': '2019-01-01'}}
    # pages_ex_2 = bulk_extract('pages', filters=filters_p2)
    #
    # Leads Example 1:
    # filters_l1 = {'created_at': {'date_end': '2018-01-01'}
    # leads_ex_1 = bulk_extract('leads', filters=filters_l1)
    #
    # Leads Example 2:
    # leads_ex_2 = bulk_extract('leads')
    #*************************************************************************************
    def bulk_extract(self, extract_obj, filters={}):

        print('>>  Unbounce Bulk Extract  <<\n')
        print('Current Extract Details:')
        print(' > Desired Extract Object: {0}'.format(extract_obj))
        print(' > Filters Appplied: {0}'.format(filters))
        print(' > API\'s \'get\' method timeout limit: {0} seconds'.format(self.get_timeout_time))
        print(' > Bulk extract runtime limit: {0} seconds'.format(self.extract_timeout_time))
        print('\n-----------------------------')

        # Initialize acceptable object types, as static variables.
        ACCEPTABLE_OBJ_TYPES = ['pages', 'leads']

        # If the passed object type is no an acceptable object type, raise an error with an explanation and example.
        if extract_obj not in ACCEPTABLE_OBJ_TYPES:
            raise ValueError('Please input a valid value for extract_obj: {0}'.format(ACCEPTABLE_OBJ_TYPES))

        # If the passed object type is 'pages' start processing the bulk Pages extract request,
        # with the passed filters argument.
        if extract_obj == 'pages':
            return self.process_bulk_pages(filters=filters)
        # Else, start processing the bulk Leads extract request,
        # with the passed filters argument.
        else:
            return self.process_bulk_leads(filters=filters)
