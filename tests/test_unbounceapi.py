# tests/test_unbounceapi.py
# Tests the functionality of API methods from unbounceapi.client()

# Importing relevant libraries
import pytest
from unbounceapi.client import Unbounce

@pytest.fixture
# Responsible only for returning an instance of unbounceapi.client
def client():
    return Unbounce('API_KEY')

# test_unbounceapi() tests the Unbounce client and a few of its methods.
def test_unbounceapi(client):
    # Initializing page_form_fields with a subset of known form fields from a given page to test with.
    # The entries in this list are acquired and entered manually.
    page_form_fields = ['first_name', 'last_name']
    # Initializing page_instance_form_fields with form fields returned by pages.get_form_fields(), from a given page to test with.
    page_instance_form_fields = client.pages.get_form_fields('PAGE_ID')

    # Parse through page_instance_form_fields['formFields'] list found in returned dictionary to extract form fill IDs.
    # These IDs should match the items manually entered into page_form_fields.
    page_instance_form_field_ids = []
    for entry in page_instance_form_fields['formFields']:
        page_instance_form_field_ids.append(entry['id'])

    # Compare page_form_fields with page_instance_form_field_ids.
    assert set(page_form_fields).issubset(page_instance_form_field_ids), 'All fields should be in the response'
