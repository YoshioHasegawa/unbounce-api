# Unbounce API
A [Unbounce API](https://developer.unbounce.com/api_reference/) wrapper written in Python.


## Getting Started
Find more information on authorization, managing API keys, using OAuth, permissions, rate limits, errors, and
more on [Unbounce's API webpage](https://developer.unbounce.com/getting_started/). Find the project on
The Python Package Index website here: [unbounce-python-api] (https://pypi.org/project/unbounce-python-api/).

To get started run `pip install unbounce-python-api`

```python
from unbounceapi.client import Unbounce
ub = Unbounce('YOUR_API_KEY')
```

## Global API
Read the docs here: [Global API](https://developer.unbounce.com/api_reference/).

**Available Methods**

- `ub.get_global()`

## Accounts
Read the docs here: [Accounts API](https://developer.unbounce.com/api_reference/#id_accounts).

**Available Methods**

- `ub.accounts.get_accounts(account_id=None, **kwargs)`
    - account_id (optional)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']

- `ub.accounts.get_sub_accounts(account_id, **kwargs)`
    - accounts_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']

- `ub.accounts.get_account_pages(account_id, **kwargs)`
    - account_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']

## Sub Accounts
Read the docs here: [Sub Accounts API](https://developer.unbounce.com/api_reference/#id_sub_accounts__sub_account_id_).

**Available Methods**

- `ub.sub_accounts.get_sub_account(sub_account_id)`
    - sub_account_id (required)

- `ub.sub_accounts.get_sub_account_domains(sub_account_id, **kwargs)`
    - sub_account_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or ' desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']

- `ub.sub_accounts.get_sub_account_page_groups(sub_account_id, **kwargs)`
    - sub_account_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']

- `ub.sub_accounts.get_sub_accounts_pages(sub_account_id, **kwargs)`
    - sub_account_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']

## Domains
Read the docs here: [Domains API](https://developer.unbounce.com/api_reference/#id_domains__domain_id_).

**Available Methods**

- `ub.domains.get_domain(domain_id)`
    - domain_id (required)

- `ub.domains.get_domain_pages(domain_id, kwargs**)`
    - domain_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']

## Pages
Read the docs here: [Pages API](https://developer.unbounce.com/api_reference/#id_pages).

**Available Methods**

- `ub.pages.get_pages(page_id=None, **kwargs)`
    - page_id (optional)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']
    - with_stats (optional) [Default: 'False', Options: 'True' or 'False']
    - role (optional) [Default: 'author', Options: 'author' or 'viewer']

- `ub.pages.get_form_fields(page_id, **kwargs)`
    - page_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - include_sub_pages [Default: 'False', Options: 'True' or 'False']

- `ub.pages.get_page_leads(page_id, lead_id=None, **kwargs)`
    - page_id (required)
    - lead_id (optional)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', ex: '10']

- `ub.pages.create_page_lead(page_id)`
    - page_id (required)

- `ub.pages.delete_page_lead(page_id, lead_id)`
    - page_id (required)
    - lead_id (required)

- `ub.pages.post_lead_deletion_request(page_id)`
    - page_id (required)

- `ub.pages.get_lead_deletion_request_status(page_id, lead_deletion_request_id)`
    - page_id (required)
    - lead_deletion_request_id (required)

## Page Groups
Read the docs here: [Page Groups API](https://developer.unbounce.com/api_reference/#id_page_groups__page_group_id__pages).

**Available Methods**

- `ub.page_groups.get_page_group_pages(page_group_id, **kwargs)`
    - page_group_id (required)
    - sort_order (optional) [Default: 'asc', Options: 'asc' or 'desc']
    - count (optional) [Default: 'False', Options: 'True' or 'False']
    - _from (optional) [ex: '2018-01-01T00:00:00.000Z']
    - to (optional) [ex: '2018-12-31T00:00:00.000Z']
    - offset (optional) [ex: '3']
    - limit (optional) [Default: '50', Max: '1000', Ex: '10']

## Leads
Read the docs here: [Leads API](https://developer.unbounce.com/api_reference/#id_leads__lead_id_).

**Available Methods**

- `ub.leads.get_lead(lead_id)`
    - lead_id (required)

## Users
Read the docs here: [Users API](https://developer.unbounce.com/api_reference/#id_users).

- `ub.users.get_user(user_id=None)`
    - user_id (optional)


## Contributors
* Yoshio Hasegawa


## Support / Feedback / Bugs
For support, feedback or, if you've found a bug you may contact the primary contributor here: [Yoshio Hasegawa](mailto:yoshio.seisuke.hasegawa@gmail.com).
