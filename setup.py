# unbounceapi/setup.py
from setuptools import setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
  name = 'unbounce-python-api',
  packages = ['unbounceapi'],
  version = '1.3.2',
  license='MIT',
  description = 'An Unbounce API wrapper written in python.',
  long_description = long_description,
  long_description_content_type = 'text/markdown',
  author = 'Yoshio Hasegawa',
  author_email = 'yoshiohasegawa206@gmail.com',
  url = 'https://github.com/YoshioHasegawa/unbounce-python-api',
  download_url = 'https://github.com/YoshioHasegawa/unbounce-python-api/archive/v1.3.2.tar.gz',
  keywords = ['Unbounce', 'API', 'Wrapper'],
  install_requires=[
          'requests',
          'pytest'
      ],
  classifiers=[
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
  ],
)
