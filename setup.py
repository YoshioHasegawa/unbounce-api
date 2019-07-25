# unbounceapi/setup.py
from distutils.core import setup
setup(
  name = 'unbounceapi',
  packages = ['unbounceapi'],
  version = '1.1.1',
  license='MIT',
  description = 'An Unbounce API wrapper written in python.',
  author = 'Yoshio Hasegawa',
  author_email = 'yoshiohasegawa206@gmail.com',
  url = 'https://github.com/YoshioHasegawa/unbounce-api',
  download_url = 'https://github.com/YoshioHasegawa/unbounce-api/archive/1.1.1.tar.gz',
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
