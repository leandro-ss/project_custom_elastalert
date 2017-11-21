import os
from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(name='CustomRule',
      version='1.0',
      license='BSD License',
      include_package_data=True,
      packages=find_packages(exclude=['examp','tests']),
      url='https://www.inmetrics.com.br/',
      description='Elastialert Custom Rule',
      author='Leandro Sampaio',
      author_email='lss.aspira@gmail.com',
      long_description=README,
      classifiers=[
            'Intended Audience :: Deveopers',
            'License :: OSI Approved :: BSD License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2.7'
      ],
      install_requires=['elastalert'],
      extras_require={
            'dev': ['elastalert']
      }
)