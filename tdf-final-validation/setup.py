from setuptools import setup, find_packages
import os
import sys
requirements = [l.strip() for l in open('requirements.txt').readlines()]
python_version=str(sys.version_info[0])+'.'+str(sys.version_info[1])
with open('README.rst') as file:
    long_description = file.read()
dir_path = os.path.dirname(os.path.realpath(__file__))
package_name=dir_path.split("/")[-1]
config = {
    'name':package_name,
    'description': 'python module'+package_name,
    'long_description': long_description,
    'version': python_version+'-'+os.environ['BUILD_NUMBER'],
    'author': 'parthiv.sagi',
    'author_email': 'parthiv.sagi@nielsen.com',
    'url': 'ssh://git@adlm.nielsen.com:7999/tmf/'+package_name,
    'license': 'MIT',
    'include_package_data': True,
    'packages': find_packages(),
    'install_requires':requirements
}

setup(**config)
