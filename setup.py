import io
from setuptools import find_packages, setup
from django_backup import __version__

setup(
    name='django-backup',
    version=__version__,
    description='A backup script for the Django admin',
    long_description=io.open('README.rst', encoding='utf-8').read(),
    author='Dmitriy Kovalev, Michael Huynh, msaelices, Andy Baker, Chen Zhe, Chris Cohoat',
    author_email='andy@ixxy.co.uk',
    url='http://github.com/django-backup/django-backup',
    packages=find_packages(exclude=('test_project',)),
    include_package_data=True,
    install_requires=['pysftp'],
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Development Status :: 4 - Beta',
        'Framework :: Django',
        'Framework :: Django :: 1.7',
        'Framework :: Django :: 1.8',
        'License :: OSI Approved :: BSD License',
    ]
)
