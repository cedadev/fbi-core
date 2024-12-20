# Always prefer setuptools over distutils
# To use a consistent encoding
from codecs import open
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='fbi-core',
    version='1.3.0',
    description='File Based Index  (FBI) core tools',
    long_description=long_description,

    # The project's main homepage.
    url='http://www.ceda.ac.uk',
    # Author details
    author='Sam Pepler',
    author_email='sam.pepler@stfc.ac.uk',
    # Choose your license
    license='BSD',
    install_requires=['tabulate', 'click', 'pyyaml', 'elasticsearch', 'colorama'],

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Beta',
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='ingest',
    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(),

    entry_points={
        'console_scripts': [
            'fbi_filesize=fbi_core.fbi_filesize:summary',
            'fbi_ls=fbi_core.fbi_filesize:ls2',
            'fbi_random=fbi_core.fbi_filesize:random_paths',
            'fbi_show_record=fbi_core.fbi_filesize:show_record',
            'fbi_parameters=fbi_core.fbi_filesize:show_parameters',
            'fbi_last_updated=fbi_core.fbi_filesize:show_last_updated',
            'fbi_split=fbi_core.fbi_filesize:find_splits',
            'fbi_rsplit=fbi_core.fbi_filesize:find_rsplits',
            'fbi_md5sum=fbi_core.fbi_filesize:md5sum',
            'fbi_md5sum_check=fbi_core.fbi_filesize:check_archive_by_checksum',
            'fbi_dump=fbi_core.fbi_dump:setup_run',
            'fbi_batch_create=fbi_core.fbi_dump:setup_run',
            'fbi_launch_run=fbi_core.fbi_dump:launch_run',
            'fbi_batch_run=fbi_core.fbi_dump:batch_run',
            'fbi_links_to=fbi_core.fbi_filesize:find_links_to',
            'fbi_annotate=fbi_core.annotate:grab_moles',
        ],
    },
)
