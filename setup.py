from setuptools import setup

setup(
    name='ureport-data',
    version='0.1',
    packages=['ureport_data'],
    install_requires=['ipython[notebook]', 'celery[redis]', 'Humongolus', 'rapidpro-python', 'retrying'],
    dependency_links=[
        'git+https://github.com/xkmato/Humongolus.git@patch#egg=Humongolus-1.0.6'
    ],
    zip_safe=False,
    license='BSD',
    include_package_data=True,

    author='kenneth',
    description='Open ureport data for Ipython Notebooks',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ]
)
