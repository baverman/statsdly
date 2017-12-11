from setuptools import setup, find_packages
import statsdly

setup(
    name='statsdly',
    version=statsdly.version,
    url='https://github.com/baverman/statsdly/',
    license='MIT',
    author='Anton Bobrov',
    author_email='baverman@gmail.com',
    description='StatsD server with exporting metrics to graphite/carbon',
    long_description=open('README.rst').read(),
    py_modules=['statsdly'],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    entry_points={
        'console_scripts': ['statsdly = statsdly:main']
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
        'Topic :: System :: Monitoring',
    ]
)
