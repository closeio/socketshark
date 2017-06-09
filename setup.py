from setuptools import setup

install_requires = [
    'aiohttp',
    'aioredis',
    'click',
    'structlog[dev]',
    'websockets',
]

tests_require = install_requires + [
    'aioresponses',
    'pytest',
    'pytest-asyncio',
]

setup(
    name='socketshark',
    version='0.1',
    url='http://github.com/closeio/socketshark',
    license='MIT',
    description='WebSocket message router',
    test_suite='tests',
    tests_require=tests_require,
    platforms='any',
    install_requires=install_requires,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    packages=[
        'socketshark',
        'socketshark.backend',
        'socketshark.metrics',
    ],
    entry_points={
        'console_scripts': [
            'socketshark = socketshark.__main__:run',
        ],
    },
)
