#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="batching-kafka-consumer",
    version="0.0.5",
    author="Sentry Team and Contributors",
    author_email="hello@getsentry.com",
    url="https://github.com/getsentry/batching-kafka-consumer",
    description="Kafka Consumer abstraction that assists with processing batches and committing offsets.",
    long_description="Kafka Consumer abstraction that assists with processing batches and committing offsets.",
    packages=find_packages(exclude=("tests", "tests.*")),
    zip_safe=False,
    license="BSD",
    install_requires=["confluent-kafka==1.5.0"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
