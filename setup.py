from setuptools import setup, find_packages

setup(
    name="vumi-message-store",
    version="0.1.4",
    url="http://github.com/praekelt/vumi-message-store",
    license="BSD",
    description="A persistent store for vumi messages.",
    long_description=open("README.rst", "r").read(),
    author="Praekelt Foundation",
    author_email="dev@praekeltfoundation.org",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "vumi>=0.5.21",
        "Twisted>=13.1.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking",
    ],
)
