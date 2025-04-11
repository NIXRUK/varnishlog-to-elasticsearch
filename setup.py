from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="varnishlog-to-elasticsearch",
    version="0.1.0",
    author="Will Riches",
    author_email="will@nixr.com",
    description="A tool to parse Varnish HTTP logs and send them to Elasticsearch",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NIXRUK/varnishlog-to-elasticsearch",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.31.0",
        "urllib3>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "varnishlog-to-es=varnishlog_to_elasticsearch.cli:main",
        ],
    },
)
