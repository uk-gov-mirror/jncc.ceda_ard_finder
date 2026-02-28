import setuptools

setuptools.setup(
    name="ceda_ard_finder",
    version="0.0.4",
    author="JNCC",
    author_email="developers@jncc.gov.uk",
    description="A luigi workflow to get ARD products from the CEDA archive",
    long_description="""
        A Luigi workflow to get Sentnel 1 and 2 ARD products from the CEDA archive
        
        The workflow uses the CEDA archive to retreive details of ARD products or create symlinks on JASMIN.
        
        [The CEDA data portal ARD data](https://catalogue.ceda.ac.uk/uuid/bf9568b558204b81803eeebcc7f529ef)

        [Luigi workflow](https://luigi.readthedocs.io/en/stable/index.html)
    """,
    long_description_content_type="text/markdown",
    url="https://github.com/jncc/ceda_ard_finder",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'luigi',
        'elasticsearch'
    ],
    python_requires='>=3.7',
)
