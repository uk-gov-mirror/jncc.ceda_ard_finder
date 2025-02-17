import luigi
import json
import os
import logging
import re

from luigi.util import requires
from .SearchForProducts import SearchForProducts

log = logging.getLogger("luigi-interface")

patternSets = {
    "S1": ["^([\w\d]{3})_\d{8}(_[^_]*){9}_[^_\.]*"],
    "S2": ["^([\w\d]{3})_\d{8}_lat\d+lon\d+(_[^_]*){3}_[^_\.]*",
           "^([\w\d]{3})_\d{8}_lat[ns]\d+lon[ew]\d+(_[^_]*){4}_[^_\.]*"]
}


@requires(SearchForProducts)
class CreateTextFileList(luigi.Task):
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()
    productLocation = luigi.Parameter()
    descriptor = luigi.Parameter(default="")

    @staticmethod
    def get_identifier(filename):

        patterns = patternSets[filename[:2]]

        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                return match.group()

        return None

    def run(self):
        products = []
        filename = f"{self.startDate:%Y-%m-%d}_{self.endDate:%Y-%m-%d}_{self.descriptor}.txt"
        with self.input().open('r') as searchForProductsFile:
            products = json.load(searchForProductsFile)['productList']
        products = [p if '/' not in p else p.rsplit('/', 1)[1] for p in products]
        products = [self.get_identifier(p) for p in products]

        with open(os.path.join(self.productLocation, filename), "w") as outfile:
            for product in products:
                outfile.write(f"{product}\n")
