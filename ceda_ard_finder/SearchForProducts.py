import luigi
import json
import os
import logging
import glob
from datetime import datetime, timedelta

from .CedaElasticsearchQueryer import CedaElasticsearchQueryer

# https://elasticsearch.ceda.ac.uk/ceda-eo/_search
# "POLYGON((-3.8295000316687355 53.23354636293673, -2.7418535472937355 53.23354636293673, -2.7418535472937355 52.484002471274685, -3.8295000316687355 52.484002471274685, -3.8295000316687355 53.23354636293673))"

log = logging.getLogger("luigi-interface")


def _matches_orbit(filename, orbit):
    """Check if filename matches the orbit number (handles both _XXX_ and _ORBXXX_ formats)."""
    orbit_str = f"_{orbit:03d}_"
    orbit_str_orb = f"_ORB{orbit:03d}_"
    return orbit_str in filename or orbit_str_orb in filename


def _matches_orbit_direction(filename, direction):
    """Check if filename matches the orbit direction (asc/desc)."""
    return f"_{direction.lower()}_" in filename.lower()


class SearchForProducts(luigi.Task):
    stateFolder = luigi.Parameter()
    ardFilter = luigi.Parameter(default="")

    _stateFileName = luigi.Parameter(default="SearchForProducts.json")

    # Sentinel-1C and Sentinel-2C ARD products are not indexed in CEDA's Elasticsearch
    # s1c / s2c base paths and suffixes
    _s1cArdBasePath = luigi.Parameter(default="/neodc/sentinel_ard/data/sentinel_1")
    _s2cArdBasePath = luigi.Parameter(default="/neodc/sentinel_ard/data/sentinel_2")
    _s1cTifSuffix = luigi.Parameter(default="RTCK_SpkRL.tif")
    _s2cTifSuffix = luigi.Parameter(default="vmsk_sharp_rad_srefdem_stdsref.tif")

    # search filters
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()
    wkt = luigi.Parameter(default="")
    spatialOperator = luigi.ChoiceParameter(choices=["", "intersects", "disjoint", "contains", "within"])
    satelliteFilter = luigi.Parameter(default="")
    orbit = luigi.IntParameter(default=-9999)
    orbitDirection = luigi.Parameter(default="")

    elasticsearchHost = luigi.Parameter(default="elasticsearch.ceda.ac.uk")
    elasticsearchPort = luigi.IntParameter(default=443)
    elasticsearchIndex = luigi.Parameter(default="ceda-eo")
    elasticsearchPageSize = luigi.IntParameter(default=100)

    def parseResults(self, results):
        productList = []
        for result in results['hits']['hits']:
            dataFilepath = os.path.join(result['_source']['file']['directory'], result['_source']['file']['data_file'])
            productList.append(dataFilepath)

        return productList

    def queryAllResults(self, queryer):
        productList = []
        results = queryer.query(index=self.elasticsearchIndex, start=0, size=self.elasticsearchPageSize)
        log.info(results)
        productList.extend(self.parseResults(results))

        while len(productList) < results['hits']['total']['value']:
            nextPage = queryer.query(index=self.elasticsearchIndex, start=len(productList), size=self.elasticsearchPageSize)
            log.info(nextPage)
            productList.extend(self.parseResults(nextPage))

        return productList

    def _parse_satellite_list(self):
        """Parse satelliteFilter into a list of uppercase satellite names."""
        if not self.satelliteFilter:
            return []
        return [x.strip().upper() for x in self.satelliteFilter.split(',')]

    def _is_sentinel_c_search(self):
        """
        Check if this search includes Sentinel-1C or Sentinel-2C ARD products.
        These products are not indexed in CEDA's Elasticsearch.
        Returns the prefix ("S1C" or "S2C") if C satellite search is needed, None otherwise.
        Note: S1C and S2C are never searched together (always Sentinel-1 OR Sentinel-2).
        """
        satellites = self._parse_satellite_list()
        for sat in satellites:
            if "SENTINEL-1C" in sat:
                return "S1C"
            elif "SENTINEL-2C" in sat:
                return "S2C"

        if self.ardFilter and self.ardFilter.strip().upper().startswith("S1C"):
            return "S1C"
        elif self.ardFilter and self.ardFilter.strip().upper().startswith("S2C"):
            return "S2C"

        return None

    def _needs_elasticsearch_search(self):
        """
        Check if this search requires Elasticsearch (for A/B satellites).
        Returns True if the search includes any satellite other than 1C/2C,
        or if no specific satellite filter is set (general search).
        """
        satellites = self._parse_satellite_list()
        if not satellites:
            if self.ardFilter and self.ardFilter.strip().upper().startswith(("S1C", "S2C")):
                # If no satellite filter but ardFilter indicates S1C or S2C, we only need filesystem search
                return False
            # No specific filter - need ES for general search (won't return C though)
            return True

        # Check if any satellite is NOT 1C or 2C (i.e., needs ES)
        for sat in satellites:
            if "SENTINEL-1C" not in sat and "SENTINEL-2C" not in sat:
                return True
        # All satellites are C variants - no ES needed
        return False

    def _get_elasticsearch_satellite_filter(self):
        """
        Get the satellite filter for Elasticsearch, excluding 1C and 2C satellites.
        """
        if not self.satelliteFilter:
            return self.satelliteFilter

        satellites = [x.strip() for x in self.satelliteFilter.split(',')]
        es_satellites = [s for s in satellites
                         if "Sentinel-1C" not in s and "Sentinel-2C" not in s]
        return ', '.join(es_satellites)

    def _search_filesystem_for_sentinel_c(self, prefix):
        """
        Search the filesystem for Sentinel-1C or Sentinel-2C ARD products.
        These products are not indexed in CEDA's Elasticsearch but exist on disk.

        Directory structure: /neodc/sentinel_ard/data/sentinel_X/YYYY/MM/DD/

        NOTE: Spatial filtering (WKT/geometry) is NOT supported for filesystem search.
        Only the following filters are applied:
        - Date range (startDate, endDate)
        - ardFilter (filename prefix matching)
        - orbit
        - orbitDirection

        Args:
            prefix: "S1C" or "S2C" indicating which satellite to search for
        """
        if prefix == "S1C":
            base_path = self._s1cArdBasePath
        elif prefix == "S2C":
            base_path = self._s2cArdBasePath
        else:
            return []

        productList = []
        current_date = self.startDate
        end_date = self.endDate

        log.info(f"Searching filesystem for {prefix} ARD products in {base_path}")
        log.info(f"Date range: {current_date} to {end_date}")

        # Iterate through each day in the date range
        while current_date <= end_date:
            day_path = os.path.join(
                base_path,
                str(current_date.year),
                f"{current_date.month:02d}",
                f"{current_date.day:02d}"
            )

            if os.path.isdir(day_path):
                # Build search pattern with correct suffix for main product TIF only
                tif_suffix = self._s1cTifSuffix if prefix == "S1C" else self._s2cTifSuffix

                if self.ardFilter and self.ardFilter.strip().upper().startswith(prefix):
                    # Search for files matching the ardFilter prefix with correct suffix
                    # For input list searching
                    search_pattern = os.path.join(day_path, f"{self.ardFilter}*{tif_suffix}")
                elif self.ardFilter:
                    # For general ARD searching
                    search_pattern = os.path.join(day_path, f"{prefix}*{self.ardFilter}*{tif_suffix}")
                else:
                    # Search for all S1C or S2C main product TIF files
                    search_pattern = os.path.join(day_path, f"{prefix}_*{tif_suffix}")

                matching_files = glob.glob(search_pattern)

                # Apply orbit filter if specified
                if self.orbit != -9999:
                    matching_files = [f for f in matching_files
                                      if _matches_orbit(os.path.basename(f), self.orbit)]

                # Apply orbit direction filter if specified
                if self.orbitDirection:
                    matching_files = [f for f in matching_files
                                      if _matches_orbit_direction(os.path.basename(f), self.orbitDirection)]

                productList.extend(matching_files)
                if matching_files:
                    log.info(f"Found {len(matching_files)} {prefix} matching products in {day_path}")

            current_date = current_date + timedelta(days=1)

        # Sort by filename (which includes date) to match Elasticsearch behavior
        productList.sort()
        log.info(f"Total {prefix} ARD products found via filesystem: {len(productList)}")

        return productList

    def run(self):
        productList = []

        # Check if we need filesystem search for Sentinel-1C or Sentinel-2C
        sentinel_c_prefix = self._is_sentinel_c_search()
        needs_elasticsearch = self._needs_elasticsearch_search()

        # Search filesystem for Sentinel-C if needed
        if sentinel_c_prefix:
            # Warn if spatial filter is specified - not supported for filesystem search
            if self.wkt and self.spatialOperator:
                log.warning(
                    f"Spatial filtering (wkt/spatialOperator) is not supported for {sentinel_c_prefix} "
                    "filesystem search. Spatial filter will be IGNORED for Sentinel-C products. "
                    "Only date range, orbit, and orbitDirection filters are applied."
                )

            log.info(f"{sentinel_c_prefix} ARD search detected - using filesystem search")
            filesystem_products = self._search_filesystem_for_sentinel_c(sentinel_c_prefix)
            productList.extend(filesystem_products)

        # Search Elasticsearch for A/B satellites if needed
        if needs_elasticsearch:
            log.info("Searching Elasticsearch for indexed satellites")
            queryer = CedaElasticsearchQueryer(host=self.elasticsearchHost, port=self.elasticsearchPort)

            if self.spatialOperator != "" and self.wkt != "":
                queryer.addGeoFilter(spatialOperation=self.spatialOperator, wkt=self.wkt)

            if self.startDate != None and self.endDate != None:
                queryer.addDateFilter(start_date=self.startDate, end_date=self.endDate)

            # Use filtered satellite list (excluding 1C/2C) if we have a mixed search
            es_satellite_filter = self._get_elasticsearch_satellite_filter()

            if es_satellite_filter:
                satelliteFilterArray = [x.strip() for x in es_satellite_filter.split(',')]
                queryer.addSatelliteFilters(satelliteFilterArray)
            elif self.satelliteFilter == "" and self.ardFilter != "":
                # Derive satellite from ardFilter for non-C products
                queryer.addSatelliteFilters([f"Sentinel-{self.ardFilter.strip()[1:3]} ARD"])

            if self.orbit != -9999:
                queryer.addOrbitFilter(self.orbit)

            if self.orbitDirection != "":
                queryer.addOrbitDirectionFilter(self.orbitDirection)

            if self.ardFilter != "":
                queryer.addArdFilter(self.ardFilter)

            es_products = self.queryAllResults(queryer)
            productList.extend(es_products)

        # Sort combined results
        productList.sort()

        output = {
            "count": len(productList),
            "productList": productList
        }

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, self._stateFileName))
