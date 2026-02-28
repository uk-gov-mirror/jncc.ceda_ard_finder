from elasticsearch import Elasticsearch
import logging
import json

log = logging.getLogger("luigi-interface")


class CedaElasticsearchQueryer:
    client = None
    satellites = [
        'LANDSAT-5',
        'LANDSAT-7',
        'LANDSAT-8',
        'Sentinel-1A',
        'Sentinel-1A ARD',
        'Sentinel-1B',
        'Sentinel-1B ARD',
        'Sentinel-1C ARD',  # Not indexed in Elasticsearch - uses filesystem search
        'Sentinel-2A',
        'Sentinel-2A ARD',
        'Sentinel-2B',
        'Sentinel-2B ARD',
        'Sentinel-2C ARD',  # Not indexed in Elasticsearch - uses filesystem search
        'Sentinel-3A',
        'Sentinel-3B',
        'Sentinel-TMPDEPOSIT',
        'Sentinel-TMPDEPOSIT ARD'
    ]
    orbitDirection = ['asc', 'desc']

    def __init__(self, host, port) -> None:
        if CedaElasticsearchQueryer.client is None:
            CedaElasticsearchQueryer.client = Elasticsearch(f'https://{host}:{port}')
        self.arguments = []

    def getCoordinateList(self, coordinates):
        coordinate_list = []
        points = coordinates.split(',')
        if len(points) < 4:
            raise Exception("Invalid polygon WKT format, number of points in a ring "
                            "must be >= 4")
        for point in points:
            point = point.strip()
            try:
                long, lat = point.split(" ")
            except ValueError:
                raise Exception("Invalid polygon WKT format")
            coordinate_list.append([float(long), float(lat)])
        return coordinate_list

    def addGeoFilter(self, spatialOperation, wkt):
        so = spatialOperation.lower()

        if so not in ['intersects', 'disjoint', 'contains', 'within']:
            raise Exception(
                "{} is not a valid spatial operation. Only the operations 'intersects', 'disjoint', 'contains', 'within' are allowed".format(spatialOperation))

        geometry = wkt.upper()

        if not geometry.startswith('POLYGON'):
            raise Exception("Invalid geometry, it must be a POLYGON")
        try:
            rings = (geometry.split('POLYGON')[1].strip().split('('))
            outer_ring = rings[2].split(')')[0]
            if len(rings) > 3:
                inner_ring = rings[3].split(')')[0]
            else:
                inner_ring = None
        except (ValueError, IndexError):
            raise Exception("Invalid polygon WKT format, {}".format(geometry))

        polygon = []
        polygon.append(self.getCoordinateList(outer_ring))
        if inner_ring is not None:
            polygon.append(self.getCoordinateList(inner_ring))

        query = {
            "geo_shape": {
                "spatial.geometries.search": {
                    "shape": {
                        "type": "polygon", "coordinates": polygon
                    },
                    "relation": so
                }
            }
        }
        self.arguments.append(query)

        return query

    def addDateFilter(self, start_date, end_date):
        self.arguments.append({
            "range": {
                "temporal.start_time": {
                    "from": start_date.strftime('%Y-%m-%d'),
                    "to": end_date.strftime('%Y-%m-%d')
                }
            }
        })

    def addSatelliteFilters(self, satellites):
        for satellite in satellites:
            if satellite not in self.satellites:
                raise ValueError(f'Satellite not in allowed list, found {satellite}')

        self.arguments.append({
            "terms": {
                "misc.platform.Satellite.raw": satellites
            }
        })

    def addOrbitFilter(self, orbit):
        self.arguments.append({
            "regexp": {
                "file.data_file": {
                    "value": f".*_(ORB)?{orbit}_.*",
                    "case_insensitive": "true"
                }
            }
        })

    def addOrbitDirectionFilter(self, orbitDirection):
        direction = orbitDirection.lower()
        if direction not in self.orbitDirection:
            raise Exception(f"Orbit direction not in allowed list, found {direction}")

        self.arguments.append({
            "wildcard": {
                "file.data_file": {
                    "value": f"*_{direction}_*",
                    "case_insensitive": "true"
                }
            }
        })

    def addArdFilter(self, ardFile):
        self.arguments.append({
            "wildcard": {
                "file.data_file": {
                    "value": f"{ardFile}*",
                    "case_insensitive": "true"
                }
            }
        })

    def buildQuery(self, start, size):
        return {
            "_source": {
                "includes": [
                    "file.filename",
                    "file.data_file",
                    "file.location",
                    "file.directory",
                    "temporal.start_time",
                    "temporal.end_time"
                ]
            },
            "from": start,
            "size": size,
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": self.arguments
                        }
                    }
                }
            },
            "sort": [{"temporal.start_time": "asc"}],  # need to specify an order to ensure immutability in paged results
            "track_total_hits": "true"
        }

    def query(self, index, start, size):
        requestBody = self.buildQuery(start, size)
        log.info(f'Sending request: {json.dumps(requestBody)}')

        return self.client.search(
            index=index,
            body=requestBody
        )
