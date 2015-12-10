#!/usr/bin/env python



import xml.etree.cElementTree as ET
import pprint
import pymongo
import re
import codecs
import json
from pymongo import MongoClient
from collections import defaultdict

osmfile = '/Users/Stevenstuff/Downloads/raleigh_north-carolina.osm'

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = ["version", "changeset", "timestamp", "user", "uid"]


def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way":
        node["id"] = element.attrib["id"]
        node["type"] = element.tag
        node["visible"] = element.get("visible")
        created = {}
        created["version"] = element.attrib["version"]
        created["changeset"] = element.attrib["changeset"]
        created["timestamp"] = element.attrib["timestamp"]
        created["user"] = element.attrib["user"]
        created["uid"] = element.attrib["uid"]
        node["created"] = created
        if "lat" in element.keys() and "lon" in element.keys():
            node["pos"] = [float(element.attrib["lat"]), float(element.attrib["lon"])]
        else:
            node["pos"] = None

        address = {}
        for tag in element.iter("tag"):
            tagname = tag.attrib["k"]
            value = tag.attrib["v"]
            if "addr" in tagname:
                add_keys = tagname.split(":")
                if add_keys[1] == "housenumber" or add_keys[1] == "postcode":
                    address[add_keys[1]] = value
                elif add_keys[1] == "street" and len(add_keys) == 2:
                    address[add_keys[1]] = value
            elif tagname == "amenity":
                node["amenity"] = value
            elif tagname == "cuisine":
                node["cuisine"] = value
            elif tagname == "name":
                node["name"] = value
            elif tagname == "phone":
                node["phone"] = value
        if len(address) > 0:
            node["address"] = address

        if element.tag == "way":
            node_refs = []
            for nd in element.iter("nd"):
                if "ref" in nd.keys():
                    node_refs.append(nd.get("ref"))
            node["node_refs"] = node_refs
        return node
    else:
        return None


def process_map(file_in, pretty=False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2) + "\n")
                else:
                    fo.write(json.dumps(el) + "\n")

    return data


street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

street_types = defaultdict(int)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons", "Plaza", "Center", "Bypass", "Circle", "Way", "Highway 54"]

# UPDATE THIS VARIABLE
mapping = {"St": "Street",
           "St.": "Street",
           "st": "Street",
           "Ave.": "Avenue",
           "Ave": "Avenue",
           "avenue": "Avenue",
           "AVE": "Avenue",
           "ave": "Avenue",
           "Blvd.": "Boulevard",
           "Rd.": "Road",
           "CIrcle": "Circle"
           }


def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return elem.attrib['k'] == "addr:street"


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])

    return street_types


def update_name(streetname, mapping):
    updatednames_sec = []
    name = streetname.split()
    for index, word in enumerate(name):
        if word in mapping.keys():
            updatednames_sec.append(mapping[word])
        else:
            updatednames_sec.append(word)
    updatednames = ' '.join(updatednames_sec)
    return updatednames


def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db


def make_pipeline():
    pipeline = [{"$match": {"address.postcode": {"$exists": 1}}},
                {"$group": {"_id": "$address.postcode", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}]

    return pipeline


def aggregate(db, pipeline):
    return [doc for doc in db["raleigh_north-carolina.osm"].aggregate(pipeline)]


if __name__ == '__main__':
    db = get_db('raleigh')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)

    pprint.pprint(result)
