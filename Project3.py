#!/usr/bin/env python



import xml.etree.cElementTree as ET
import pprint
import pymongo
import re
import codecs
import json
from pymongo import MongoClient

osm_file = '/Users/Stevenstuff/Downloads/raleigh_north-carolina.osm'

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


client = MongoClient("mongodb://localhost:27017")
data = process_map(osm_file, True)
