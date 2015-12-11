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


def fix_street(street_name):
    street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

    expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
                "Trail", "Parkway", "Commons", "Plaza", "Center", "Bypass", "Circle", "Way", "54", "55",
                "Suite", "Hill", "Hills", "Extension", "West", "East", "North", "South", "Fork"]

    mapping = {"St": "Street",
               "St.": "Street",
               "st": "Street",
               "Ave.": "Avenue",
               "Ave": "Avenue",
               "avenue": "Avenue",
               "AVE": "Avenue",
               "ave": "Avenue",
               "Blvd.": "Boulevard",
               "Blvd": "Boulevard",
               "Rd": "Road",
               "Rd.": "Road",
               "CIrcle": "Circle",
               "Dr.": "Drive",
               "Dr": "Drive",
               "Pl": "Place",
               "Pl.": "Place",
               "Ext": "Extension",
               "Ext.": "Extension"}
    m = street_type_re.search(street_name)
    if m and m.group() not in expected and m.group in mapping.keys():
        return street_name.replace(m.group(), mapping[m.group()])
    else:
        return street_name


def fix_postcode(postcode):
    # search postcode for five digits, hyphen, four digits
    postcode_type_re = re.compile(r'^\d{5}-\d{4}$')
    n = postcode_type_re.search(postcode)
    if n:
        return postcode.split("-")[0]
    else:
        return postcode


def fix_housenumber(housenumber):
    # search housenumbers for one to many digits, hyphen, one to many letters (ignore case)
    housenumber_re = re.compile(r'^\d+-[a-zA-Z]+$')
    h = housenumber_re.search(housenumber)
    if h:
        housenumber = housenumber.replace("-", "")
        housenumber = housenumber.upper()
    return housenumber


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
                if add_keys[1] == "housenumber":
                    address[add_keys[1]] = fix_housenumber(value)
                elif add_keys[1] == "postcode":
                    address[add_keys[1]] = fix_postcode(value)
                elif add_keys[1] == "street" and len(add_keys) == 2:
                    address[add_keys[1]] = fix_street(value)
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


def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db


def make_pipeline():
    # Group and sort postcodes by descending count
    # pipeline = [{"$match": {"address.postcode": {"$exists": 1}}},
    # {"$group": {"_id": "$address.postcode", "count": {"$sum": 1}}},
    # {"$sort": {"count": -1}}]

    # Sift out house numbers with hyphens and letters in the housenumbers field
    # pipeline = [{"$project": {"address.housenumber": {"$substr": ["$address.housenumber", 0, -1]}}},
    # {"$sort": {"address.housenumber": 1}},
    # {"$match": {"address.housenumber": {"$regex": '[-a-zA-Z]'}}}]

    return pipeline


def aggregate(db, pipeline):
    return [doc for doc in db["raleigh_north-carolina.osm"].aggregate(pipeline)]


if __name__ == '__main__':
    process_map(osmfile, True)
    db = get_db('raleigh')
    # pipeline = make_pipeline()
    # result = aggregate(db, pipeline)

    # pprint.pprint(result)
