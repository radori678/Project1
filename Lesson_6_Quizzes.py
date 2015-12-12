#!/usr/bin/env python

import xml.etree.cElementTree as ET
from collections import defaultdict
import pprint
import re
import codecs
import json


# iterative parsing
def count_tags(filename):
    osm_file = open(filename, 'r')
    tags_list = {}
    for event, element in ET.iterparse(osm_file, events=('start', 'end')):
        if event == 'start':
            if element.tag in tags_list.keys():
                tags_list[element.tag] += 1
            else:
                tags_list[element.tag] = 1
        elif event == 'end':
            element.clear()
    return tags_list


# tag types
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    if element.tag == "tag":
        k = element.attrib['k']
        if len(lower.findall(k)) > 0:
            keys['lower'] += 1
        elif len(lower_colon.findall(k)) > 0:
            keys['lower_colon'] += 1
        elif len(problemchars.findall(k)) > 0:
            keys['problemchars'] += 1
        else:
            keys['other'] += 1
            print k

    return keys


def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys


# exploring users
def get_user(element):
    if element.tag == 'node' or element.tag == 'way' or element.tag == 'relation':
        uid = element.get('uid')
    return uid


def process_map(filename):
    users = set()
    for _, element in ET.iterparse(filename):
        if element.tag == 'node' or element.tag == 'way' or element.tag == 'relation':
            if get_user(element):
                users.discard('')
                users.add(element.get('uid'))
            else:
                pass

    return users


# improving street names
OSMFILE = "example.osm"
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons"]

mapping = {"St": "Street",
           "St.": "Street",
           "Ave": "Avenue",
           "Rd.": "Road"
           }


def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])

    return street_types


def update_name(name, mapping):
    m = street_type_re.search(name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            print street_type
            name = re.sub(street_type_re, mapping[street_type], name)
    return name
