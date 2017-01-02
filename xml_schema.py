""" Determine the structure of an xml file by traversing the document tree. The
implicit assumption with the input xml file is that it contains entries in some
sort of relational form. This means that the data is implicitly structured in
some way.

First, a data structure should be constructed in order to enumerate all the
possible fields for an entry. It should contain the lower and upper bounds on
the count of entries. Associated with each key should be a set of all possible
values to be used to determine the type in our new data format.

This intermediate data structure can be used to generate an output format. This
data-structure should also be available in pretty printing form for manual
verification. Ultimately, this script should lead into some form of schema
validation.
"""
import argparse
import sys

def main(argv):
    """ Entry point into the schema generation tool """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p' '--path', help="Path to xml dump")
    args = parser.parse_args(argv)

if __name__ == '__main__':
    main(sys.argv[1:])
