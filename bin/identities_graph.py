#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================
#       Set-up
# ====================

# Import required modules
from collections import OrderedDict
import gc
import getopt
import locale
import sys
from identities_tools.graph_tools import *

# Set locale to assist with sorting
locale.setlocale(locale.LC_ALL, '')

# Set threshold for garbage collection (helps prevent the program run out of memory)
gc.set_threshold(400, 5, 5)

__author__ = 'Victoria Morris'
__license__ = 'MIT License'
__version__ = '1.0.0'
__status__ = '4 - Beta Development'

# ====================
#     Constants
# ====================


OPTIONS = OrderedDict([
    ('F', 'Find name matches'),
    ('L', 'Parse VIAF Links table'),
    ('N', 'Parse NACO files'),
    ('T', 'Parse TSV files'),
    ('V', 'Parse VIAF files'),
    ('Q', 'Parse list of ISBN eQuivalences'),
    ('I', 'build Indexes'),
    ('X', 'eXport graph'),
    ('E', 'Exit program'),
])

ACTIONS = {
    'F': find_name_matches,
    'L': parse_viaf,
    'N': parse_marc,
    'T': parse_tsv,
    'V': parse_marc,
    'Q': parse_isbns,
    'I': index,
    'X': export_graph,
    'E': sys.exit,
}


# ====================
#       Classes
# ====================


class OptionHandler:

    def __init__(self, selected_option=None):
        self.selection = None
        if selected_option in OPTIONS:
            self.selection = selected_option
        else: self.get_selection()

    def get_selection(self):
        print('\n----------------------------------------')
        print('\n'.join('{}:\t{}'.format(o, OPTIONS[o]) for o in OPTIONS))
        self.selection = input('Choose an option:').upper()
        while self.selection not in OPTIONS:
            self.selection = input('Sorry, your choice was not recognised. '
                                   'Please enter one of {}:'.format(', '.join(opt for opt in OPTIONS))).upper()

    def set_selection(self, selected_option):
        if selected_option in OPTIONS:
            self.selection = selected_option

    def execute(self):

        if self.selection not in OPTIONS:
            self.get_selection()

        date_time_message(message(OPTIONS[self.selection]))

        if self.selection == 'E':
            sys.exit()

        if self.selection == 'N':
            parse_marc(record_type='NACO')
        elif self.selection == 'V':
            parse_marc(record_type='VIAF')
        else: ACTIONS[self.selection]()
        self.selection = None
        return


# ====================
#      Functions
# ====================

def usage():
    """Function to print information about the program"""
    print('Correct syntax is:')
    print('identities_graph [options]')
    print('\nOptions')
    print('EXACTLY ONE of the following:')
    for o in OPTIONS:
        print('    -{}    {}'.format(o.lower(), OPTIONS[o]))
    print('ANY of the following:')
    print('    --help    Display this message and exit')
    exit_prompt()


# ====================
#      Main code
# ====================


def main(argv=None):
    if argv is None:
        name = str(sys.argv[1])

    selected_option = None

    print('========================================')
    print('identities_graph')
    print('========================================')

    try: opts, args = getopt.getopt(argv, ''.join(o.lower() for o in OPTIONS), ['help'])
    except getopt.GetoptError as err:
        exit_prompt('Error: {}'.format(str(err)))
    for opt, arg in opts:
        if opt == '--help': usage()
        elif opt.upper().strip('-') in OPTIONS:
            selected_option = opt.upper().strip('-')
        else: exit_prompt('Error: Option {} not recognised'.format(opt))

    # Check that files exist
    if not os.path.isfile(DATABASE_PATH):
        exit_prompt('Error: The file {} cannot be found'.format(DATABASE_PATH))

    option = OptionHandler(selected_option)

    while option.selection:
        option.execute()
        option.get_selection()

    date_time_exit()


if __name__ == '__main__':
    main(sys.argv[1:])
