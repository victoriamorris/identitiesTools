# identitiesTools
Tools for reconciling ISNI, VIAF and other identifiers for agents

## Requirements

Requires the regex module from https://bitbucket.org/mrabarnett/mrab-regex. The built-in re module is not sufficient.

Also requires fuzzywuzzy, glob, sqlite3.

## Installation

From GitHub:

    git clone https://github.com/victoriamorris/identitiesTools
    cd nielsenTools

To install as a Python package:

    python setup.py install
    
To create stand-alone executable (.exe) files for individual scripts:

    python setup.py py2exe 
    
Executable files will be created in the folder \dist, and should be copied to an executable path.

Both of the above commands can be carried out by running the shell script:

    compile_identities.sh

## Usage

### Running scripts

The following scripts can be run from anywhere, once the package is installed:

#### identities_graph

Parses input files and adds information about identifiers to an SQL database.
    
    Usage: identities_graph [options]
    
    Options:
	EXACTLY ONE of the following:
	
		Options for adding data to the database:
		-l	Parse VIAF Links table
		-v	Parse VIAF files
		-n	Parse NACO files
		-t	Parse TSV files		
		-q	Parse list of ISBN eQuivalences
		
		Options for reporting:
		-f	Find name matches
		-x	eXport graph
		
		Other options:
		-i	build Indexes
		-e	Exit program
		--help	Show help message and exit.
      
The SQL database must be named identities_graph.db, and must be present in the same folder as the folder in which the script is run.

The VIAF links table must be saved in the folder ./Data/VIAF, with a filename of the form viaf*-links.txt

VIAF files must be saved in the folder ./Data/VIAF, with filenames of the form viaf*-marc21.lex

NACO files must be saved in the folder ./Data/NACO, with filenames of the form naco*.lex

TSV files must be saved in the folder ./Data/TSV, with filenames of the form *.tsv

Lists of ISBN equivalences must be saved in the folder ./Data/ISBN, with filenames of the form *.txt


When searching for name matches, TSV files must be saved in the folder ./Data/TSV, with filenames of the form *.tsv

Headings in TSV files must be one of:
* isbn
* string
* isni
* naco
* viaf
* penguin
* harpercollins
* randomhouse
