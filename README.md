Validata
========
Validata is designed for line-based data pipeline or general purpose data validation.
For more information please contact kaede@yahoo-inc.com.

Usage:

    $ python validata.py config.yaml datafile.ext [...]

or

    from validata import *
    validata = Validata('config.yaml')
    validata.check_file('data.txt')

