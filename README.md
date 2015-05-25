Validata
========
Validata is designed for line-based data pipeline or general purpose data validation.
For more information please contact kaedetai@gmail.com.

Usage:

    $ python validata.py config.yaml data.txt [...]

or

    from validata import *
    validata = Validata('config.yaml')
    validata.check_file('data.txt')

