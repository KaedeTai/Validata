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

config.yaml now supports these commands:

Pattern matching

    all: ^(?P<name>\w+)\t(?P<class>\w+)\t(?P<attr>.*)$

Find all those match the pattern for further validation

    attr: ?(?P<key>\w+)=(?P<value>\w+)

Multiple rules for a single line

    attr:
    - ^\w+=\w+(,\w+=\w+)*
    - (^|,)language=(english|中文)(,|$)

Split by pattern and validate each part

    split:
      by: (\t|,)
      as: (?P<key>\w+)=(?P<value>\w+)

Count the number of a value

    key:
      count: key

Count the numbers of each group of value

    key:
      group: key

Valid range and alert range defined in absolute number, relative or percentage differences

    __size:
      valid: -100, +1000
      alert: 10%

Include pre-defined rules

    __include: qlas.yaml

Notify by email

    __email: kaedetai@gmail.com
