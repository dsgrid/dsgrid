# -*- coding: utf-8 -*-
"""
Module to handle parsing configuration parameters from .toml files
"""
import collections
import toml


def flatten_dict(d, parent_key="", sep="/"):
    """
    Flattent nested dictionary.
    Combine keys using given seperator sep
    Parameters
    ----------
    d : 'dict'
        Nested dictionary
    parent_key : 'str'
        parent key from upper levels
    sep : 'str'
        Seperator to use when combining keys
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class TOMLParser:
    """
    Class object to parse in configuration parameters from .toml files
    """

    def __init__(self, toml_file):
        """
        Parse .toml into a dictionary and flatten it
        Parameters
        ----------
        toml_file : 'str'
            Path to config .toml file
        """
        self._toml = toml.load(toml_file)
        self._flat_dict = flatten_dict(self._toml)

    def __getitem__(self, section_name):
        """
        Extract given section from toml dictionary
        Parameters
        ----------
        section_name : 'str'
            toml section name
        Returns
        -------
        'dict'
            Section from toml file containing subsections or key: value pairs
        """
        return self._toml[section_name]

    def get_value(self, *keys):
        """
        Decode config entry converting missing or 'None' entires to None
        Parameters
        ----------
        keys : 'string'
            keys for config entry to be read
        Returns
        ---------
        'object'
            decoded config entry using eval
        """
        key = "/".join(keys)
        val = self._flat_dict[key]
        if val in ["None", "none", "NONE", "Null", "null", "NULL", ""]:
            val = None

        return val
