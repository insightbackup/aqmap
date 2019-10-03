from configparser import ConfigParser

# code adapted from 
# https://www.dev2qa.com/how-to-connect-postgresql-server-use-configuration-file-in-python/

#  uses info in config file to get API keys for OpenCage and MapBox
#  config_file_path : Is the configuration file saved path, the configuration 
#  file is database.ini in this example, and it is saved in the same path of PostgresqlManager.py file.
#  section_name :  the section name in above configuration file. 
#  The options in this section record the postgresql database server connection info.

# config_file_path is currently just 'geocode.ini' and section_name is 'geocode_keys'
def get_geocode_keys(config_file_path, section_name):
    if(len(config_file_path) > 0 and len(section_name) > 0):
            # Create an instance of ConfigParser class.
            config_parser = ConfigParser()
            # read the configuration file.
            config_parser.read(config_file_path)
            # if the configuration file contains the provided section name.
            if(config_parser.has_section(section_name)):
                # read the options of the section. the config_params is a list object.
                config_params = config_parser.items(section_name)
                # so we need below code to convert the list object to a python dictionary object.
                # define an empty dictionary.
                geocode_dict = {}
                # loop in the list.
                for config_param in config_params:
                    # get options key and value.
                    key = config_param[0]
                    value = config_param[1]
                    # add the key value pair in the dictionary object.
                    geocode_dict[key] = value
                # get connection object use above dictionary object.
                return(geocode_dict['opencage_key'], geocode_dict['mapbox_key'])
