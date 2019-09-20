from configparser import ConfigParser # from psycopg2
import psycopg2 
# code  adapted from https://www.dev2qa.com/how-to-connect-postgresql-server-use-configuration-file-in-python/

'''
        uses saved config info to establish a database connection 
        config_file_path : Is the configuration file saved path, here database.ini in this example
        section_name : the first line, in [], in the config file 
    '''
def get_connection_by_config(config_file_path, section_name):
    if(len(config_file_path) > 0 and len(section_name) > 0):
            # Create an instance of ConfigParser class, read
            config_parser = ConfigParser()
            config_parser.read(config_file_path)
            # if the configuration file contains the provided section name.
            if(config_parser.has_section(section_name)):
                # read the options of the section. the config_params is a list object.
                config_params = config_parser.items(section_name)
                # so we need below code to convert the list object to a python dictionary object.
                db_conn_dict = {}
                for config_param in config_params:
                    key = config_param[0]
                    value = config_param[1]
                    db_conn_dict[key] = value
                # get connection object using above dictionary object.
                conn = psycopg2.connect(**db_conn_dict)
                return conn
