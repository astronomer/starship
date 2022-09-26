def get_version():
    import os
    import configparser
    config = configparser.RawConfigParser()
    config.read_file(open(r'setup.cfg'))
    version = config.get('metadata', 'version')
    os.environ["VERSION"] = version
    return version

if __name__ == '__main__':
    version = get_version()
    print(version)
