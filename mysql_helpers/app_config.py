from os.path import (dirname as _dirname, abspath as _abspath)


def app_root_folder_path():
    """Get full path of project"""
    # https://stackoverflow.com/questions/25389095/python-get-path-of-root-project-structure/40227116
    root_dir = _dirname(_dirname(_abspath(__file__)))
    return root_dir


if __name__ == '__main__':
    print(app_root_folder_path())
