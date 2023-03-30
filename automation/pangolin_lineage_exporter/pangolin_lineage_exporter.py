import logging
import sys
import time
from datetime import datetime
from os import environ
from os import path
from pathlib import Path, PurePath

import psycopg2
from psycopg2.extensions import AsIs
from webdav4.fsspec import WebdavFileSystem

if 'DB_HOST' not in environ or 'DB_DBNAME' not in environ or 'DB_USER' not in environ or 'DB_PASSWORD' not in environ \
        or 'WEBDAV_BASE_URL' not in environ or 'WEBDAV_USER' not in environ or 'WEBDAV_PASSWORD' not in environ \
        or 'WEBDAV_EXPORT_DIRECTORY' not in environ:
    raise Exception('Setup all required environment variables.')

EXPORT_DIRECTORY = environ['WEBDAV_EXPORT_DIRECTORY']
CONNECTION_STRING = f"host='{environ['DB_HOST']}' dbname='{environ['DB_DBNAME']}' user='{environ['DB_USER']}' " + \
                    f"password='{environ['DB_PASSWORD']}'"

SLEEP_IN_SECONDS = int(environ.get('SLEEP_IN_SECONDS', 60*60*2))
SLEEP_IN_SECONDS_AFTER_FAILURE = int(environ.get('SLEEP_IN_SECONDS_AFTER_FAILURE', 20))
EXPORT_IMMEDIATELY_AFTER_X_DAYS = int(environ.get('EXPORT_IMMEDIATELY_AFTER_X_DAYS', 8))
MAX_LINES_PER_FILE = int(environ.get('MAX_LINES_PER_FILE', 100))

temporary_file_name = 'tmp.csv'
filesystem = WebdavFileSystem(environ['WEBDAV_BASE_URL'], auth=(environ['WEBDAV_USER'], environ['WEBDAV_PASSWORD']))


class MultipleException(Exception):
    exceptions = []

    def __init__(self, exceptions, *args, **kwargs):
        self.exceptions = exceptions
        super(MultipleException, self).__init__(*args, **kwargs)


def sleep(seconds):
    sys.stdout.flush()
    time.sleep(seconds)


def get_last_chunk_number(lab):
    export_sub_directory = f"{EXPORT_DIRECTORY}/{lab}"
    filesystem.mkdir(export_sub_directory, create_parents=True)
    directories = [path.basename(item['name']) for item in filesystem.ls(export_sub_directory, detail=True)]

    if len(directories) == 0:
        return 0

    export_sub_directory += f"/{max(directories)}"
    files = [path.basename(item['name']) for item in filesystem.ls(export_sub_directory, detail=True)]

    if len(files) == 0:
        return 0

    file = max(files, key=lambda f: int(f.split('.')[0]))
    last_chunk_number = int(file.split('.')[0])

    return last_chunk_number


def export_data(lab, current_chunk_number):
    exceptions = []
    connection = None

    try:
        while True:
            connection = psycopg2.connect(CONNECTION_STRING)
            cursor = connection.cursor()
            sql = cursor.mogrify(
                "COPY (SELECT * FROM pangolin_lineage(NULL::%s, %s, %s::SMALLINT, %s)) " +
                "TO STDOUT WITH (FORMAT CSV, DELIMITER ',', HEADER)",
                (AsIs(lab), current_chunk_number, EXPORT_IMMEDIATELY_AFTER_X_DAYS, MAX_LINES_PER_FILE))

            with open(temporary_file_name, 'bw+') as file:
                cursor.copy_expert(sql, file)
                connection.commit()
                connection.close()
                file.close()

                if cursor.rowcount > 0:
                    current_chunk_number += 1
                    export_sub_directory = f"{EXPORT_DIRECTORY}/{lab}/{datetime.now().strftime('%Y-%m')}"
                    filesystem.makedirs(export_sub_directory, exist_ok=True)
                    export_file = f"{export_sub_directory}/{current_chunk_number}.csv"
                    # WebDAV upload is an atomic operation.
                    filesystem.put_file(lpath=PurePath(temporary_file_name), rpath=export_file)
                    print(f"Exported the file: {export_file}")
                    continue

                break
        return current_chunk_number
    except Exception as e1:
        exceptions.append(e1)
        try:
            if connection:
                connection.close()
        except Exception as e2:
            exceptions.append(e2)
    finally:
        try:
            Path(temporary_file_name).unlink(missing_ok=True)
        except Exception as e3:
            exceptions.append(e3)

        if len(exceptions) > 0:
            raise MultipleException(exceptions)


def main():
    print('Initializing the database')
    while True:
        connection = None
        try:
            connection = psycopg2.connect(CONNECTION_STRING)
            cursor = connection.cursor()
            cursor.execute(open('init.sql', 'r').read())
            connection.commit()
            connection.close()

            break
        except Exception as e1:
            logging.warning(e1, exc_info=True)
            try:
                if connection:
                    connection.close()
            except Exception as e2:
                logging.warning(e2, exc_info=True)
            sleep(SLEEP_IN_SECONDS_AFTER_FAILURE)

    while True:
        print('Collecting the last chunk numbers')
        while True:
            try:
                current_eoc_id = get_last_chunk_number('eoc')
                current_imv_id = get_last_chunk_number('imv')

                break
            except Exception as e:
                logging.warning(e, exc_info=True)
                sleep(SLEEP_IN_SECONDS_AFTER_FAILURE)

        print('Exporting the data chunks')
        while True:
            try:
                current_eoc_id = export_data('eoc', current_eoc_id)
                current_imv_id = export_data('imv', current_imv_id)

                sleep(SLEEP_IN_SECONDS)
            except MultipleException as e:
                for exception in e.exceptions:
                    logging.warning(exception, exc_info=True)
                sleep(SLEEP_IN_SECONDS_AFTER_FAILURE)

                break


if __name__ == "__main__":
    main()
