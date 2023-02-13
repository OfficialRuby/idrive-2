import logging
import os
import boto3
import datetime
from dotenv import load_dotenv
load_dotenv()
LOG_FILE = "media.log"

BACKUP_PATH = os.getcwd()
ENDPOINT_URL = os.getenv("IDRIVE_ENDPOINT")
BUCKET_NAME = 'test-bucket'
ROOT_DIR = 'media'

logging.basicConfig(filename='logs/app.log', filemode='w', format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.DEBUG)


class IdriveBackup:
    def __init__(self, endpoint_url, bucket_name, backup_path=BACKUP_PATH) -> None:
        self.ENDPOINT_URL = endpoint_url
        self.BUCKET_NAME = bucket_name
        self.BACKUP_PATH = backup_path
        try:
            self.client = boto3.client("s3", endpoint_url=self.ENDPOINT_URL)
        except Exception as e:
            # raise e
            logging.exception("Error occured")

    def remove_base_path(self, string, root_dir=ROOT_DIR):
        file_dir = string.replace(self.BACKUP_PATH, '')
        return f"media/{file_dir}"

    def create_backup(self):
        for media in self._get_new_files():
            with open(str(media), 'rb') as f:
                self.client.put_object(
                    Key=self.remove_base_path(str(media)),
                    Body=f,
                    Bucket=self.BUCKET_NAME
                )
        self._log_backup()

    def perform_backup(self):
        if self.has_new_file:
            pass

    def _get_new_files(self):
        "Get list of newly added media"
        new_files = []
        old_media = self._get_last_backup()
        current_media = self._get_current_files()

        for media in current_media:
            if media not in old_media:
                new_files.append(media)
        return new_files

    def _get_last_backup(self):
        "Convert last backup into a list from log history"
        last_backup = []
        with open(LOG_FILE, 'r') as f:
            for item in f.readlines():
                item = item.strip()
                last_backup.append(item)
        return last_backup

    @property
    def has_new_file(self):
        if len(self._get_new_files()) > 0:
            return True
        return False

    def _log_backup(self):
        with open(LOG_FILE, 'w') as f:
            for (root, dirs, files) in os.walk(self.BACKUP_PATH):
                for media in files:
                    f.writelines(f'{os.path.join(root, media)}\n')

    def _get_current_files(self):
        current_media = []
        for (root, dirs, files) in os.walk(self.BACKUP_PATH):
            for media in files:
                media_file = os.path.join(root, media).strip()
                current_media.append(media_file)
        return current_media


# start_time = datetime.datetime.now()
# idrive = IdriveBackup(endpoint_url=ENDPOINT_URL, bucket_name=BUCKET_NAME)
# if idrive.has_new_file:
#     idrive.create_backup()

# print(f'Program completed in {datetime.datetime.now()-start_time} ')
