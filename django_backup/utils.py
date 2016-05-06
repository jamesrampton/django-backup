import calendar
from datetime import datetime, timedelta
import os
import re
from django.conf import settings
from django.core.management import BaseCommand, CommandError
from pysftp import Connection

try:
    from urllib.parse import splitport
except ImportError:
    from urllib import splitport

DEFAULT_PORT = 22
TIME_FORMAT = '%Y%m%d-%H%M%S'
GOOD_RSYNC_FLAG = '__good_backup'
regex = re.compile(r'(\d){8}-(\d){6}')


def is_db_backup(filename):
    return filename.startswith('backup_')


def is_media_backup(filename):
    return filename.startswith('dir_')


def is_backup(filename):
    return is_db_backup(filename) or is_media_backup(filename)


def get_date(filename):
    """
    Given the name of the backup file, return the datetime it was created.
    """
    result = regex.search(filename)
    date_str = result.group()
    d = datetime.strptime(date_str, TIME_FORMAT)
    return d


def between_interval(filename, start, end):
    """
    Given a filename and an interval, tell if it's between the interval
    """
    d = get_date(filename)
    if start < d <= end:
        return True
    else:
        return False


def decide_remove(backups, config):
    """
    Given a list of backup filenames and setttings, decide the files to be deleted.
    """
    reserve = []
    remove_list = []
    reserve += reserve_interval(backups, 'monthly', config['monthly'])
    reserve += reserve_interval(backups, 'weekly', config['weekly'])
    reserve += reserve_interval(backups, 'daily', config['daily'])
    if config.get('hourly', 0):
        reserve += reserve_interval(backups, 'hourly', config.get('hourly', 0))
    for i in backups:
        if i not in reserve:
            remove_list.append(i)
    return remove_list


def reserve_interval(backups, type, num):
    """
    Given a list of backup filenames, interval type(monthly, weekly, daily),
    and the number of backups to keep, return a list of filenames to reserve.
    """

    result = []
    now = datetime.now()

    if type == 'monthly':
        delta = timedelta(30)
        interval_end = datetime(now.year, now.month, 1)  # begin of the month
        interval_start = interval_end - delta

    elif type == 'weekly':
        delta = timedelta(7)
        weekday = calendar.weekday(now.year, now.month, now.day)
        weekday_delta = timedelta(weekday)
        interval_end = datetime(now.year, now.month, now.day) - weekday_delta  # begin of the week
        interval_start = interval_end - delta

    elif type == 'daily':
        delta = timedelta(1)
        interval_end = datetime(now.year, now.month, now.day) + delta
        interval_start = interval_end - delta

    elif type == 'hourly':
        delta = timedelta(minutes=60)
        interval_end = datetime(now.year, now.month, now.day, now.hour) + delta
        interval_start = interval_end - delta

    else:
        raise CommandError("Unknown backup interval")

    for i in range(1, num + 1):
        for backup in backups:
            if between_interval(backup, interval_start, interval_end):
                result.append(backup)
                break  # reserve only one backup per interval
        interval_end = interval_end - delta
        interval_start = interval_start - delta
    return result


def get_setting(key, default):
    """
    This helper abstracts away some behaviour we encountered when working
    with setting-overrides inside of tests. Here the settings object is already
    patched to always return a None if a non-existing attribute is requested
    hence a normal getattr no longer works.

    Use this when you expect a None value to represent a not-existing
    setting.
    """
    value = getattr(settings, key, None)
    if value is None:
        return default
    return value


class BaseBackupCommand(BaseCommand):
    def __init__(self):

        super(BaseBackupCommand, self).__init__()

        try:
            self.engine = settings.DATABASES['default']['ENGINE']
            self.db = settings.DATABASES['default']['NAME']
            self.user = settings.DATABASES['default']['USER']
            self.passwd = settings.DATABASES['default']['PASSWORD']
            self.host = settings.DATABASES['default']['HOST']
            self.port = settings.DATABASES['default']['PORT']
        except NameError:
            self.engine = settings.DATABASE_ENGINE
            self.db = settings.DATABASE_NAME
            self.user = settings.DATABASE_USER
            self.passwd = settings.DATABASE_PASSWORD
            self.host = settings.DATABASE_HOST
            self.port = settings.DATABASE_PORT

        self.backup_dir = get_setting('BACKUP_LOCAL_DIRECTORY', os.getcwd())
        self.remote_dir = get_setting('BACKUP_FTP_DIRECTORY', '')
        self.remote_restore_dir = get_setting('RESTORE_FROM_FTP_DIRECTORY', self.remote_dir)
        self.ftp_server = get_setting('BACKUP_FTP_SERVER', '')
        self.ftp_username = get_setting('BACKUP_FTP_USERNAME', '')
        self.ftp_password = get_setting('BACKUP_FTP_PASSWORD', '')
        self.private_key = getattr(settings, 'BACKUP_FTP_PRIVATE_KEY', None)
        self.directory_to_backup = getattr(settings, 'DIRECTORY_TO_BACKUP', settings.MEDIA_ROOT)
        self.rsyncnosymlink = getattr(settings, 'BACKUP_DISABLE_RSYNC_SYMLINK', False)  # Disable symlink directories when doing rsync backup
        # convert remote dir to absolute path if necessary
        if not self.remote_dir.startswith('/') and self.ftp_username:
            self.remote_dir = '/home/%s/%s' % (self.ftp_username, self.remote_dir)

    def get_connection(self):
        """
        Get the ssh connection to the remote server.
        """
        if getattr(self, '_ssh', None):
            return self._ssh

        conn_config = {
            'host': self.ftp_server,
            'username': self.ftp_username,
        }

        host, port = splitport(conn_config['host'])

        if port is None:
            port = DEFAULT_PORT
        else:
            port = int(port)

        conn_config.update({
            'host': host,
            'port': port,
        })

        if self.private_key:
            conn_config['private_key'] = self.private_key
            conn_config['password'] = None
        else:
            conn_config['password'] = self.ftp_password

        self._ssh = Connection(**conn_config)
        return self._ssh

    def close_connection(self):
        if getattr(self, '_ssh', None):
            self._ssh.close()
