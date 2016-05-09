import os
import time
from copy import copy
from datetime import datetime
from optparse import make_option

from django_backup.utils import (
    GOOD_RSYNC_FLAG,
    TIME_FORMAT,
    decide_remove,
    is_db_backup,
    is_media_backup,
    is_backup,
    BaseBackupCommand,
)


from django.core.management.base import BaseCommand, CommandError
from django.core.mail import EmailMessage
from django.conf import settings
from django.db import connection


# Based on: http://www.djangosnippets.org/snippets/823/
# Based on: http://www.yashh.com/blog/2008/sep/05/django-database-backup-view/
class Command(BaseBackupCommand):
    
    option_list = BaseCommand.option_list + (
        make_option(
            '--email',
            default=None, dest='email',
            help='Sends email with attached dump file'
        ),
        make_option(
            '--ftp', '-f',
            action='store_true', default=False, dest='ftp',
            help='Backup file via FTP'
        ),
        make_option(
            '--compress', '-c',
            action='store_true', default=False, dest='compress',
            help='Compress dump file'
        ),
        make_option(
            '--directory', '-d',
            action='append', default=[], dest='directories',
            help='Destination Directory'
        ),
        make_option(
            '--zipencrypt', '-z',
            action='store_true', default=False,
            dest='zipencrypt', help='Compress and encrypt SQL dump file using zip'
        ),
        make_option(
            '--media', '-m',
            action='store_true', default=False, dest='media',
            help='Backup media dir'
        ),
        make_option(
            '--rsync', '-r',
            action='store_true', default=False, dest='rsync',
            help='Backup media dir with rsync'
        ),
        make_option(
            '--cleandb',
            action='store_true', default=False, dest='clean_db',
            help='Clean up surplus database backups'
        ),
        make_option(
            '--cleanmedia',
            action='store_true', default=False, dest='clean_media',
            help='Clean up surplus media backups'
        ),
        make_option(
            '--cleanrsync',
            action='store_true', default=False, dest='clean_rsync',
            help='Clean up broken rsync backups'
        ),
        make_option(
            '--nolocal',
            action='store_true', default=False, dest='no_local',
            help='Reserve local backup or not'
        ),
        make_option(
            '--deletelocal',
            action='store_true', default=False, dest='delete_local',
            help='Delete all local backups'
        ),
        make_option(
            '--cleanlocaldb',
            action='store_true', default=False, dest='clean_local_db',
            help='Clean up surplus local database backups'
        ),
        make_option(
            '--cleanremotedb',
            action='store_true', default=False, dest='clean_remote_db',
            help='Clean up surplus remote database backups'
        ),
        make_option(
            '--cleanlocalmedia',
            action='store_true', default=False, dest='clean_local_media',
            help='Clean up surplus local media backups'
        ),
        make_option(
            '--cleanremotemedia',
            action='store_true', default=False, dest='clean_remote_media',
            help='Clean up surplus remote media backups'
        ),
        make_option(
            '--cleanlocalrsync',
            action='store_true', default=False, dest='clean_local_rsync',
            help='Clean up local broken rsync backups'
        ),
        make_option(
            '--cleanremotersync',
            action='store_true', default=False, dest='clean_remote_rsync',
            help='Clean up remote broken rsync backups'
        ),
        make_option(
            '--application', '-a',
            action='append', default=[], dest='apps',
            help='Optionally only back up certain Django apps'
        ),
    )
    help = "Backup database. Only Mysql and Postgresql engines are implemented"

    def handle(self, *args, **kwargs):
        try:
            self._handle(*args, **kwargs)
        finally:
            self.close_connection()

    def _handle(self, *args, **options):
        self.time_suffix = time.strftime(TIME_FORMAT)
        self.email = options.get('email')
        self.ftp = options.get('ftp')
        self.compress = options.get('compress')
        self.directories = options.get('directories')
        self.zipencrypt = options.get('zipencrypt')
        self.encrypt_password = os.environ.get('BACKUP_PASSWORD')
        self.media = options.get('media')
        self.rsync = options.get('rsync')
        self.clean = options.get('clean')
        self.clean_db = options.get('clean_db')
        self.clean_media = options.get('clean_media')
        self.clean_rsync = options.get('clean_rsync') and self.rsync  # Only when rsync is True
        self.clean_local_db = options.get('clean_local_db')
        self.clean_remote_db = options.get('clean_remote_db')
        self.clean_local_media = options.get('clean_local_media')
        self.clean_remote_media = options.get('clean_remote_media')
        self.clean_local_rsync = options.get('clean_local_rsync') and self.rsync  # Only when rsync is True
        self.clean_remote_rsync = options.get('clean_remote_rsync') and self.rsync  # Only when rsync is True
        self.no_local = options.get('no_local')
        self.delete_local = options.get('delete_local')
        self.apps = options.get('apps')

        if self.zipencrypt and not self.encrypt_password:
            raise CommandError(
                'Please specify a password for your backup file'
                ' using the BACKUP_PASSWORD environment variable.'
            )

        if self.clean_rsync:
            self.stdout.write('cleaning broken rsync backups')
            self.clean_broken_rsync()
        else:
            if self.clean_local_rsync:
                self.stdout.write('cleaning local broken rsync backups')
                self.clean_local_broken_rsync()

            if self.clean_remote_rsync:
                self.stdout.write('cleaning remote broken rsync backups')
                self.clean_remote_broken_rsync()

        if self.clean_db:
            self.stdout.write('cleaning surplus database backups')
            self.clean_surplus_db()

        if self.clean_local_db:
            self.stdout.write('cleaning local surplus database backups')
            self.clean_local_surplus_db()

        if self.clean_remote_db:
            self.stdout.write('cleaning remote surplus database backups')
            self.clean_remote_surplus_db()

        if self.clean_media:
            self.stdout.write('cleaning surplus media backups')
            self.clean_surplus_media()

        if self.clean_local_media:
            self.stdout.write('cleaning local surplus media backups')
            self.clean_local_surplus_media()

        if self.clean_remote_media:
            self.stdout.write('cleaning remote surplus media backups')
            self.clean_remote_surplus_media()

        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)

        outfile = os.path.join(self.backup_dir, 'backup_%s.sql' % self.time_suffix)

        # Doing backup
        if self.engine == 'django.db.backends.mysql' or 'mysql' in self.engine:
            self.stdout.write('Doing Mysql backup to database %s into %s' % (self.db, outfile))
            self.do_mysql_backup(outfile)
        # TODO reinstate postgres support
        elif self.engine == 'django.db.backends.postgresql_psycopg2':
            self.stdout.write('Doing Postgresql backup to database %s into %s' % (self.db, outfile))
            self.do_postgresql_backup(outfile)
        else:
            raise CommandError('Backup in %s engine not implemented' % self.engine)

        # Compressing backup
        if self.compress:
            compressed_outfile = outfile + '.gz'
            self.stdout.write('Compressing backup file %s to %s' % (outfile, compressed_outfile))
            self.do_compress(outfile, compressed_outfile)
            outfile = compressed_outfile

        if self.zipencrypt:
            zip_encrypted_outfile = "{}.zip".format(outfile)
            self.stdout.write('Zipping and cncrypting backup file {} to {}'.format(outfile, zip_encrypted_outfile))
            self.do_encrypt(outfile, zip_encrypted_outfile)
            outfile = zip_encrypted_outfile

        # Backing up media directories,
        if self.media:
            self.directories += [self.directory_to_backup]

        # Backing up directories
        dir_outfiles = []

        if self.directories:  # We need to do media backup
            all_directories = ' '.join(self.directories)
            self.all_directories = all_directories
            if self.rsync:
                self.do_media_rsync_backup()
            else:
                # Backup all the directories in one file.
                all_outfile = os.path.join(self.backup_dir, 'dir_%s.tar.gz' % self.time_suffix)
                self.compress_dir(all_directories, all_outfile)
                dir_outfiles.append(all_outfile)

        # Sending mail with backups
        if self.email:
            self.stdout.write("Sending e-mail with backups to '%s'" % self.email)
            self.sendmail(settings.SERVER_EMAIL, [self.email], dir_outfiles + [outfile])

        if self.ftp:
            self.stdout.write("Saving to remote server")
            self.store_ftp(local_files=[os.path.join(os.getcwd(), x) for x in dir_outfiles + [outfile]])

    def compress_dir(self, directory, outfile):
        self.stdout.write('Backup directories ...')
        command = 'cd %s && tar -czf %s *' % (directory, outfile)
        self.stdout.write('=' * 70)
        self.stdout.write('Running Command: %s' % command)
        os.system(command)

    @staticmethod
    def get_blacklist_tables():
        """
        Exclude BACKUP_TABLES_BLACKLIST if it's defined.
        """
        return getattr(settings, 'BACKUP_TABLES_BLACKLIST', [])

    @staticmethod
    def get_tables_for_apps(*apps):
        """
        Get table names for all for the given applications.
        """
        
        tables = connection.introspection.django_table_names(only_existing=True)
        
        def check_table(table):
            return any(table.startswith('%s_' % app) for app in apps)
        
        return list(filter(check_table, tables))

    def store_ftp(self, local_files=None):
        
        if not local_files:
            local_files = []
            
        sftp = self.get_connection()
        
        if self.remote_dir:
            try:
                sftp.mkdir(self.remote_dir)
            except IOError:
                pass
        for local_file in local_files:
            filename = os.path.split(local_file)[-1]
            self.stdout.write('Saving %s to remote server ' % local_file)
            sftp.put(local_file, os.path.join(self.remote_dir or '', filename))
        if self.delete_local:
            backups = os.listdir(self.backup_dir)
            backups = list(filter(is_backup, backups))
            backups.sort()
            self.stdout.write('=' * 70)
            self.stdout.write('--cleanlocal, local db and media backups found: %s' % backups)
            remove_list = backups
            self.stdout.write('local db and media backups to clean %s' % remove_list)
            remove_all = ' '.join([os.path.join(self.backup_dir, i) for i in remove_list])
            if remove_all:
                self.stdout.write('=' * 70)
                self.stdout.write('cleaning up local db and media backups')
                command = 'rm -r %s' % remove_all
                self.stdout.write('=' * 70)
                self.stdout.write('Running Command: %s' % command)
                os.system(command)
            # remote(ftp server)
        elif self.no_local:
            to_remove = local_files
            self.stdout.write('=' * 70)
            self.stdout.write('--nolocal, Local files to remove %s' % to_remove)
            remove_all = ' '.join(to_remove)
            if remove_all:
                self.stdout.write('=' * 70)
                self.stdout.write('cleaning up local backups')
                command = 'rm -r %s' % remove_all
                self.stdout.write('=' * 70)
                self.stdout.write('Running Command: %s' % command)
                os.system(command)

    @staticmethod
    def sendmail(address_from, addresses_to, attachments):
        subject = "Your DB-backup for " + datetime.now().strftime("%d %b %Y")
        body = "Timestamp of the backup is " + datetime.now().strftime("%d %b %Y")

        email = EmailMessage(subject, body, address_from, addresses_to)
        email.content_subtype = 'html'
        for attachment in attachments:
            email.attach_file(attachment)
        email.send()

    @staticmethod
    def do_compress(infile, outfile):
        os.system('gzip --stdout %s > %s' % (infile, outfile))
        os.system('rm %s' % infile)

    def do_encrypt(self, infile, outfile):
        os.system('zip -P %s %s %s' % (self.encrypt_password, outfile, infile))
        os.system('rm %s' % infile)

    def do_mysql_backup(self, outfile):

        if self.apps:
            raise NotImplementedError("Backuping up only ceratain apps not implemented in MySQL")
        args = []
        if self.user:
            args += ["--user='%s'" % self.user]
        if self.passwd:
            args += ["--password='%s'" % self.passwd]
        if self.host:
            args += ["--{}='{}'".format("socket" if self.host.startswith('/') else "host", self.host)]
        if self.port:
            args += ["--port=%s" % self.port]
        args += [self.db]
        base_args = copy(args)
        blacklist_tables = self.get_blacklist_tables()
        if blacklist_tables:
            all_tables = connection.introspection.get_table_list(connection.cursor())
            tables = list(set(all_tables) - set(blacklist_tables))
            args += tables
        os.system('%s %s > %s' % (getattr(settings, 'BACKUP_SQLDUMP_PATH', 'mysqldump'), ' '.join(args), outfile))
        
        # Append table structures of blacklist_tables
        if blacklist_tables:
            all_tables = connection.introspection.get_table_list(connection.cursor())
            blacklist_tables = list(set(all_tables) and set(blacklist_tables))
            args = base_args + ['-d'] + blacklist_tables
            cmd = '%s %s >> %s' % (getattr(settings, 'BACKUP_SQLDUMP_PATH', 'mysqldump'), ' '.join(args), outfile)
            os.system(cmd)

    def do_postgresql_backup(self, outfile):
        args = []
        if self.user:
            args += ["--username=%s" % self.user]
        if self.host:
            args += ["--host=%s" % self.host]
        if self.port:
            args += ["--port=%s" % self.port]
        if self.db:
            args += [self.db]
        pgdump_path = getattr(settings, 'BACKUP_PG_DUMP_PATH', 'pg_dump')

        if self.passwd:
            os.environ['PGPASSWORD'] = self.passwd
        table_args = ' '.join(
            '-t %s ' % table for table in self.get_tables_for_apps(*self.apps)
        )
        if table_args:
            table_args = '-a %s' % table_args
        pgdump_cmd = '%s %s %s > %s' % (pgdump_path, ' '.join(args), table_args or '--clean', outfile)
        self.stdout.write(pgdump_cmd)
        os.system(pgdump_cmd)

    def clean_local_surplus_db(self):
        try:
            backups = os.listdir(self.backup_dir)
            backups = list(filter(is_db_backup, backups))
            backups.sort()
            self.stdout.write('=' * 70)
            self.stdout.write('local db backups found: %s' % backups)
            remove_list = decide_remove(backups, settings.BACKUP_DATABASE_COPIES)
            self.stdout.write('=' * 70)
            self.stdout.write('local db backups to clean %s' % remove_list)
            remove_all = ' '.join([os.path.join(self.backup_dir, i) for i in remove_list])
            if remove_all:
                self.stdout.write('=' * 70)
                self.stdout.write('cleaning up local db backups')
                command = 'rm %s' % remove_all
                self.stdout.write('=' * 70)
                self.stdout.write('Running Command: %s' % command)
                os.system(command)
        except ImportError:
            self.stderr.writeln('cleaned nothing, because BACKUP_DATABASE_COPIES is missing')

    def clean_remote_surplus_db(self):
        try:
            sftp = self.get_connection()
            backups = [i.strip() for i in sftp.listdir(self.remote_dir)]
            backups = list(filter(is_db_backup, backups))
            backups.sort()
            self.stdout.write('=' * 70)
            self.stdout.write('remote db backups found: %s' % backups)
            remove_list = decide_remove(backups, settings.BACKUP_DATABASE_COPIES)
            self.stdout.write('=' * 70)
            self.stdout.write('remote db backups to clean %s' % remove_list)
            if remove_list:
                self.stdout.write('=' * 70)
                self.stdout.write('cleaning up remote db backups')
                for file_ in remove_list:
                    target_path = os.path.join(self.remote_dir, file_)
                    self.stdout.write('Removing {}'.format(target_path))
                    sftp.remove(target_path)
        except ImportError:
            self.stderr.writeln('cleaned nothing, because BACKUP_DATABASE_COPIES is missing')

    def clean_surplus_db(self):
        self.clean_local_surplus_db()
        self.clean_remote_surplus_db()

    def clean_surplus_media(self):
        self.clean_local_surplus_media()
        self.clean_remote_surplus_media()

    def clean_local_surplus_media(self):
        try:
            # local(web server)
            backups = os.listdir(self.backup_dir)
            backups = list(filter(is_media_backup, backups))
            backups.sort()
            self.stdout.write('=' * 70)
            self.stdout.write('local media backups found: %s' % backups)
            remove_list = decide_remove(backups, settings.BACKUP_MEDIA_COPIES)
            self.stdout.write('=' * 70)
            self.stdout.write('local media backups to clean %s' % remove_list)
            remove_all = ' '.join([os.path.join(self.backup_dir, i) for i in remove_list])
            if remove_all:
                self.stdout.write('=' * 70)
                self.stdout.write('cleaning up local media backups')
                command = 'rm -r %s' % remove_all
                self.stdout.write('=' * 70)
                self.stdout.write('Running Command: %s' % command)
                os.system(command)
        except ImportError:
            self.stderr.writeln('cleaned nothing, because BACKUP_MEDIA_COPIES is missing')

    def clean_remote_surplus_media(self):
        try:
            sftp = self.get_connection()
            backups = [i.strip() for i in sftp.listdir(self.remote_dir)]
            backups = list(filter(is_media_backup, backups))
            backups.sort()
            self.stdout.write('=' * 70)
            self.stdout.write('remote media backups found: %s' % backups)
            remove_list = decide_remove(backups, settings.BACKUP_MEDIA_COPIES)
            self.stdout.write('=' * 70)
            self.stdout.write('remote media backups to clean %s' % remove_list)
            if remove_list:
                self.stdout.write('=' * 70)
                self.stdout.write('cleaning up remote media backups')
                for file_ in remove_list:
                    target_path = os.path.join(self.remote_dir, file_)
                    self.stdout.write('Removing {}'.format(target_path))
                    sftp.remove(target_path)
        except ImportError:
            self.stderr.writeln('cleaned nothing, because BACKUP_MEDIA_COPIES is missing')

    def do_media_rsync_backup(self):
        
        # Local media rsync backup
        
        if not self.delete_local and not self.no_local:
            self.stdout.write('Doing local media rsync backup')
            local_current_backup = os.path.join(self.backup_dir, 'current')
            local_backup_target = os.path.join(self.backup_dir, 'dir_%s' % self.time_suffix)
            local_info = {
                'local_current_backup': local_current_backup,
                'all_directories': self.all_directories,
                'local_backup_target': local_backup_target,
                'rsync_flag': GOOD_RSYNC_FLAG,
            }
            local_rsync_cmd = 'rsync -az --copy-dirlinks --link-dest=%(local_current_backup)s %(all_directories)s %(local_backup_target)s' % local_info
            local_mark_cmd = 'touch %(local_backup_target)s/%(rsync_flag)s' % local_info
            local_link_cmd = 'rm -f %(local_current_backup)s && ln -s %(local_backup_target)s %(local_current_backup)s' % local_info
            cmd = '\n'.join(['%s&&%s' % (local_rsync_cmd, local_mark_cmd), local_link_cmd])
            self.stdout.write(cmd)
            os.system(cmd)

        # Remote media rsync backup
        
        if self.ftp:
            self.stdout.write('Doing remote media rsync backup')
            host = '%s@%s' % (self.ftp_username, self.ftp_server)
            remote_current_backup = os.path.join(self.remote_dir, 'current')
            remote_backup_target = os.path.join(self.remote_dir, 'dir_%s' % self.time_suffix)
            remote_info = {
                'remote_current_backup': remote_current_backup,
                'all_directories': self.all_directories,
                'host': host,
                'remote_backup_target': remote_backup_target,
                'rsync_flag': GOOD_RSYNC_FLAG,
            }
            
            remote_rsync_cmd = 'rsync -az --copy-dirlinks --link-dest=%(remote_current_backup)s %(all_directories)s %(host)s:%(remote_backup_target)s' % remote_info
            remote_mark_cmd = 'ssh %(host)s "touch %(remote_backup_target)s/%(rsync_flag)s"' % remote_info
            remote_link_cmd = 'ssh %(host)s "rm -f %(remote_current_backup)s && ln -s %(remote_backup_target)s %(remote_current_backup)s"' % remote_info
            
            cmd = '\n'.join(['%s&&%s' % (remote_rsync_cmd, remote_mark_cmd), remote_link_cmd])
            self.stdout.write(cmd)
            sftp = self.get_connection()
            try:
                sftp.mkdir(self.remote_dir)
            except IOError:
                pass
            os.system(cmd)

    def clean_broken_rsync(self):
        self.clean_local_broken_rsync()
        self.clean_remote_broken_rsync()

    def clean_remote_broken_rsync(self):
        sftp = self.get_connection()
        backups = [i.strip() for i in sftp.execute('ls %s' % self.remote_dir)]
        backups = list(filter(is_media_backup, backups))
        backups.sort()
        commands = []
        for backup in backups:
            # Find the GOOD_RSYNC_FLAG file in the backup dir
            backup_path = os.path.join(self.remote_dir, backup)
            flag_file = os.path.join(backup_path, GOOD_RSYNC_FLAG)
            cmd = 'test -e %s||rm -rf %s' % (flag_file, backup_path)
            commands.append(cmd)

        full_cmd = '\n'.join(commands)
        self.stdout.write(full_cmd)
        sftp.execute(full_cmd)

    def clean_local_broken_rsync(self):
        # local(web server)
        backups = os.listdir(self.backup_dir)
        backups = list(filter(is_media_backup, backups))
        backups.sort()
        commands = []
        for backup in backups:
            # Find the GOOD_RSYNC_FLAG file in the backup dir
            backup_path = os.path.join(self.backup_dir, backup)
            flag_file = os.path.join(backup_path, GOOD_RSYNC_FLAG)
            cmd = 'test -e %s||rm -rf %s' % (flag_file, backup_path)
            commands.append(cmd)
        full_cmd = '\n'.join(commands)
        self.stdout.write(full_cmd)
        os.system(full_cmd)
