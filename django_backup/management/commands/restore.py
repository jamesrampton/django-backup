import os
import time
from optparse import make_option
from tempfile import gettempdir

from django.core.management.base import BaseCommand, CommandError

from django_backup.utils import BaseBackupCommand, TIME_FORMAT, is_db_backup, is_media_backup


class Command(BaseBackupCommand):

    help = "Restores latest backup."
    options = BaseBackupCommand.options + (
        make_option(
            '--media', '-m',
            action='store_true', default=False, dest='media',
            help='Restore media dir'
        ),
        make_option(
            '--no-database', '-d',
            action='store_true', default=False, dest='no_database',
            help='Do not restore database'
        ),
    )

    @staticmethod
    def _time_suffix():
        return time.strftime(TIME_FORMAT)

    def handle(self, *args, **options):

        self.restore_media = options.get('media')
        self.no_restore_database = options.get('no_database')
        self.stdout.write('Connecting to %s...' % self.ftp_server)
        sftp = self.get_connection()
        self.stdout.write('Connected.')
        try:
            dir_list = sftp.listdir(self.remote_restore_dir)
        except IOError:
            raise CommandError("Remote directory %s does not exist" % self.remote_restore_dir)
        backups = [i.strip() for i in dir_list]
        db_backups = list(filter(is_db_backup, backups))
        db_backups.sort()

        if self.restore_media:
            media_backups = list(filter(is_media_backup, backups))
            media_backups.sort()
            media_remote = media_backups[-1]
        else:
            media_remote = None

        self.tempdir = gettempdir()

        if not self.no_restore_database:
            db_remote = db_backups[-1]

            db_local = os.path.join(self.tempdir, db_remote)
            self.stdout.write('Fetching database %s...' % db_remote)
            sftp.get(os.path.join(self.remote_restore_dir, db_remote), db_local)
            self.stdout.write('Uncompressing database...')
            uncompressed = self.uncompress(db_local)

            if uncompressed is 0:
                sql_local = db_local[:-3]
            else:
                sql_local = db_local

        if self.restore_media:
            self.stdout.write('Fetching media %s...' % media_remote)
            media_local = os.path.join(self.tempdir, media_remote)
            media_remote_full_path = os.path.join(self.remote_restore_dir, media_remote)

            # Check if the media is compressed or a folder
            cmd = 'if [[ -d "%s" ]]; then echo 1; else echo 0; fi'
            is_folder = int(sftp.execute(cmd % media_remote_full_path)[0])

            if is_folder == 1:
                media_dir = os.path.join(media_remote_full_path, "media")
                # A trailing slash to transfer only the contents of the folder
                remote_rsync = '%s@%s:%s/' % (self.ftp_username, self.ftp_server, media_dir)
                rsync_restore_cmd = 'rsync -az %s %s' % (remote_rsync, self.directory_to_backup)
                self.stdout.write('Running rsync restore command: %s' % rsync_restore_cmd)
                os.system(rsync_restore_cmd)
            else:
                sftp.get(media_remote_full_path, media_local)
                self.stdout.write('Uncompressing media...')
                self.uncompress_media(media_local)
        # Doing restore
        if not self.no_restore_database:
            if self.engine == 'django.db.backends.mysql':
                self.stdout.write('Doing Mysql restore to database %s from %s...' % (self.db, sql_local))
                self.mysql_restore(sql_local)
            # TODO reinstate postgres support
            elif self.engine == 'django.db.backends.postgresql_psycopg2':
                self.stdout.write('Doing Postgresql restore to database %s from %s...' % (self.db, sql_local))
                self.posgresql_restore(sql_local)
            else:
                raise CommandError('Backup in %s engine not implemented' % self.engine)

    def uncompress(self, filename):
        cmd = 'cd %s;gzip -df %s' % (self.tempdir, filename)
        self.stdout.write('\t%s' % cmd)
        return os.system(cmd)

    def uncompress_media(self, filename):
        cmd = u'tar -C %s -xzf %s' % (self.directory_to_backup, filename)
        self.stdout.write('\t%s' % cmd)
        os.system(cmd)

    def mysql_restore(self, infile):
        args = []
        if self.user:
            args += ["--user=%s" % self.user]
        if self.passwd:
            args += ["--password='%s'" % self.passwd]
        if self.host:
            args += ["--host=%s" % self.host]
        if self.port:
            args += ["--port=%s" % self.port]
        args += [self.db]
        cmd = 'mysql %s < %s' % (' '.join(args), infile)
        self.stdout.write('\t%s' % cmd)
        os.system(cmd)

    def posgresql_restore(self, infile):
        args = ['psql']
        if self.user:
            args.append("-U %s" % self.user)
        if self.passwd:
            os.environ['PGPASSWORD'] = self.passwd
        if self.host:
            args.append("-h %s" % self.host)
        if self.port:
            args.append("-p %s" % self.port)
        args.append('-f %s' % infile)
        args.append("-o %s" % os.path.join(self.tempdir, 'dump.log'))
        args.append(self.db)
        cmd = ' '.join(args)
        self.stdout.write('\t%s' % cmd)
        os.system(cmd)
