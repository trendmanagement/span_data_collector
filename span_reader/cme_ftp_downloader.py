#!/usr/bin/env python
"""
CME FTP data downloader
"""

import os
import sys
import argparse
import re
import ftplib
from datetime import datetime
import logging

FTP_SERVER = "ftpstc.cmegroup.com"
FTP_USER = "anonymous"
FTP_PASS = ""


class CMEFTPLoader:
    def __init__(self, args, loglevel):
        self.args = args
        self.loglevel = loglevel
        logging.getLogger("pika").setLevel(logging.WARNING)

        self.logger = logging.getLogger('CME FTP Downloader')
        self.logger.setLevel(loglevel)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # create console handler with a higher log level
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.cme_filename_regex = re.compile(r'(?P<cme_cpa>cme\.\d{8}\.c\.pa2.zip)')
        self.ftp_files_list = None

    def match_c_pa_files(self, file_list_str):
        groups = self.cme_filename_regex.findall(file_list_str)
        if len(groups) == 0:
            return None
        else:
            self.ftp_files_list.append(groups[0])

    def get_file(self, ftp, filename):
        try:
            ftp.retrbinary("RETR " + filename, open(filename, 'wb').write)
        except Exception as exc:
            self.logger.error("Error while getting FTP file: {0}".format(exc))

    def sync_ftp(self, ftp):
        self.logger.debug("Retrieving FTP files list")
        self.ftp_files_list = []
        ftp.dir(self.match_c_pa_files)
        existing_files_list = os.listdir(self.args.out_dir)

        for ftp_fn in self.ftp_files_list:
            if not self.args.force and ftp_fn in existing_files_list:
                self.logger.debug("File exists: {0} skipping.".format(ftp_fn))
                continue

            self.logger.info('Downloading: {0}'.format(ftp_fn))
            self.get_file(ftp, ftp_fn)

    def load_at_date(self, ftp):
        fn = "cme.{0}.c.pa2.zip".format(self.args.date.strftime("%Y%m%d"))
        self.logger.info("Downloading at date {0}: {1}".format(self.args.date, fn))
        self.get_file(ftp, fn)

    def main(self):
        self.logger.debug("Connecting to the server")
        ftp = ftplib.FTP(FTP_SERVER)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd('/span/data/cme/')

        if args.date is not None:
            self.load_at_date(ftp)
        else:
            self.sync_ftp(ftp)
        ftp.quit()
        self.logger.info("Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="CME FTP Data downloader",
        epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on the commandline like '%(prog)s @params.conf'.",
        fromfile_prefix_chars='@')

    parser.add_argument(
        "-v",
        "--verbose",
        help="Increase output verbosity",
        action="store_true")

    parser.add_argument(
        "-F",
        "--force",
        help="Force rewrite existing files",
        action="store_true")


    def valid_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            msg = "Not a valid date: '{0}'.".format(s)
            raise argparse.ArgumentTypeError(msg)

    parser.add_argument(
        "-D",
        "--date",
        help="Load CME span file for particular date",
        action="store", type=valid_date)

    parser.add_argument('out_dir', type=str,
                        help='Output directory for CME files')

    args = parser.parse_args()

    # Setup logging
    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    script = CMEFTPLoader(args, loglevel)
    script.main()
