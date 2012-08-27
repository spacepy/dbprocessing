#!/usr/bin/env python2.6

import ConfigParser
import os
from optparse import OptionParser
import sys

from dbprocessing import DBUtils2

import SimpleEmail


class QCEmailer(object):

    def __init__(self, *args, **kwargs):
        a = DBUtils2.DBUtils2('rbsp')
        a._openDB()
        a._createTableObjects()

        self.prod_name = kwargs['PRODUCT']

        kwargs['link'] = self.calcLink()

        info = a.getFilesQC()
        files = filter(lambda x: x[3] == self.prod_name, info)
        kwargs['dates'] = [v[1].isoformat() for v in files]
        if len(kwargs['dates']) == 0 :
            sys.stderr.write("Warning: there were no dates for QC for product: {0}, no output written\n".format(kwargs['PRODUCT']))
            sys.exit(0)


        cfgFile = kwargs['configFile']
        self.config=None # configuration from file
        self.read_conf(cfgFile)
        self.email = self.email_account()
        subject = 'Quality Assurance (QA) request for ECT Level 1 data plot  {PRODUCT}'.format(**kwargs)
        body = ['New Level 1 data has been processed at the ECT SOC and new plots for product {PRODUCT}'.format(**kwargs),
                     'are available for the following dates:',
                     '',
                     '{dates}'.format(**kwargs),
                    '',
                    '',
                    'Please follow the link below to launch into the Autoplot PNGwalk QA mode for these plots.',
                    'For each date that has not been checked, please indicate "Ok", "Ignore" or "Problem" and',
                    'add a comment if desired. Don\'t forget to "save" your choice.',
                    '',
                    '{link}'.format(**kwargs),
                    '',
                    '',
                    'Dates for which "Ok" was indicated will proceed in the processing chain',
                    'which will produce Level 1 "final" data files and will enable processing to higher levels.',
                    '',
                    'Kind Regards',
                    'ECT-SOC',
                    ]

        body = '\n'.join(body)
        if not kwargs['dryrun']:
            self.email.send_simple(self.config['REPT']['rx'], subject, body)
        else:
            print('EMAIL::\n')
            print('Subject:\n\n{0}'.format(subject))
            print('Body:\n\n{0}'.format(body))

    def calcLink(self):
        """
        return the link for the email based on the product
        """
        return 'http://www.rbsp-ect.lanl.gov/autoplot_launchers/rbspa_pre_ect-rept-hk-tvc-L1.jnlp'

    def read_conf(self, conf_file):
        cfg = ConfigParser.SafeConfigParser()
        cfg.read(conf_file)
        self.config = {}
        for key in cfg.sections():
            self.config[key] = {}
            for k2 in cfg.options(key):
                self.config[key][k2] = cfg.get(key, k2)

    def email_account(self, **kwargs):
        """Create an email account based on configuration information

        @return: the instantiated account
        @rtype: RESCSEmailAccount
        """
        return SimpleEmail.EmailAccount(self.config['QCEmail']['smtp'],
                            self.config['QCEmail']['imap'],
                            self.config['QCEmail']['address'],
                            self.config['QCEmail']['password'],
                            username=self.config['QCEmail']['username'],
                            sent=self.config['QCEmail']['sent'],
                            )



def main():
    usage = \
    """usage: %prog [-d, --dryrun] product_name
        -d dryrun mode just print the email to screen don't send
        -f config filename (default ~/dbUtils/QCEmailer_conf.txt)
        product name (or ID)"""
    parser = OptionParser(usage)
    parser.add_option("-d", "--dryrun", dest="dryrun",
                      action="store_true", help="dryrun mode", default=False)
    parser.add_option("-f", "--file", dest="filename",
                      help="config filename", default=os.path.expanduser(os.path.join('~', 'dbUtils', 'QCEmailer_conf.txt')))
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    a = DBUtils2.DBUtils2('rbsp')
    a._openDB()
    a._createTableObjects()
    prod_id = a._getProductID(args[0])
    prod_name = a.getProductName(prod_id)

    if not os.path.isfile(options.filename):
        sys.stderr.write("No configuration file: {0}\n".format(options.filename))
        sys.exit(-2)

    QCEmailer(configFile=options.filename,
                   PRODUCT=prod_name,
                   dryrun=options.dryrun)



if __name__ == "__main__":
    main()


