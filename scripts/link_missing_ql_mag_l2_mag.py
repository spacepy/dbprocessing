
import datetime
import glob
import os
import re

def get_date(inval):
    match = re.search(r'.*(\d{8}).*\.cdf$', os.path.basename(inval))
    if match:
        return match.groups(0)[0]
    else:
        None

ql_template = 'rbsp-a_magnetometer_uvw_emfisis-Quick-Look_XXXX_v1.0.0.cdf'  
# use the 1.0.0 to denote that I created it, real files have higher numbers
# and will override in the processing.

l2_template = 'rbsp-a_magnetometer_uvw_emfisis-L2_XXXX_v1.0.0.cdf'

ql_a_dir = '/n/space_data/cda/rbsp/rbspa/emfisis/quicklook'
l2_a_dir = '/n/space_data/cda/rbsp/rbspa/emfisis/l2'

ql_b_dir = '/n/space_data/cda/rbsp/rbspb/emfisis/quicklook'
l2_b_dir = '/n/space_data/cda/rbsp/rbspb/emfisis/l2'

ql_a_files = glob.glob(os.path.join(ql_a_dir, '*.cdf'))
l2_a_files = glob.glob(os.path.join(l2_a_dir, '*.cdf'))

ql_b_files = glob.glob(os.path.join(ql_b_dir, '*.cdf'))
l2_b_files = glob.glob(os.path.join(l2_b_dir, '*.cdf'))

# get all the dates from QL-a
ql_a_dates = set([get_date(v) for v in ql_a_files])
l2_a_dates = set([get_date(v) for v in l2_a_files])
a_dates = l2_a_dates.difference(ql_a_dates)
# can be done smarter, but make links betwen QL and L2 for the missing dates
for ad in a_dates:
    ql_name = ql_template.replace('XXXX', ad)
    l2_name = [v for v in l2_a_files if ad in v and 'magnetometer' in v]
    # now we only want the largest version
    if l2_name:
        l2_name = max(l2_name)
        os.symlink(l2_name, os.path.join(ql_a_dir, ql_name))
        print('{0}->{1}'.format(l2_name, os.path.join(ql_a_dir, ql_name)))



ql_b_dates = set([get_date(v) for v in ql_b_files])
l2_b_dates = set([get_date(v) for v in l2_b_files])
b_dates = l2_b_dates.difference(ql_b_dates)

for bd in b_dates:
    ql_name = ql_template.replace('XXXX', bd)
    l2_name = [v for v in l2_b_files if bd in v and 'magnetometer' in v]
    # now we only want the largest version
    if l2_name:
        l2_name = max(l2_name)
        os.symlink(l2_name, os.path.join(ql_b_dir, ql_name))
        print('{0}->{1}'.format(l2_name, os.path.join(ql_b_dir, ql_name)))







