import unittest
import DBUtils


class KnownValues(unittest.TestCase):


    def setUp(self):
        super(KnownValues, self).setUp()
        self.dbu = DBUtils.DBUtils()

    def test__build_fname(self):
        """__build_fname should give known result with known input"""
        build_fname_kv = {}
        build_fname_kv['/root/file/relative/Test-test1_Prod1_20100614_v1.1.1.cdf'] = \
                       ('/root/file/', 'relative/', 'Test', 'test1', 'Prod1', '20100614', 1, 1, 1)
        for val in build_fname_kv:
            result = self.dbu._DBUtils__build_fname(*build_fname_kv[val])
            self.assertEqual(result, val)

    def test__get_V_num(self):
        """__get_V_num should give known result with known input"""
        get_V_num_kv = {}
        get_V_num_kv['1101'] = (1,1, 1)
        for val in get_V_num_kv:
            result = self.dbu._DBUtils__get_V_num(*get_V_num_kv[val])
            self.assertEqual(str(result), val)


if __name__ == "__main__":
	unittest.main()



