"""
A Model class for parsing a PubChem Substance XML file

Unlike aid_model.py it uses the lxml
package for iterative parsing bc
the xml files typically tend to be huge.

"""
import xml.etree.cElementTree as ET
import lxml.etree as et


NAMESPACE = {'pc': 'http://www.ncbi.nlm.nih.gov'}

class Substances:

    def __init__(self, xmlfile):
        self.tree = et.iterparse(xmlfile, events=('end',), tag='{http://www.ncbi.nlm.nih.gov}PC-Substance')

    def parse_substances(self):

        subs = []

        for _, pc_elem in self.tree:
            sub = Substance(pc_elem)

            record = sub.as_record()
            yield record

    @classmethod
    def load_substances(cls, xmlfile):
        return cls(xmlfile)

class Substance:

    def __init__(self, pc_elem):
        self.sub = pc_elem

    def get_node(self, path):
        return self.sub.find(path, NAMESPACE)

    def get_nodes(self, path):
        return self.sub.findall(path, NAMESPACE)

    def get_sid(self):
        sid_node = self.get_node('pc:PC-Substance_sid/pc:PC-ID/pc:PC-ID_id')
        return int(sid_node.text)

    def get_source(self):
        pass

    def get_cids(self):
        cid_nodes = self.get_nodes('pc:PC-Substance_compound/pc:PC-Compounds/pc:PC-Compound/pc:PC-Compound_id'
                                   '/pc:PC-CompoundType/pc:PC-CompoundType_id/pc:PC-CompoundType_id_cid')
        return [int(cid_node.text) for cid_node in cid_nodes]

    def as_record(self):
        record = {}
        record['sid'] = self.get_sid()
        record['_id'] = self.get_sid()
        record['cids'] = self.get_cids()
        return record

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-f', '--xmlfile', metavar='df', type=str,
                        help='Name of data file')

    args = parser.parse_args()
    xmlfile = args.xmlfile

    substances = Substances.load_substances(xmlfile)
    prop = substances.parse_substances()

    print(prop)
