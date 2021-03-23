"""
A Model class for parsing a PubChem Compound XML file

Unlike aid_model.py it uses the lxml
package for iterative parsing bc
the xml files typically tend to be huge.

"""
import xml.etree.cElementTree as ET
import lxml.etree as et


NAMESPACE = {'pc': 'http://www.ncbi.nlm.nih.gov'}

class Compounds:

    def __init__(self, xml_file):
        self.tree = et.iterparse(xmlfile, events=('end',), tag='{http://www.ncbi.nlm.nih.gov}PC-Compound')

    def parse_compounds(self):

        cmps = []

        for _, pc_elem in self.tree:

            cmp = {}
            cmp = Compound(pc_elem)
            cid = cmp.get_cid()
            props = cmp.get_props()

            cmp['cid'] = cid
            cmp['props'] = props
            cmps.append(cmp)
        return cmps

    @classmethod
    def load_compounds(cls, xmlfile):
        return cls(xmlfile)

class Compound:

    def __init__(self, pc_elem):
        self.cmp = pc_elem

    def get_node(self, path):
        return self.cmp.find(path, NAMESPACE)

    def get_nodes(self, path):
        return self.cmp.findall(path, NAMESPACE)

    def get_cid(self):
        cid_node = self.get_node('pc:PC-Compound_id/pc:PC-CompoundType/pc:PC-CompoundType_id/pc:PC-CompoundType_id_cid')
        return int(cid_node.text)

    def get_props(self):
        info_data = self.get_nodes('pc:PC-Compound_props/pc:PC-InfoData')

        data_list = []
        for info_datum in info_data:
            # the urn has meta data about
            # the property
            prop = {}

            label = info_datum.find('pc:PC-InfoData_urn/pc:PC-Urn/pc:PC-Urn_label', NAMESPACE)
            if label is not None:
                label = label.text
            prop['label'] = label

            name = info_datum.find('pc:PC-InfoData_urn/pc:PC-Urn/pc:PC-Urn_name', NAMESPACE)
            if name is not None:
                name = name.text
            prop['name'] = name

            dtype = info_datum.find('pc:PC-InfoData_urn/pc:PC-Urn/pc:PC-Urn_datatype/pc:PC-UrnDataType', NAMESPACE)
            if dtype is not None:
                # attribute are in dictionary
                # format in lxlm
                dtype = dtype.get('value', None)
            prop['dtype'] = dtype

            value = info_datum.find('pc:PC-InfoData_value', NAMESPACE)[0]
            if value is not None:
                value = value.text
            prop['value'] = value

            data_list.append(prop)

        return data_list

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-f', '--xmlfile', metavar='df', type=str,
                        help='Name of data file containing cids (as txt)')

    args = parser.parse_args()
    xmlfile = args.xmlfile

    compounds = Compounds(xmlfile)
    prop = compounds.parse_compounds()


