"""
A Model class for parsing a PubChem Bioassay XML file

Bioassay is the main class
but each PubChem Bioassay contains
two portions, a portion describing
the descirption and a portion
describing the results.

"""
import xml.etree.cElementTree as ET

NAMESPACE = {'pc': 'http://www.ncbi.nlm.nih.gov'}

class Bioassay:

    def __init__(self, xml_root):
        self.assay = BioassayDesc(xml_root.find('pc:PC-AssaySubmit_assay', NAMESPACE))
        self.assay_results = BioassayResults(xml_root.find('pc:PC-AssaySubmit_data', NAMESPACE))


class BioassayDesc:

    def __init__(self, assay):
        self.assay = assay

    def get_node(self, path):
        """ gets a specific node from relative to the top level (e.g., PC-AssaySubmit_assay) """
        return self.assay.find(path, NAMESPACE)

    def get_aid(self):
        aid_node = self.get_node('pc:PC-AssaySubmit_assay_descr/pc:PC-AssayDescription/pc:PC-AssayDescription_aid/pc:PC-ID')
        aid = aid_node.find('pc:PC-ID_id', NAMESPACE).text
        return int(aid)

    def get_name(self):
        name_node = self.get_node('pc:PC-AssaySubmit_assay_descr/pc:PC-AssayDescription/pc:PC-AssayDescription_name')
        return name_node.text

    def get_description(self):
        desc_node = self.get_node('pc:PC-AssaySubmit_assay_descr/pc:PC-AssayDescription/pc:PC-AssayDescription_description')
        full_desc = ''
        for desc in desc_node.findall('pc:PC-AssayDescription_description_E', NAMESPACE):
            full_desc = full_desc + desc.text
        return full_desc

    def get_comment(self):
        pass

    def get_xrefs(self):
        pass

    def get_result_types(self):
        """ result types can have id, name, description, type (e.g., float), transform """
        # should be a list of result types
        results_node = self.get_node('pc:PC-AssaySubmit_assay_descr/pc:PC-AssayDescription/pc:PC-AssayDescription_results')
        results_list = []

        for result_type in results_node:
            result = {}
            for attr in ['tid', 'name', 'type', 'transform']:
                node = result_type.find('pc:PC-ResultType_{}'.format(attr), NAMESPACE)

                if node != None:
                    node_text = getattr(node, 'text')
                    node_value = getattr(node, 'value') if hasattr(node, 'value') else None
                    result[attr] = {'text': node_text, 'value': node_value}

            descriptions = result_type.findall('pc:PC-ResultType_description/pc:C-ResultType_description_E', NAMESPACE)
            desc_text = ''
            for desc in descriptions:
                desc_text = desc_text + desc.text
            # result['description'] = desc_text
            results_list.append(result)
        return results_list

    def get_revision(self):
        pass

    def get_activity_outcome_method(self):
        """ e.g., confirmatory, summary, etc. """
        pass



class BioassayResults:
    def __init__(self, assay_results):
        self.assay_results = assay_results

    def get_node(self, path):
        """ gets a specific node from relative to the top level (e.g., PC-AssayResults) """
        return self.assay_results.findall(path, NAMESPACE)

    def parse_results(self):
        """ generator function (to save on RAM) to iteratively call parse one to parse pubchem results """
        for result in self.assay_results:
            yield self.parse_one(result)


    def parse_one(self, assay_result):
        result = {}
        result['sid'] = assay_result.find('pc:PC-AssayResults_sid', NAMESPACE).text
        result['sid_source'] = assay_result.find('pc:PC-AssayResults_sid-source/pc:PC-Source/'
                                                 'pc:PC-Source_db/pc:PC-DBTracking/pc:PC-DBTracking_name', NAMESPACE).text
        oc = assay_result.find('pc:PC-AssayResults_outcome', NAMESPACE)
        result['outcome'] = {**{'text': oc.text}, **oc.attrib}
        result['rank'] = assay_result.find('pc:pc:PC-AssayResults_rank', NAMESPACE)
        data_node = assay_result.findall('pc:PC-AssayResults_data/pc:PC-AssayData', NAMESPACE)

        assay_results = []
        for assaydata in data_node:
            result_dic = {}
            result_dic['tid'] = assaydata.find('pc:PC-AssayData_tid', NAMESPACE).text
            # the PC-AssayData_value container
            # can have multiple values
            result_dic['value'] = [val.text for val in assaydata.find('pc:PC-AssayData_value', NAMESPACE)]
            assay_results.append(result_dic)
        result['results'] = assay_results
        return result

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-f', '--xmlfile', metavar='df', type=str,
                        help='Name of data file containing cids (as txt)')

    args = parser.parse_args()
    xmlfile = args.xmlfile

    tree = ET.parse(xmlfile)
    root = tree.getroot()
    aid = Bioassay(root)
    assay = aid.assay
    results = aid.assay_results
    result = results.assay_results[0]