import unittest
from pyLuceneSearcher import searcher


class TestSearchMethods(unittest.TestCase):

    def test_search_person(self):
        """
        Test to find exact person with exact name and see information about the person
        """
        output = searcher('"Scotty Young"')

        self.assertEqual([['Basic information about the person (id_person: m.0zdb3vh)',
                           'Name: Scotty Young',
                           'Birth: 1992-02-27',
                           'Death: 2092-02-02',
                           'Weight in kilograms: 95.0',
                           'Height in meters: 1.88',
                           'Nationality: United States of America',
                           'Place of birth: Denton',
                           'Birthdate validation: ',
                           'Deathdate validation: Death date could not be valid..\n']],
                         output)

    def test_find_slovakia(self):
        """
        Test to find people with nationality or place of birth Slovakia
        """
        output = searcher('Slovakia')

        temp = []
        store = []
        file = open('test_slovakia.txt', 'r')
        lines = file.readlines()
        file.close()

        for line in lines:
            if line != '\n':
                if 'Deathdate' not in line:
                    extract = line.replace('\n', '')
                    temp.append(extract)
                else:
                    temp.append(line)
            else:
                store.append(temp)
                temp = []

        self.assertEqual(store, output)


if __name__ == '__main__':
    unittest.main()
