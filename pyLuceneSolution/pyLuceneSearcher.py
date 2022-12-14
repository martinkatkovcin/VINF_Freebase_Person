import lucene
import sys
from java.nio.file import Paths
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

def searcher(search_string):
    counter = 0
    store = []
    indexDirectory = 'index'

    # create an IndexSearcher object
    reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexDirectory)))
    searcher = IndexSearcher(reader)

    # columns, we are searching in
    search_columns = ["name", "birth", "death", "weight_kg", "height_meters", "nationality", "place_of_birth"]

    query = MultiFieldQueryParser.parse(MultiFieldQueryParser(search_columns, StandardAnalyzer()), search_string)
    results = searcher.search(query, 100)

    # for each record, output the information about the Person we have found based on conditions given
    for score in results.scoreDocs:
        doc = searcher.doc(score.doc)
        print(f'Basic information about the person (id_person: {doc.get("id_person")})')
        print(f'Name: {doc.get("name")}')
        print(f'Birth: {doc.get("birth")}')
        print(f'Death: {doc.get("death")}')
        print(f'Weight in kilograms: {doc.get("weight_kg")}')
        print(f'Height in meters: {doc.get("height_meters")}')
        print(f'Nationality: {doc.get("nationality")}')
        print(f'Place of birth: {doc.get("place_of_birth")}')
        print(f'Birthdate validation: {doc.get("birth_validation")}')
        print(f'Deathdate validation: {doc.get("death_validation")}' + '\n')

        store.append([f'Basic information about the person (id_person: {doc.get("id_person")})',
                      f'Name: {doc.get("name")}',
                      f'Birth: {doc.get("birth")}',
                      f'Death: {doc.get("death")}',
                      f'Weight in kilograms: {doc.get("weight_kg")}',
                      f'Height in meters: {doc.get("height_meters")}',
                      f'Nationality: {doc.get("nationality")}',
                      f'Place of birth: {doc.get("place_of_birth")}',
                      f'Birthdate validation: {doc.get("birth_validation")}',
                      f'Deathdate validation: {doc.get("death_validation")}' + '\n'])
        counter = counter + 1

    print(f'There was found {counter} people with {search_string} condition.')
    return store


if __name__ == "__main__":
    search_string = input("Enter search string\n")
    searcher(search_string)
