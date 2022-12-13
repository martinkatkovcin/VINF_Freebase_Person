import lucene
import sys
from java.nio.file import Paths
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser

def searcher(search_string):

    counter = 0
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    indexDirectory = 'index'

    reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexDirectory)))
    searcher = IndexSearcher(reader)

    search_columns = ["name", "birth", "death", "weight_kg", "height_meters", "nationality", "place_of_birth"]

    query = MultiFieldQueryParser.parse(MultiFieldQueryParser(search_columns, StandardAnalyzer()), search_string)
    results = searcher.search(query, 100)

    for score in results.scoreDocs:
        doc = searcher.doc(score.doc)
        print(f'Basic information about the person (id_person: {doc.get("id_person")})')
        print("Name: " + doc.get("name"))
        print("Birth: " + doc.get("birth"))
        print("Death: " + doc.get("death"))
        print("Weight in kilograms: " + doc.get("weight_kg"))
        print("Height in meters: " + doc.get("height_meters"))
        print("Nationality: " + doc.get("nationality"))
        print("Place of birth: " + doc.get("place_of_birth"))
        print("Birthdate validation: " + doc.get("birth_validation"))
        print("Deathdate validation: " + doc.get("death_validation") + '\n')

        counter = counter + 1

    print(f'There was found {counter} people with {search_string} condition.')

if __name__ == "__main__":
    search_string = input("Enter search string\n")
    searcher(search_string)
