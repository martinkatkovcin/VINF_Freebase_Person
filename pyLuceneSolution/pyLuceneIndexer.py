import lucene
from java.nio.file import Paths                                                                         
from org.apache.lucene.store import NIOFSDirectory                                                      
from org.apache.lucene.analysis.standard import StandardAnalyzer                                        
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader       
from org.apache.lucene.search import IndexSearcher                                                      
from org.apache.lucene.queryparser.classic import QueryParser                                           
from org.apache.lucene.document import Document, Field, StringField, TextField, StoredField, FieldType  
import csv

def createIndex():
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    indexDirectory = 'index'
    indexWriterConfig = IndexWriterConfig(StandardAnalyzer())

    # create an IndexWriter object
    writer = IndexWriter(NIOFSDirectory(Paths.get(indexDirectory)), indexWriterConfig)

    print("Creating index...")

    # open the CSV file for reading
    with open('../data/final_dataset.csv', 'r') as csvfile:
        # read the data from the CSV file
        data = csv.reader(csvfile, delimiter=',')
        # iterate over each row in the CSV file
        for row in data:
            # create a Document object
            doc = Document()
            # add a Field object for each column in the row
            doc.add(Field('id_person', row[0], TextField.TYPE_STORED))
            doc.add(Field('name', row[1], TextField.TYPE_STORED))
            doc.add(Field('birth', row[2], TextField.TYPE_STORED))
            doc.add(Field('death', row[3], TextField.TYPE_STORED))
            doc.add(Field('weight_kg', row[4], TextField.TYPE_STORED))
            doc.add(Field('height_meters', row[5], TextField.TYPE_STORED))
            doc.add(Field('birth_validation', row[6], TextField.TYPE_STORED))
            doc.add(Field('death_validation', row[7], TextField.TYPE_STORED))
            doc.add(Field('nationality', row[8], TextField.TYPE_STORED))
            doc.add(Field('place_of_birth', row[9], TextField.TYPE_STORED))
            # add the Document to the index
            writer.addDocument(doc)
    writer.commit()

    print("Finished")

if __name__ == "__main__":
    createIndex()
