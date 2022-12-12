Documentation:
https://vi2022.ui.sav.sk/doku.php?id=user:martin.katkovcin:start

In this project, we were using Python 3.10.8 and also Python container for PyLucene (https://hub.docker.com/r/coady/pylucene).

Firstly, for our searcher we want to create index (it's already created if you pull this project, so before creating new index, you have to delete the files that are inside index folder)
and after that we can search through the fields, that are actually the columns, that we have in the dataset omitted by id_person. We have to run the pyLuceneIndexer.py, after successful run, we
can now use pyLuceneSearcher.py, where we have an initial question about the search string, we want to search. For example, we can say, we want to find some person, so we just write here the name of the person, people of 
the exact height or weight, nationality or place of birth. We can also will have statistic here, how many people in our dataset from freebnase there are. Also we have here the validation of birthdate or deathdate,
because there can be probably some (predictions?) unconfirmed values about the dates, for example, we have here some poeple that their deathdate is 2090, so we will notice the user, that this could not be a valid value.

In the documentation, you can find some statistics with the graphs about the datasets, distribution of nationality, place of birth or the description of the columns about the number of NA values, unique values and standard
EDA (Explorative Data Analysis) information. 
