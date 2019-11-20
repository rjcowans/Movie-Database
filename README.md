# Movie-Database
Movie Database using Python and Java

In order to properly simulate the code. 

One would need to run [Extraction](https://github.com/rjcowans/Movie-Database/blob/master/src/Data_Exfil/Extractor.py)

Using the two csv found in the folder at -> [Inputs](https://github.com/rjcowans/Movie-Database/tree/master/input)

The Extractor will pipe out 6 csvs that would look like the files in this link [Outputs](https://github.com/rjcowans/Movie-Database/tree/master/output)

When attempting to run theses 6 csvs need to be in the same working directory as the java file

Once a MySQL Datbase is Required. 

**Adivise, I used default creds as an example, but locally this would be kept secret and changed**

After that one would need the **mssql-jdbc-7.2.1.jre8.jar** from -> [MySQL Java Connector](https://dev.mysql.com/downloads/connector/j/)

Then One can complile and run the java application

Thank you for you time.

Richard Cowans

Shoutout to [@PS2emu](https://github.com/PS2emu) for working with the super 50 line SQL Statement for bringing the top 5 movies based on genres, actors,keywords,rating, and vote

