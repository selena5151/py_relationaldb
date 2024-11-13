import pandas as pd
import warnings
warnings.filterwarnings("ignore")

class Relation(pd.DataFrame):

    #load dataframe
    def __init__(self, filepath=None, sep='|'):
        if type(filepath) == str:
            super().__init__(pd.read_csv(filepath,sep=sep))
        elif type(filepath) == pd.DataFrame:
            super().__init__(filepath)
        else:
            print('help')

    #select
    def select(self, condition):
        """
        Example:
        >>>Input: invoices.select('BillingCountry == "Germany"').head()
                InvoiceId  CustomerId InvoiceDate           BillingAddress BillingCity BillingState BillingCountry BillingPostalCode  Total
        0           1           2  2009-01-01  Theodor-Heuss-Stra??e 34   Stuttgart         None        Germany             70174       1.98
        5           6          37  2009-01-19         Berger Stra??e 10   Frankfurt         None        Germany             60316       0.99
        6           7          38  2009-02-01      Barbarossastra??e 19      Berlin         None        Germany             10779       1.98
        11         12           2  2009-02-11  Theodor-Heuss-Stra??e 34   Stuttgart         None        Germany             70174       13.86
        28         29          36  2009-05-05       Tauentzienstra??e 8      Berlin         None        Germany             10789       1.98
        >>>Input:
        """
        return Relation(self.query(condition))

    #project
    def project(self, col):
        """
        Example:
        >>>Input: invoices.project(['CustomerId', 'BillingCity']).head()
                CustomerId BillingCity
        0           2   Stuttgart
        1           4        Oslo
        2           8    Brussels
        3          14    Edmonton
        4          23      Boston
        >>>Input:
        """

        if type(col) != list:
            raise ValueError("You must put the attribute in []:(example: ['attr1','attr2'])")
        for name in col:
            if name not in self.columns:
                raise ValueError("'{}' is not in relation".format(name))
            
        return Relation(self[col])

    #cartesien
    def cartesien(self, df1):
        """
        Example:
        >>>Input: albums.cartesien(artists).select('ArtistId_albums == ArtistId_artists').project(['AlbumId','Title','Name']).head()
              AlbumId                                  Title       Name
        0           1  For Those About To Rock We Salute You      AC/DC
        276         2                      Balls to the Wall     Accept
        551         3                      Restless and Wild     Accept
        825         4                      Let There Be Rock      AC/DC
        1102        5                               Big Ones  Aerosmith
        >>>Input:
        """
        name1 = [ x for x in globals() if globals()[x] is self][0] #get the dataframe name
        name2 = [ x for x in globals() if globals()[x] is df1][0]  #get the dataframe name
        return Relation(self.merge(df1, how='cross',suffixes=['_'+name1,'_'+name2]))

    #rename
    def dfrename(self, data_name=None, old_col=None, new_col=None):
        """
        Example:
        >>>Input: albums.cartesien(artists).select('ArtistId_albums == ArtistId_artists').dfrename(old_col=['ArtistId_albums'], new_col=['ArtistId']).project(['AlbumId','Title','ArtistId','Name']).head() 
            AlbumId                                  Title  ArtistId       Name
        0           1  For Those About To Rock We Salute You         1      AC/DC
        276         2                      Balls to the Wall         2     Accept
        551         3                      Restless and Wild         2     Accept
        825         4                      Let There Be Rock         1      AC/DC
        1102        5                               Big Ones         3  Aerosmith
        >>>Input:
        """

        if old_col!=None and new_col!=None:
            if (type(old_col) != list) or (type(new_col) != list):
                raise ValueError("You must put the attribute in []:(example: ['attr1','attr2'])")
            for name in old_col:
                if name not in self.columns:
                    raise ValueError("'{}' is not in relation".format(name))

            for i in range(len(old_col)):
                self=self.rename(columns = {old_col[i]:new_col[i]})

        if data_name!=None:
            globals()[data_name] = self
            return globals()[data_name]
        else:
            return Relation(self)

    #union
    def union(self,df1):
        """
        Example:
        >>>Input: invoices.select('BillingCountry == "Germany"').union(invoices.select('BillingCountry == "USA"'))       
            InvoiceId  CustomerId InvoiceDate           BillingAddress    BillingCity BillingState BillingCountry BillingPostalCode  Total
        0            1           2  2009-01-01  Theodor-Heuss-Stra??e 34      Stuttgart         None        Germany             70174   1.98
        1            6          37  2009-01-19         Berger Stra??e 10      Frankfurt         None        Germany             60316   0.99
        2            7          38  2009-02-01      Barbarossastra??e 19         Berlin         None        Germany             10779   1.98
        3           12           2  2009-02-11  Theodor-Heuss-Stra??e 34      Stuttgart         None        Germany             70174  13.86
        4           29          36  2009-05-05       Tauentzienstra??e 8         Berlin         None        Germany             10789   1.98
        ..         ...         ...         ...                      ...            ...          ...            ...               ...    ...
        114        397          27  2013-10-13          1033 N Park Ave         Tucson           AZ            USA             85719  13.86
        115        405          20  2013-11-21     541 Del Medio Avenue  Mountain View           CA            USA         94040-111   0.99
        116        406          21  2013-12-04         801 W 4th Street           Reno           NV            USA             89503   1.98
        117        407          23  2013-12-04          69 Salem Street         Boston           MA            USA              2113   1.98
        118        408          25  2013-12-05    319 N. Frances Street        Madison           WI            USA             53703   3.96
        >>>Input:
        """
        return Relation(pd.concat([pd.DataFrame(self),pd.DataFrame(df1)]).drop_duplicates().reset_index(drop=True))

    #difference
    def difference(self,df1):
        """
        Example:
        >>>Input: invoices.select('BillingCountry == "Germany"').difference(invoices.select('BillingCity == "Berlin"')).project(['CustomerId','BillingCountry','BillingCity'])
            CustomerId BillingCountry BillingCity
        0    2           Germany        Stuttgart 
        5    37          Germany        Frankfurt 
        11   2           Germany        Stuttgart 
        66   2           Germany        Stuttgart 
        126  37          Germany        Frankfurt 
        137  37          Germany        Frankfurt 
        192  37          Germany        Frankfurt 
        195  2           Germany        Stuttgart 
        218  2           Germany        Stuttgart 
        240  2           Germany        Stuttgart 
        292  2           Germany        Stuttgart 
        321  37          Germany        Frankfurt 
        344  37          Germany        Frankfurt
        366  37          Germany        Frankfurt
        >>>Input:
        """
        return Relation(pd.concat([self,df1,df1]).drop_duplicates(keep=False))
    


    #set intersection
    def intersect(self,df):
        """
        Example:
        >>>Input: invoices.select('BillingCountry == "Germany"').intersect(invoices.select('BillingCity == "Berlin"')) 
            InvoiceId  CustomerId InvoiceDate       BillingAddress BillingCity BillingState BillingCountry BillingPostalCode  Total
        2           7          38  2009-02-01  Barbarossastra??e 19      Berlin         None        Germany             10779   1.98
        4          29          36  2009-05-05   Tauentzienstra??e 8      Berlin         None        Germany             10789   1.98
        5          30          38  2009-05-06  Barbarossastra??e 19      Berlin         None        Germany             10779   3.96
        6          40          36  2009-06-15   Tauentzienstra??e 8      Berlin         None        Germany             10789  13.86
        7          52          38  2009-08-08  Barbarossastra??e 19      Berlin         None        Germany             10779   5.94
        9          95          36  2010-02-13   Tauentzienstra??e 8      Berlin         None        Germany             10789   8.91
        10        104          38  2010-03-29  Barbarossastra??e 19      Berlin         None        Germany             10779   0.99
        16        224          36  2011-09-20   Tauentzienstra??e 8      Berlin         None        Germany             10789   1.98
        17        225          38  2011-09-20  Barbarossastra??e 19      Berlin         None        Germany             10779   1.98
        18        236          38  2011-10-31  Barbarossastra??e 19      Berlin         None        Germany             10779  13.86
        20        247          36  2011-12-23   Tauentzienstra??e 8      Berlin         None        Germany             10789   3.96
        21        269          36  2012-03-26   Tauentzienstra??e 8      Berlin         None        Germany             10789   5.94
        22        291          38  2012-06-30  Barbarossastra??e 19      Berlin         None        Germany             10779   8.91
        24        321          36  2012-11-14   Tauentzienstra??e 8      Berlin         None        Germany             10789   0.99
        >>>Input:
        """

        a = self.union(df)
        b = self.difference(df)
        c = df.difference(self)
        d = a.difference(b)
        e = d.difference(c)
            
        return e

      
if __name__ == '__main__':  
    #load table from db  
    employees = Relation(pd.read_sql_table("employees", "sqlite:///chinook.db"))
    customers = Relation(pd.read_sql_table("customers", "sqlite:///chinook.db"))
    invoices = Relation(pd.read_sql_table("invoices", "sqlite:///chinook.db"))
    invoice_items = Relation(pd.read_sql_table("invoice_items", "sqlite:///chinook.db"))
    artists = Relation(pd.read_sql_table("artists", "sqlite:///chinook.db"))
    albums = Relation(pd.read_sql_table("albums", "sqlite:///chinook.db"))
    media_types = Relation(pd.read_sql_table("media_types", "sqlite:///chinook.db"))
    genres = Relation(pd.read_sql_table("genres", "sqlite:///chinook.db"))
    tracks = Relation(pd.read_sql_table("tracks", "sqlite:///chinook.db"))
    playlists = Relation(pd.read_sql_table("playlists", "sqlite:///chinook.db"))
    playlist_track = Relation(pd.read_sql_table("playlist_track", "sqlite:///chinook.db"))
    
    #read schema from excel
    schema = pd.read_excel('schema.xlsx')

    # Permanently changes the pandas settings
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth',-1)
    
    att = input('Input: ')

    while True:
        #if enter quit will break
        if att == "quit":
           break
        #enter schema will print all tables' schema
        elif att =="schema":
            print(schema)
            att = input('\nInput: ')
        #enter table will list the table from db
        elif att == "table":
            table = ['employees','customers','invoices','invoice_items','artists','albums','media_types','genres','tracks','platlists','playlist_track']
            print(table)
            att = input('\nInput: ')
        #enter help will call the document and show how to use the command
        elif att == "help":
            f = open('help.txt', 'r')
            file_contents = f.read()
            print ("\n",file_contents,"\n")
            f.close()
            att = input('Input: ')
        #enter command
        else:
            output = att
            from database_relation import Relation 
            print(eval(att))
            att = input('\nInput: ')
        continue

    

