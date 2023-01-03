import os.path
import pandas as pd
import numpy as np
import os.path
import re
import wget
import time
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = (SparkSession.builder 
    .master('local[*]') 
    .config("spark.driver.memory", "15g") 
    .appName('imdb_spark') 
    .getOrCreate())

class GestionDataset:
    
    def __init__(self,path_origin,typeFileOrigin,dict_file,liste_file,url_base):
        self.path_origin =path_origin
        self.typeFileOrigin=typeFileOrigin
        self.dict_file=dict_file
        self.liste_file=liste_file
        self.url_base = url_base
        
    def download_file(self):
        for i in self.liste_file:
            url_download = f'{self.url_base}{i}.{self.typeFileOrigin}'
            wget.download(url_download)
            self.dict_file.update({i:spark.read.csv(f'{i}.{self.typeFileOrigin}',sep="\t",header=True)})
        return self.dict_file
    
    def verif_file(self):
        liste_verif = []
        file_to_extract  = os.listdir(self.path_origin)
        pattern =f"((?!\.[a-z])[a-z]*\.[a-z]*(?=\.[{self.typeFileOrigin}]))"
        try:
            for file in file_to_extract:
                split_name = "".join(re.findall(pattern,file))
        
                if split_name in liste_verif:
                    pass
                else:
                    if split_name in  self.liste_file :
                    
                        self.dict_file.update({split_name:spark.read.csv(f'{self.path_origin}{split_name}.{self.typeFileOrigin}',sep="\t",header=True)})
                        liste_verif.append(split_name)
                    else:
                        pass
        except:
            return self.download_file()
        if set(liste_verif) == set(self.liste_file):
            return self.dict_file
        else:
            return self.download_file()
        
    
    def createOrRemplaceView(self):
         for name_file,dataframe in  self.dict_file.items():
            name_file=name_file.replace(".","_")
            a=name_file
            dataframe.createOrReplaceTempView(a)


class upload_dataset(GestionDataset):
    
    def __init__(self,path_origin,typeFileOrigin,dict_file,liste_file,url_base,path_to_save,name_csv):
        super().__init__(path_origin,typeFileOrigin,dict_file,liste_file,url_base)
        self.path = path_to_save
        self.name = name_csv    
    
    def table_temp(self):
        table_temp_join = spark.sql(""" 
        SELECT  DISTINCT(TP.nconst) AS Nconst,TB.tconst AS Tconst,primaryName,birthYear,deathYear, 
        primaryTitle,originalTitle,startYear,runtimeMinutes,genres,category,primaryProfession,TB.titleType AS titleType,TA.region AS region ,isAdult,
        (CASE WHEN category = 'actor' THEN 0 WHEN category = 'actress' THEN 1 ELSE 'NaN' END ) sexe,
        (CASE WHEN INT(deathYear)>0 THEN deathYear-birthYear ELSE YEAR(NOW())-birthYear END) AS age,
        startYear-birthYear AS age_in_movie,
        FLOOR(ROUND(startYear/10,2))*10 AS decade
        FROM 
        title_basics AS TB 
        INNER JOIN title_akas AS TA ON TB.tconst = TA.titleId 
        INNER JOIN title_principals TP  ON TA.titleId  = TP.tconst
        INNER JOIN name_basics AS NB ON TP.nconst = NB.nconst
        WHERE  RLIKE (category,'(director|actor|actress)') AND NOT RLIKE (primaryProfession,'(cinematographer|miscellaneous)') AND titleType='movie' AND region="FR" AND isAdult = 0
    
        """)
        table_temp_join.createOrReplaceTempView("table_temp_join")

    def req_movie(self):
        
        movie_sql = spark.sql("""
        WITH final_tab AS (
        SELECT DISTINCT(TB.tconst),primaryTitle,originalTitle,startYear,runtimeMinutes,genres,averageRating,numVotes,FLOOR(ROUND(startYear/10,2))*10 AS decade
        FROM title_basics AS TB 
        INNER JOIN title_akas AS TA ON TB.tconst = TA.titleId 
        INNER JOIN title_ratings AS TR  ON  TB.tconst =TR.tconst
        WHERE region="FR" AND TitleType="movie" AND isAdult=0 AND runtimeMinutes<=(60*5)
        )

        SELECT DISTINCT(tconst),primaryTitle,originalTitle,startYear,decade,runtimeMinutes,genres,averageRating,numVotes,SPLIT(genres,",")[0] AS genre   FROM final_tab  WHERE SPLIT(genres,",")[0] IS NOT NULL AND SPLIT(genres,",")[0] != 'Adult'
        UNION 
        SELECT DISTINCT(tconst),primaryTitle,originalTitle,startYear,decade,runtimeMinutes,genres,averageRating,numVotes,SPLIT(genres,",")[1] AS genre   FROM final_tab WHERE SPLIT(genres,",")[1] IS NOT NULL AND SPLIT(genres,",")[1] != 'Adult'
        UNION 
        SELECT DISTINCT(tconst),primaryTitle,originalTitle,startYear,decade,runtimeMinutes,genres,averageRating,numVotes,SPLIT(genres,",")[2] AS genre  FROM final_tab AS WHERE SPLIT(genres,",")[2] IS NOT NULL AND SPLIT(genres,",")[2] != 'Adult'
        ORDER BY tconst ASC
        """)
        return movie_sql
        
    def recuperation_file_statistique(self):
        
        super().verif_file()
        super().createOrRemplaceView()
        self.table_temp()
        
        # statistiques globales 
        # contenu  => contenu FR => contenu movie => contenu movie FR
        stat_global = spark.sql(""" SELECT total_contenu,total_contenu_region,total_movie,total_movie_fr 
        FROM 
        ( SELECT COUNT(tconst) AS total_contenu FROM title_basics) AS TB_T,
        ( SELECT DISTINCT(COUNT(titleId)) AS total_contenu_region ,region FROM title_akas WHERE region="FR" GROUP BY region) AS TA_R,
        ( SELECT COUNT(tconst) total_movie FROM title_basics WHERE titleType="movie") AS TB_M,
        ( SELECT DISTINCT(COUNT(tconst)) total_movie_fr FROM title_basics AS TB INNER JOIN title_akas AS TA ON tconst=titleId  WHERE titleType="movie" AND region="FR") AS TA_TB_M_FR
        """).toPandas()
    
        
        # gestion des genres 
        genre_in_dataset = spark.sql("""
        SELECT TR.tconst,averageRating,numVotes,SPLIT(genres,",")[0] AS genre,runtimeMinutes   FROM title_basics AS TB INNER JOIN title_ratings AS TR ON TB.tconst = TR.tconst  WHERE runtimeMinutes >0
        UNION 
        SELECT TR.tconst,averageRating,numVotes, SPLIT(genres,",")[1] AS genre,runtimeMinutes   FROM title_basics AS TB INNER JOIN title_ratings AS TR ON TB.tconst = TR.tconst  WHERE runtimeMinutes >0 
        UNION 
        SELECT TR.tconst,averageRating,numVotes, SPLIT(genres,",")[2] AS genre,runtimeMinutes  FROM title_basics AS TB INNER JOIN title_ratings AS TR ON TB.tconst = TR.tconst  WHERE runtimeMinutes >0
        ORDER BY tconst ASC

        """).toPandas()
        genre_in_dataset.columns = genre_in_dataset.columns.str.lower()
        genre_in_dataset=genre_in_dataset.astype({"runtimeminutes":"int16","averagerating":"float16","numvotes":"int32"})
        genre_in_dataset=genre_in_dataset.groupby(["genre"])["runtimeminutes","averagerating","numvotes"].mean().reset_index()
        genre_in_dataset.iloc[:,1:]=genre_in_dataset.iloc[:,1:].round(2)
      
        
        # gestion des types de contenus 
        
        stat_contenu= spark.sql("""
                SELECT DISTINCT(titleType),((COUNT(tconst) OVER(PARTITION BY titleType))/COUNT(tconst) OVER() * 100 ) AS percent 
                FROM title_basics 
                GROUP BY titleType,tconst
                ORDER BY percent DESC
                """).toPandas()
       
    
        # statistiques movies 
        movie_sql = self.req_movie()
        df_movie =movie_sql.toPandas()
        df_movie.columns = df_movie.columns.str.lower()
        df_movie.replace("\\N",np.nan,inplace=True)
        df_movie=df_movie.fillna(-9999)
        df_movie=df_movie.astype({"startyear":"int16","decade":"int16","runtimeminutes":"int16","averagerating":"float16","numvotes":"int32"})
      
        # actor 
        
        
        actor_sql = spark.sql(""" 
        WITH table_final AS (
        SELECT Nconst,tconst,primaryName,birthYear,deathYear, primaryTitle,originalTitle,startYear,runtimeMinutes,genres,sexe,age_in_movie,age,decade,COUNT(Tconst) OVER(PARTITION BY Nconst) AS nb_film
        FROM  table_temp_join
        WHERE  RLIKE (category,'actor|actress')
        )

        SELECT Nconst,tconst,primaryName,birthYear,deathYear, primaryTitle,originalTitle,startYear,runtimeMinutes,genres,nb_film,sexe,age_in_movie,age,decade,
        DENSE_RANK() OVER (ORDER BY nb_film DESC) AS rank 
        FROM table_final 
        WHERE sexe =0 
        UNION 
        SELECT Nconst,tconst,primaryName,birthYear,deathYear, primaryTitle,originalTitle,startYear,runtimeMinutes,genres,nb_film,sexe,age_in_movie,age,decade,
        DENSE_RANK() OVER (ORDER BY nb_film DESC) AS rank 
        FROM table_final 
        WHERE sexe =1
        """)
        df_actor = actor_sql.toPandas()
        df_actor=df_actor.replace("\\N",np.nan)
        df_actor['film_per_decade']=df_actor.groupby(["primaryName","decade"]).decade.transform('count')
        df_actor=df_actor.fillna(-9999)
        df_actor=df_actor.astype({'startYear':'int16','runtimeMinutes':'int16','birthYear':'int16','deathYear':'int16','rank':'int8','film_per_decade':'int8','nb_film':'int16'})
        
        
        # director
        director_sql = spark.sql(""" 
        WITH table_final AS (
        SELECT Nconst,tconst,primaryName,birthYear,deathYear,primaryProfession, primaryTitle,originalTitle,startYear,runtimeMinutes,genres,age_in_movie,age,decade,COUNT(Tconst) OVER(PARTITION BY Nconst) AS nb_film
        FROM  table_temp_join
        WHERE  RLIKE (category,'director') 
        )
        SELECT Nconst,tconst,primaryName,birthYear,deathYear,primaryProfession, primaryTitle,originalTitle,startYear,runtimeMinutes,genres,age_in_movie,age,decade,nb_film,
        DENSE_RANK() OVER (ORDER BY nb_film DESC) AS rank 
        FROM table_final
        """)
        df_director = director_sql.toPandas()
        df_director=df_director.replace("\\N",np.nan)
        df_director['film_per_decade']=df_director.groupby(["primaryName","decade"]).decade.transform('count')
        df_director=df_director.fillna(-9999)
        df_director=df_director.astype({'startYear':'int16','runtimeMinutes':'int16','birthYear':'int16','deathYear':'int16','rank':'int8','film_per_decade':'int8','nb_film':'int16'})
        

        if os.path.exists(f'{self.path}'):
            pass
        else:
            os.mkdir(f'{self.path}')
            

        stat_global.to_csv(f"{self.path}stat_global.csv",sep=",",decimal=",") # to Power BI  
        genre_in_dataset.to_csv(f'{self.path}stat_gen_genre.csv',sep=",",decimal=",") # to PowerBI
        stat_contenu.to_csv(f"{self.path}stat_gen_content.csv",sep=",",decimal=",") # to Power BI 
        df_movie.to_csv(f"{self.path}movie.csv",sep=",",decimal=",") # to PowerBI
        df_actor.to_csv(f"{self.path}actor.csv",sep=",",decimal=",") # to powerBi
        df_director.to_csv(f"{self.path}producer.csv",sep=",",decimal=",") # to powerBi

        del actor_sql
        del df_actor
        del movie_sql
        del df_movie
        del stat_contenu
        del genre_in_dataset
        del stat_global
        del director_sql
        del df_director
        



    def recuperation_file_ml(self):
        # téléchargement de chaque de nom de fichier de la liste
        # lecture avec aparche spark
        # transformation des df spark en TempView pour utilisation du SQL
        super().verif_file()
        super().createOrRemplaceView()
        self.table_temp()
    

        movie_sql = self.req_movie()
        
        distribution_sql = spark.sql(""" 
            WITH table_final AS (
            SELECT Nconst,tconst,primaryName,category,COUNT(Tconst) OVER(PARTITION BY Nconst) AS nb_film
            FROM  table_temp_join
            WHERE startYear > 1970)
            SELECT tconst,primaryName,nb_film FROM table_final WHERE (RLIKE (category,'(actor|actress)') AND nb_film >=30) OR (RLIKE (category,'director') AND nb_film >=12)
            
        """)


        movie_sql.createOrReplaceTempView("movie_sql")
        distribution_sql.createOrReplaceTempView("df_distribution")
        r_sql = spark.sql(""" 
            SELECT DSM.tconst,primaryName,primaryTitle,originalTitle,genres,startYear,decade,runtimeMinutes,averageRating,numVotes,genre 
            FROM movie_sql AS DSM 
            LEFT JOIN df_distribution AS D_D ON 
            DSM.tconst = D_D.tconst """)

       
        df= r_sql.toPandas()
        df=df.replace("\\N",np.nan)
        df=df.astype({'startYear':'float32','runtimeMinutes':'float32','averageRating':'float32','numVotes':'float32'})
        df = pd.get_dummies(df,columns=["genre"],prefix="")
        df = pd.get_dummies(df,columns=["primaryName"],prefix="")
        df.columns=df.columns.str.lower().str.replace("_","").str.replace("-","_").str.replace(" ","_")
        df=df.groupby([i for i in df.loc[:,"tconst":"numvotes"].columns])[df.loc[:,"action":].columns].max().reset_index()
        df=df.astype({"startyear":"int16","decade":"int16","runtimeminutes":"int16","averagerating":"float16","numvotes":"int32"})
    
            
        if os.path.exists(f'{self.path}'):
            pass
        else:
            os.mkdir(f'{self.path}')

        df.to_csv(f"{self.path}{self.name}.csv",sep=";",encoding="utf-8")

dict_file={}
url_base="https://datasets.imdbws.com/"
liste_file = sorted(["title.akas","title.basics","title.ratings","title.principals","name.basics"])
path=input('Indiquez le chemin du dossier d\'enregistrement depuis le dossier en cours defaut = csv/ ')
path_origin=input("Indiquez le chemin du dossier d'origine de vérification de fichier à défaut, le dossier en cours => laissez vide ")
typeFileOrigin = input("Indiquez l'extension des fichiers, à défaut .tsv.gz => laissez vide")
name_file = input("Indiquez le nom du fichier de destination pour le dataset ML à défaut df_movie => laissez vide ")

if len(path)<1:
    path="csv/"
if len(path_origin)<1:
    path_origin = "./"

if len(name_file)<1:
    name_file="df_movie"

if len(typeFileOrigin) <1:
    typeFileOrigin ="tsv.gz"
        
dataset = upload_dataset(path_origin,typeFileOrigin,dict_file,liste_file,url_base,path,name_file)

option = int(input("Tapez 1 : récupération file power bi, Tapez 2 : récupération file machine learning , Tapez 3 : les deux " ))
if  option == 1:
     dataset.recuperation_file_statistique()
elif option ==2 :
    dataset.recuperation_file_ml()

else: 
    dataset.recuperation_file_statistique()
    dataset.recuperation_file_ml()