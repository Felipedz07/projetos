# projetos de recomendação de música spotify
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                           .appName("Recomendador PySpark")\
                           .getOrCreate()
spark
from google.colab import drive
drive.mount('/content/drive')
dados=spark.read.csv("/content/drive/MyDrive/Alura/dados_musicas.csv", header= True, sep=";", inferSchema= True)
import pyspark.sql.functions as f
import plotly.express as px

fig= px.line(dados_anos.toPandas(), x="year", y="loudness",markers=True, title="Variação do loudness conforme os anos")
fig.show()
import plotly.graph_objects as go

fig=go.Figure()

temp=dados_anos.toPandas()

fig.add_trace(go.Scatter(x=temp["year"],y=temp["acousticness"], name="Acousticness"))
fig.add_trace(go.Scatter(x=temp['year'], y=temp['valence'],
                    name='Valence'))
fig.add_trace(go.Scatter(x=temp['year'], y=temp['danceability'],
                    name='Danceability'))
fig.add_trace(go.Scatter(x=temp['year'], y=temp['energy'],
                    name='Energy'))
fig.add_trace(go.Scatter(x=temp['year'], y=temp['instrumentalness'],
                    name='Instrumentalness'))
fig.add_trace(go.Scatter(x=temp['year'], y=temp['liveness'],
                    name='Liveness'))
fig.add_trace(go.Scatter(x=temp['year'], y=temp['speechiness'],
                    name='Speechiness'))

fig.show()

fig= px.imshow(dados_anos.drop("mode").toPandas().corr(), text_auto=True)
fig.show()
dados_gen=spark.read.csv("/content/drive/MyDrive/Alura/dados_musicas_genero.csv", header=True, inferSchema=True)

from pyspark.ml.feature import VectorAssembler

X=dados_gen.columns
X.remove("genres")
X
dados_gen_vector= VectorAssembler(inputCols=X, outputCol="features").transform(dados_gen).select(["features", "genres"])
