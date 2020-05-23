import sys
import numpy as np
import pickle
import pydoop.hdfs as hdfs
from glob import glob
from sklearn.linear_model import PassiveAggressiveRegressor
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


# Main funkcija koja prima kao argumente
# putanju do foldera koji sadrzi spojene fajlove
# (koji ce biti korisceni da bi se izgradio Passive Aggressive Regressor model),
# indeks koji odredjuju izlazni atribut za koji ce model da vrsi predikciju
# (mora biti ceo broj iz skupa {0, 1, 2, 3, 4}),
# kao i izlaznu putanju na kojoj ce biti sacuvani dobijeni modeli
# (prvo scikit-learn model, a onda i Spark ML model)
def main(input_path, output_attribute_index, scikit_output_path, spark_output_path):

    # Instancira se Passive Aggressive Regressor model
    regressor = PassiveAggressiveRegressor()
    for file_path in hdfs.ls(input_path):
        # Ucitava se sadrzaj fajla i kreira string matrica od njega
        content = hdfs.load(file_path)
        temp = content.split("\n")
        temp = list(map(lambda x: x.split(","), temp))
        temp = list(filter(lambda x: len(x) > 1, temp))
        raw_matrix = np.array(temp)
        # Ucitava se numpy matrica i zatim parsira u matricu realnih vrednosti
        # koja se nakon toga koristi prilikom treniranja modela
        # raw_matrix = np.genfromtxt(file_path, delimiter=',', dtype='string')
        input_matrix = raw_matrix[1:, 3:-5].astype('float64')
        output_vector = raw_matrix[1:, -5 +
                                   output_attribute_index].astype('float64')
        # Model se trenira u vidu iterativnog poboljsanja
        regressor.partial_fit(input_matrix, output_vector)
        # Na konzoli se stampa putanja do obradjenog fajla
        print(file_path)

    # Cuva se kreirani model na izlaznoj putanji
    # koja je prosledjena u vidu argumenta
    with hdfs.open(scikit_output_path, 'w') as opened_file:
        pickle.dump(regressor, opened_file)

    # Inicijalizacija konfiguracije i konteksta izvrsenja aplikacije
    configuration = SparkConf().setAppName("BigDataProj3_Trainer")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")
    # Inicijalizacija sesije
    # (mora da se obavi zbog upisivanja modela)
    session = SparkSession(context)

    # Ucitavanje RDD podataka sa ulazne putanje
    input_data = context.textFile(input_path)
    # Parsiranje svakog reda na reci
    input_data = input_data.map(lambda x: x.split(","))
    # Ignorisu se header-i
    input_data = input_data.filter(lambda x: x[0] != "Timestamp")
    # Ignorisu se prve tri vrste (Timestamp, Latitude i Longitude)
    # i bira se odgovarajuca izlazna kolona
    # (u zavisnosti od output_attribute_index promenljive)
    input_data = input_data.map(lambda x: list(map(lambda y: float(y), x[3:-5])) +
                                [float(x[-5+output_attribute_index])]
                                )

    # Formira se odgovarajuci DataFrame objekat
    # (VectorAssembler se koristi kod formiranja kolona
    # koje omogucavaju koriscenje fit metode linearne regresije)
    input_cols = []
    for i in range(15):
        input_cols.append("_" + str(i+1))
    assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
    data_frame = assembler.transform(input_data.toDF())

    # Instancira se LinearRegression objekat i vrsi njegovo treniranje
    # i zatim cuvanje na zadatoj putanji
    regression = LinearRegression(featuresCol='features', labelCol='_16')
    model = regression.fit(data_frame)
    model.write().overwrite().save(spark_output_path)


# Sintaksa za pokretanje programa s komandne linije je:
# python trainer.py input_path output_attribute_index scikit_output_path spark_output_path
# Na primer:
# python trainer.py /home/aleksandra.stojnev/apps/ivan.damnjanovic/merged 0 /home/aleksandra.stojnev/apps/ivan.damnjanovic/model.pickle /home/aleksandra.stojnev/apps/ivan.damnjanovic/spark-model
if __name__ == "__main__":
    if len(sys.argv) != 5:
        # Greska nastupa ukoliko sintaksa nije dobra, tj. nema tacno 4 parametra
        print("\nBad command syntax. The correct parameter syntax is:")
        print("input_path output_attribute_index scikit_output_path spark_output_path\n")
    else:
        good = True
        try:
            input_path = sys.argv[1]
            output_attribute_index = int(sys.argv[2])
            scikit_output_path = sys.argv[3]
            spark_output_path = sys.argv[4]
        except ValueError:
            # Greska nastupa ukoliko ne mogu svi parametri da se parsiraju uspesno
            print("\nInvalid parameters.\n")
            good = False
        if not (0 <= output_attribute_index <= 4):
            print("\nInvalid parameters.\n")
        elif good:
            # Pokretanje programa uz odgovarajuce parametre
            main(input_path, output_attribute_index,
                 scikit_output_path, spark_output_path)
