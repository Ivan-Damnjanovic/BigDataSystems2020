import sys
import numpy as np
import pickle
import math
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from sklearn.linear_model import PassiveAggressiveRegressor
from sklearn.metrics import mean_squared_error
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


def prediction(input_data, regressor, model, output_attribute_index):
    # Ako nema podataka, ne radi se nikakva predikcija
    if input_data.isEmpty():
        print("No input data.")
        print("")
        return

    # Svaki rekord se transformise u niz reci
    fixed_data = input_data.map(lambda x: x[1].split(','))
    # Vrsi se parsiranje svih reci tako da se dobiju odgovarajuce vrednosti
    parsed_data = fixed_data.map(
        lambda x: list(map(lambda y: float(y), x[3:-5])))
    # Parsiraju se vrednosti odabranog izlaznog atributa
    output_data = fixed_data.map(
        lambda x: float(x[-5 + output_attribute_index]))

    # Broji se koliko ima ukupno elemenata i ispisuje
    count = parsed_data.count()
    print("Total number of elements: " + str(count) + ".")

    # Ukoliko ima 0 elemenata, tj. RDD je prazan, ne vrsi se nikakva predikcija
    if parsed_data.isEmpty():
        print("There are no elements so no prediction can be made.")
        print("")
        return

    # Iz RDD-ova se generisu Numpy matrica koja predstavlja
    # ulaz u Passive Aggressive Regressor model,
    # kao i vektor koji odredjuje ocekivane izlazne vrednosti
    input_matrix = np.array(parsed_data.collect())
    output_vector = np.array(output_data.collect())
    # Vrsi se predikcija i dobija se vektor
    # koji sadrzi vrednosti koje predvidja model
#DS    scikit_prediction_vector = regressor.predict(input_matrix)

    # Formira se DataFrame objekat na osnovu koga
    # se vrsi predikcija preko ucitanog modela
    input_cols = []
    for i in range(15):
        input_cols.append("_" + str(i+1))
    assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
    data_frame = assembler.transform(parsed_data.toDF())
    prediction = model.transform(data_frame)
    spark_prediction_vector = np.array(prediction.select("prediction").collect())

    # Racunaju se root mean squared error metrike i ispisuju na konzoli
#DS    metrics_scikit = mean_squared_error(output_vector, scikit_prediction_vector)
    metrics_spark = mean_squared_error(output_vector, spark_prediction_vector)
#DS    print("Scikit-learn - Root mean squared error: " + str(math.sqrt(metrics_scikit)))
    print("Spark ML - Root mean squared error: " + str(math.sqrt(metrics_spark)))


# Main funkcija koja prima kao argumente adresu Kafka brokera,
# ime topika sa kog se vrsi preuzimanje podataka za obradu,
# vremenski interval koji definise granularnost obrade,
# putanju do modela koji treba da se koristi,
# kao i indeks koji definise izlazni atribut za koji se vrsi predikcija
# (obavezno mora biti ceo broj iz skupa {0, 1, 2, 3, 4})
def main(bootstrap_server, topic_name, time_interval, scikit_model_path, spark_model_path, output_attribute_index):

    # Ucitavanje Passive Aggressive Regressor modela
#DS    with open(scikit_model_path, 'rb') as opened_file:
#DS        regressor = pickle.load(opened_file)
    regressor = "Passive Aggressive Regressor"  #DS

    # Inicijalizacija konfiguracije i konteksta izvrsenja aplikacije
    configuration = SparkConf().setAppName("BigDataProj3_Consumer")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")
    # Inicijalizacija sesije
    # (mora da se obavi zbog ucitavanja modela)
    session = SparkSession(context)

    # Ucitavanje Spark modela sa zadate putanje
    model = LinearRegressionModel.load(spark_model_path)

    # Instanciranje streaming konteksta
    # (tako da se obrada izvrsava na svakih time_interval sekundi),
    # kao i stream-a uz zadat topik i Kafka broker
    streaming_context = StreamingContext(context, time_interval)
    stream = KafkaUtils.createDirectStream(streaming_context, [topic_name], {
        "metadata.broker.list": bootstrap_server})

    # Za svaki RDD, definise se funkcija koja vrsi predikciju
    # (uz odgovarajuci prosledjen model i indeks izlaznog atributa)
    stream.foreachRDD(lambda input_data: prediction(
        input_data, regressor, model, output_attribute_index))

    # Zapocinje se obrada stream-a
    streaming_context.start()
    streaming_context.awaitTermination()


# Sintaksa za pokretanje programa s komandne linije je:
# spark-submit consumer.py bootstrap_server topic_name time_interval scikit_model_path spark_model_path output_attribute_index
# Na primer:
# spark-submit consumer.py localhost:9092 Merged 5 /home/aleksandra.stojnev/apps/ivan.damnjanovic/model.pickle /home/aleksandra.stojnev/apps/ivan.damnjanovic/spark-model 0
if __name__ == "__main__":
    if len(sys.argv) != 7:
        # Greska nastupa ukoliko sintaksa nije dobra, tj. nema tacno 6 parametara
        print("\nBad command syntax. The correct parameter syntax is:")
        print(
            "bootstrap_server topic_name time_interval scikit_model_path spark_model_path output_attribute_index\n")
    else:
        good = True
        try:
            bootstrap_server = sys.argv[1]
            topic_name = sys.argv[2]
            time_interval = float(sys.argv[3])
            scikit_model_path = sys.argv[4]
            spark_model_path = sys.argv[5]
            output_attribute_index = int(sys.argv[6])
        except ValueError:
            # Greska nastupa ukoliko ne mogu svi parametri da se parsiraju uspesno
            print("\nInvalid parameters.\n")
            good = False
        if not (0 <= output_attribute_index <= 4):
            print("\nInvalid parameters.\n")
        elif good:
            # Pokretanje programa uz odgovarajuce parsirane parametre
            main(bootstrap_server, topic_name, time_interval,
                 scikit_model_path, spark_model_path, output_attribute_index)
